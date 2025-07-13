from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Tuple, Optional
from pyproj import Transformer
from datetime import datetime, timedelta
from PIL import Image
from io import BytesIO
import os
import math
import requests
import pytz
import uuid
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from subprocess import run, CalledProcessError
import re

# ---------------- CONFIG ----------------

TILE_SIZE_PX = 256
TIME_INTERVAL_MINUTES = 30
MAX_WORKERS = 8
RETRIES = 2
TIMEOUT = 20
RETRY_DELAY = 2
FFMPEG_PATH = "ffmpeg"

transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

COMMON_PARAMS = {
    "SERVICE": "WMS",
    "VERSION": "1.3.0",
    "REQUEST": "GetMap",
    "FORMAT": "image/png",
    "TRANSPARENT": "true",
    "LAYERS": "IMG_VIS",
    "STYLES": "boxfill/greyscale",
    "COLORSCALERANGE": "0,407",
    "BELOWMINCOLOR": "extend",
    "ABOVEMAXCOLOR": "extend",
    "CRS": "EPSG:3857",
    "WIDTH": str(TILE_SIZE_PX),
    "HEIGHT": str(TILE_SIZE_PX),
}

WMS_BASE_TEMPLATE = (
    "https://mosdac.gov.in/live_data/wms/live3RL1BSTD1km/products/Insat3r/"
    "3R_IMG/{folder_date}/3RIMG_{file_date}_{time}_L1B_STD_V01R00.h5"
)

TEMP_SESSION_DIRS = set()

# ---------------- FASTAPI ----------------

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class TileRequest(BaseModel):
    datetime: str
    endtime: str
    bbox: List[float]
    zoom: int

class InterpolationRequest(BaseModel):
    session_id: str

def fetch_with_retries(url, params, retries=RETRIES, delay=RETRY_DELAY) -> Optional[requests.Response]:
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Retry {attempt+1}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
    return None

def download_tile(col: int, row: int, bbox: List[float], wms_url: str, timestamp_str: str) -> Optional[Tuple[int, int, Image.Image]]:
    params = COMMON_PARAMS.copy()
    params["BBOX"] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
    response = fetch_with_retries(wms_url, params)
    if response is None:
        return None
    try:
        image = Image.open(BytesIO(response.content)).convert("RGBA")
        return (col, row, image)
    except Exception as e:
        print(f"❌ Failed tile ({col},{row}) - {e}")
        return None

@app.post("/fetch-stitched-frames")
def fetch_stitched_frames(req: TileRequest):
    ist = pytz.timezone("Asia/Kolkata")
    start_dt = ist.localize(datetime.strptime(req.datetime, "%Y-%m-%d %H:%M"))
    end_dt = ist.localize(datetime.strptime(req.endtime, "%Y-%m-%d %H:%M"))
    if start_dt.minute not in [15, 45] or end_dt.minute not in [15, 45]:
        raise HTTPException(status_code=400, detail="Only :15 or :45 minutes allowed.")
    if start_dt > end_dt:
        raise HTTPException(status_code=400, detail="Start time must be before end time.")

    min_lon, min_lat, max_lon, max_lat = req.bbox
    min_x, min_y = transformer.transform(min_lon, min_lat)
    max_x, max_y = transformer.transform(max_lon, max_lat)

    mpp = 156543.03 / (2 ** req.zoom)
    tile_extent = TILE_SIZE_PX * mpp
    snapped_min_x = math.floor(min_x / tile_extent) * tile_extent
    snapped_min_y = math.floor(min_y / tile_extent) * tile_extent
    cols = math.ceil((max_x - snapped_min_x) / tile_extent)
    rows = math.ceil((max_y - snapped_min_y) / tile_extent)

    if cols * rows > 400:
        raise HTTPException(status_code=400, detail="Too many tiles requested.")

    session_id = uuid.uuid4().hex[:8]
    temp_dir = os.path.join(os.path.dirname(__file__), "temp_stitched", f"session_{session_id}")
    os.makedirs(temp_dir, exist_ok=True)
    TEMP_SESSION_DIRS.add(temp_dir)

    current_time = start_dt
    while current_time <= end_dt:
        utc_time = current_time.astimezone(pytz.utc)
        folder_date = utc_time.strftime("%Y/%d%b").upper()
        file_date = utc_time.strftime("%d%b%Y").upper()
        time_str = utc_time.strftime("%H%M")
        timestamp_str = current_time.strftime("%H%M")

        wms_url = WMS_BASE_TEMPLATE.format(
            folder_date=folder_date,
            file_date=file_date,
            time=time_str
        )

        tile_tasks = []
        for row in range(rows):
            for col in range(cols):
                x0 = snapped_min_x + col * tile_extent
                y0 = snapped_min_y + row * tile_extent
                bbox = [x0, y0, x0 + tile_extent, y0 + tile_extent]
                tile_tasks.append((col, row, bbox, wms_url, timestamp_str))

        tile_images = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(download_tile, *task) for task in tile_tasks]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    tile_images.append(result)

        if tile_images:
            stitched_img = Image.new("RGBA", (cols * TILE_SIZE_PX, rows * TILE_SIZE_PX))
            for col, row, img in tile_images:
                stitched_img.paste(img, (col * TILE_SIZE_PX, (rows - 1 - row) * TILE_SIZE_PX))
            stitched_img.save(os.path.join(temp_dir, f"frame_{timestamp_str}.png"))

        current_time += timedelta(minutes=TIME_INTERVAL_MINUTES)

    return {
        "message": "Frames stitched successfully",
        "directory": temp_dir,
        "session_id": f"session_{session_id}"
    }

@app.post("/interpolate-and-generate-video")
def interpolate_and_generate_video(req: InterpolationRequest):
    session_id = req.session_id
    session_dir = os.path.join(os.path.dirname(__file__), "temp_stitched", session_id)
    interpolated_dir = os.path.join(session_dir, "interpolated_frames")
    os.makedirs(interpolated_dir, exist_ok=True)

    frames = sorted([
        f for f in os.listdir(session_dir)
        if f.startswith("frame_") and f.endswith(".png")
    ], key=lambda name: int(re.search(r"_(\d{4})", name).group(1)))

    def timestamp_to_minutes(ts):
        h, m = int(ts[:2]), int(ts[2:])
        return h * 60 + m

    def minutes_to_timestamp(mins):
        return f"{mins // 60:02d}{mins % 60:02d}"

    rife_script_img = os.path.abspath(os.path.join("Practical-RIFE", "inference_img_preserve.py"))
    rife_model = os.path.abspath(os.path.join("Practical-RIFE", "train_log"))

    for i in range(len(frames) - 1):
        src1 = os.path.join(session_dir, frames[i])
        src2 = os.path.join(session_dir, frames[i + 1])
        t1 = re.search(r"_(\d{4})", frames[i]).group(1)

        tmp = os.path.join(session_dir, "tmp_rife")
        os.makedirs(tmp, exist_ok=True)
        shutil.copy(src1, os.path.join(tmp, "0.png"))
        shutil.copy(src2, os.path.join(tmp, "1.png"))

        try:
            run([
                "python", rife_script_img,
                "--img", "0.png", "1.png",
                "--exp", "5",
                "--model", rife_model
            ], cwd=tmp, check=True)
        except CalledProcessError as e:
            raise HTTPException(status_code=500, detail=f"RIFE interpolation failed: {e}")

        start_min = timestamp_to_minutes(t1)
        output_dir = os.path.join(tmp, "output")
        for i in range(32):
            new_min = start_min + i
            ts = minutes_to_timestamp(new_min)
            shutil.move(os.path.join(output_dir, f"{i}.png"),
                        os.path.join(interpolated_dir, f"{ts}.png"))

        shutil.rmtree(output_dir, ignore_errors=True)
        shutil.rmtree(tmp)

    # 🔁 Rename timestamped frames to 0.png, 1.png, ...
    sorted_imgs = sorted(os.listdir(interpolated_dir), key=lambda x: int(x.split(".")[0]))
    renaming_dir = os.path.join(interpolated_dir, "renamed")
    os.makedirs(renaming_dir, exist_ok=True)

    for i, fname in enumerate(sorted_imgs):
        shutil.copy(os.path.join(interpolated_dir, fname), os.path.join(renaming_dir, f"{i}.png"))

    video_path = os.path.join(session_dir, "interpolated_video.mp4")

    try:
        run([
            FFMPEG_PATH, "-y", "-framerate", "30",
            "-i", "%d.png",
            "-c:v", "libx264", "-pix_fmt", "yuv420p",
            os.path.basename(video_path)
        ], cwd=renaming_dir, check=True)

        shutil.move(os.path.join(renaming_dir, os.path.basename(video_path)), video_path)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="FFmpeg not found. Set FFMPEG_PATH correctly.")
    except CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"FFmpeg failed: {e}")

    shutil.rmtree(renaming_dir, ignore_errors=True)

    return {
        "message": "Interpolation and video generation complete",
        "video_path": f"/temp_stitched/{session_id}/interpolated_video.mp4"
    }

@app.on_event("shutdown")
def cleanup_temp_sessions():
    for path in TEMP_SESSION_DIRS:
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
    TEMP_SESSION_DIRS.clear()
