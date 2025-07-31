from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List
from pyproj import Transformer
from datetime import datetime, timedelta
from PIL import Image
from io import BytesIO
from subprocess import run
from pyproj import CRS
from concurrent.futures import ThreadPoolExecutor, as_completed
import os, shutil, sys, re
import math
import pytz
import uuid
import time
import paramiko
import traceback
import cv2
import numpy as np
import h5py
from threading import Lock

# ---------------- CONFIG ----------------

TILE_SIZE_PX = 256
TIME_INTERVAL_MINUTES = 30
MAX_WORKERS = 8

transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

TEMP_SESSION_DIRS = set()

SSH_HOST = "192.168.2.221"
SSH_PORT = 22
SSH_USERNAME = "sac"
SSH_PASSWORD = "sac123"
REMOTE_HDF_DIR = "/home/sac/karnav/INSAT/30"

job_status = {}
job_lock = Lock()

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
    job_id: str


@app.post("/fetch-stitched-frames")
def fetch_stitched_frames(req: TileRequest):
    ist = pytz.timezone("Asia/Kolkata")
    start_dt = ist.localize(datetime.strptime(req.datetime, "%Y-%m-%d %H:%M"))
    end_dt = ist.localize(datetime.strptime(req.endtime, "%Y-%m-%d %H:%M"))

    if start_dt.minute not in [15, 45] or end_dt.minute not in [15, 45]:
        raise HTTPException(status_code=400, detail="Only :15 or :45 minutes allowed.")
    if start_dt > end_dt:
        raise HTTPException(status_code=400, detail="Start time must be before end time.")

    session_id = uuid.uuid4().hex[:8]
    job_id = uuid.uuid4().hex[:8]
    with job_lock:
        job_status[job_id] = ["Job started..."]

    temp_dir = os.path.join(os.path.dirname(__file__), "temp_stitched", f"session_{session_id}")
    os.makedirs(temp_dir, exist_ok=True)
    TEMP_SESSION_DIRS.add(temp_dir)

    current_time = start_dt
    while current_time <= end_dt:
        timestamp_str = current_time.strftime("%H%M")
        frame_output_path = os.path.join(temp_dir, f"frame_{timestamp_str}.png")

        with job_lock:
            job_status[job_id].append(f"Fetching frame for time {current_time.strftime('%H:%M')}")

        try:
            # Convert IST -> UTC for filename
            utc_dt = current_time.astimezone(pytz.utc)
            utc_time_str = utc_dt.strftime('%H%M')
            h5_filename = f"3RIMG_{current_time.strftime('%d%b%Y').upper()}_{utc_time_str}_L2C_INS_V01R00.h5"
            remote_h5_path = os.path.join(REMOTE_HDF_DIR, h5_filename)

            extract_region_from_hdf(remote_h5_path, req.bbox, frame_output_path)

            with job_lock:
                job_status[job_id].append(f"Stitched frame for time {current_time.strftime('%H:%M')}")

        except Exception as e:
            traceback.print_exc()
            with job_lock:
                job_status[job_id].append(f"Error: {str(e)}")

        current_time += timedelta(minutes=TIME_INTERVAL_MINUTES)

    return {
        "message": "Frames stitched successfully",
        "directory": temp_dir,
        "session_id": f"session_{session_id}",
        "job_id": job_id
    }



def fetch_tile(fname, remote_folder, local_dir, tile_min_x, tile_min_y,
               snapped_min_x, snapped_min_y, tile_extent):
    try:
        transport = paramiko.Transport((SSH_HOST, SSH_PORT))
        transport.connect(username=SSH_USERNAME, password=SSH_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, fname)
        remote_path = os.path.join(remote_folder, fname)

        if not os.path.exists(local_path) or os.path.getsize(local_path) < 10_000:
            sftp.get(remote_path, local_path)

        sftp.close()
        transport.close()

        img = Image.open(local_path).convert("RGBA")
        col = int((tile_min_x - snapped_min_x) / tile_extent)
        row = int((tile_min_y - snapped_min_y) / tile_extent)
        return (col, row, img)

    except Exception as e:
        print(f"[Thread error] {fname}: {e}")
        return None

@app.post("/interpolate-and-generate-video")
def interpolate_and_generate_video(req: InterpolationRequest):
    job_id = req.job_id
    session_dir = os.path.join(os.path.dirname(__file__), "temp_stitched", req.session_id)
    normalized_dir = os.path.join(session_dir, "normalized")
    output_dir = os.path.join(session_dir, "video_frames")
    os.makedirs(output_dir, exist_ok=True)

    frame_files = [f for f in os.listdir(session_dir) if f.startswith("frame_") and f.endswith(".png")]
    if len(frame_files) < 2:
        raise HTTPException(status_code=400, detail="At least two stitched frames required for interpolation.")

    # Brightness normalization
    try:
        normalizer = BrightnessNormalizer(input_dir=session_dir, output_dir=normalized_dir, max_threads=MAX_WORKERS)
        normalizer.compute_global_min_max()
        normalizer.normalize_images()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Brightness normalization failed: {str(e)}")

    frames = sorted([
        f for f in os.listdir(normalized_dir)
        if f.startswith("frame_") and f.endswith(".png")
    ], key=lambda f: int(re.search(r"_(\d{4})", f).group(1)))

    rife_script = os.path.abspath(os.path.join("Practical-RIFE", "inference_img.py"))
    rife_model = os.path.abspath(os.path.join("Practical-RIFE", "train_log"))
    python_exec = sys.executable

    global_frame_index = 0
    for i in range(len(frames) - 1):
        frame_a = os.path.join(normalized_dir, frames[i])
        frame_b = os.path.join(normalized_dir, frames[i + 1])

        time_a = re.search(r"_(\d{4})", frames[i]).group(1)
        time_b = re.search(r"_(\d{4})", frames[i + 1]).group(1)
        with job_lock:
            job_status[job_id].append(f"Generating frames between {time_a[:2]}:{time_a[2:]} and {time_b[:2]}:{time_b[2:]}")

    for i in range(len(frames) - 1):
        frame_a = os.path.join(normalized_dir, frames[i])
        frame_b = os.path.join(normalized_dir, frames[i + 1])

        tmp_dir = os.path.join(session_dir, "tmp_rife")
        os.makedirs(tmp_dir, exist_ok=True)
        shutil.copy(frame_a, os.path.join(tmp_dir, "0.png"))
        shutil.copy(frame_b, os.path.join(tmp_dir, "1.png"))

        run([python_exec, rife_script, "--img", "0.png", "1.png", "--exp", "5", "--model", rife_model],
            cwd=tmp_dir, check=True)

        shutil.copy(frame_a, os.path.join(output_dir, f"img{global_frame_index}.png"))
        global_frame_index += 1

        output_files = os.listdir(os.path.join(tmp_dir, "output"))
        output_files = sorted(
            output_files,
            key=lambda name: int(re.search(r"(\d+)", name).group(1)) if re.search(r"(\d+)", name) else float("inf")
        )

        for f in output_files:
            shutil.move(os.path.join(tmp_dir, "output", f), os.path.join(output_dir, f"img{global_frame_index}.png"))
            global_frame_index += 1

        shutil.rmtree(tmp_dir, ignore_errors=True)

    shutil.copy(os.path.join(normalized_dir, frames[-1]), os.path.join(output_dir, f"img{global_frame_index}.png"))

    video_path = os.path.join(session_dir, "interpolated_video.mp4")
    with job_lock:
        job_status[job_id].append("Combining frames into final video...")

    run(["ffmpeg", "-y", "-r", "15", "-f", "image2", "-i", "img%d.png",
         "-c:v", "libx264", "-pix_fmt", "yuv420p", "-q:v", "0", "-q:a", "0", os.path.basename(video_path)],
         cwd=output_dir, check=True)

    shutil.move(os.path.join(output_dir, os.path.basename(video_path)), video_path)

    with job_lock:
        job_status[job_id].append("Video generation complete.")

    return FileResponse(path=video_path, media_type="video/mp4", filename="interpolated_video.mp4")

@app.get("/job-status/{job_id}")
def get_job_status(job_id: str):
    with job_lock:
        if job_id not in job_status:
            raise HTTPException(status_code=404, detail="Job ID not found")
        return {"status": job_status[job_id]}

@app.on_event("shutdown")
def cleanup_temp_sessions():
    temp_root = os.path.join(os.path.dirname(__file__), "temp_stitched")
    if os.path.exists(temp_root):
        shutil.rmtree(temp_root, ignore_errors=True)
    TEMP_SESSION_DIRS.clear()

from fastapi import Query

@app.get("/preview-frame")
def preview_frame(
    datetime_str: str = Query(..., alias="datetime"),
    bbox: List[float] = Query(...),
    zoom: int = Query(5),
    session_id: str = Query(...)
):
    try:
        ist = pytz.timezone("Asia/Kolkata")
        dt = ist.localize(datetime.strptime(datetime_str, "%Y-%m-%d %H:%M"))
        if dt.minute not in [15, 45]:
            raise HTTPException(status_code=400, detail="Only :15 or :45 minutes allowed.")

        timestamp_str = dt.strftime("%H%M")
        temp_dir = os.path.join(os.path.dirname(__file__), "temp_stitched", session_id)
        os.makedirs(temp_dir, exist_ok=True)
        TEMP_SESSION_DIRS.add(temp_dir)

        preview_path = os.path.join(temp_dir, "preview_frame.png")
        if os.path.exists(preview_path):
            return FileResponse(preview_path, media_type="image/png")

        # Convert IST -> UTC for filename
        utc_dt = dt.astimezone(pytz.utc)
        utc_time_str = utc_dt.strftime('%H%M')
        h5_filename = f"3RIMG_{dt.strftime('%d%b%Y').upper()}_{utc_time_str}_L2C_INS_V01R00.h5"
        remote_h5_path = os.path.join(REMOTE_HDF_DIR, h5_filename)

        extract_region_from_hdf(remote_h5_path, bbox, preview_path)

        return FileResponse(preview_path, media_type="image/png")

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")


class BrightnessNormalizer:
    def __init__(self, input_dir, output_dir, max_threads=8):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.max_threads = max_threads
        self.global_min = None
        self.global_max = None
        os.makedirs(self.output_dir, exist_ok=True)

    def min_percentile(self, img):
        return np.percentile(img.flatten(), 2)

    def max_percentile(self, img):
        return np.percentile(img.flatten(), 98)

    def compute_global_min_max(self):
        min_vals = []
        max_vals = []
        files = [f for f in os.listdir(self.input_dir) if f.endswith('.png') and f.startswith('frame_')]

        if not files:
            raise ValueError(f"No frame_*.png files found in directory: {self.input_dir}")

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = {executor.submit(self._compute_file_min_max, f): f for f in files}
            for future in as_completed(futures):
                try:
                    min_val, max_val = future.result()
                    min_vals.append(min_val)

                    max_vals.append(max_val)
                except Exception as e:
                    print(f"Failed to compute min/max: {e}")

        if not min_vals or not max_vals:
            raise ValueError("Failed to compute min/max values from frames.")

        self.global_min = min(min_vals)
        self.global_max = max(max_vals)
        print(f"Global min: {self.global_min}, max: {self.global_max}")

    def _compute_file_min_max(self, filename):
        img = cv2.imread(os.path.join(self.input_dir, filename), cv2.IMREAD_GRAYSCALE)
        return self.min_percentile(img), self.max_percentile(img)

    def normalize_images(self):
        files = [f for f in os.listdir(self.input_dir) if f.endswith('.png') and f.startswith('frame_')]

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = {
                executor.submit(self._normalize_file, f): f for f in files
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Failed to normalize: {e}")

    def _normalize_file(self, filename):
        input_path = os.path.join(self.input_dir, filename)
        output_path = os.path.join(self.output_dir, filename)

        img = cv2.imread(input_path, cv2.IMREAD_UNCHANGED)

        if img is None:
            print(f"Warning: Failed to load image {input_path}")
            return

        if len(img.shape) == 2:  # grayscale fallback
            img = cv2.cvtColor(img, cv2.COLOR_GRAY2RGB)

        # Normalize each channel independently
        norm = np.zeros_like(img)
        for c in range(3):
            norm[:, :, c] = np.clip((img[:, :, c] - self.global_min) * (255.0 / (self.global_max - self.global_min)), 0, 255)

        cv2.imwrite(output_path, norm.astype(np.uint8))

from pyproj import CRS, Transformer
import paramiko
import h5py
import numpy as np
from PIL import Image
import os

def extract_region_from_hdf(remote_path, bbox_4326, output_path):
    local_h5 = os.path.join("/tmp", os.path.basename(remote_path))

    print(f"[DEBUG] Attempting to fetch remote HDF5 file: {remote_path}")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(SSH_HOST, port=SSH_PORT, username=SSH_USERNAME, password=SSH_PASSWORD)
    sftp = ssh.open_sftp()
    try:
        sftp.get(remote_path, local_h5)
    finally:
        sftp.close()
        ssh.close()

    with h5py.File(local_h5, "r") as f:
        x_vals = f["X"][:]
        y_vals = f["Y"][:]
        data = f["INS"][0]
        fill_value = f["INS"].attrs.get("_FillValue", -999.0)

        # -------- CF Metadata to CRS --------
        grid_mapping = f["Projection_Information"]
        cf_attrs = {}
        for k, v in grid_mapping.attrs.items():
            if isinstance(v, np.ndarray) and v.shape == (1,):
                val = v[0]
            else:
                val = v
            if isinstance(val, bytes):
                val = val.decode()
            cf_attrs[k] = val

        try:
            crs_cf = CRS.from_cf(cf_attrs)
        except Exception as e:
            print("[ERROR] Failed to construct CRS from CF:", e)
            raise ValueError("Could not parse CRS from file metadata.")

        transformer = Transformer.from_crs("EPSG:4326", crs_cf, always_xy=True)

        # -------- Transform BBOX --------
        min_lon, min_lat, max_lon, max_lat = bbox_4326
        min_x, min_y = transformer.transform(min_lon, min_lat)
        max_x, max_y = transformer.transform(max_lon, max_lat)

        # -------- Pixel Range in Data --------
        x_start = np.searchsorted(x_vals, min_x, side="left")
        x_end   = np.searchsorted(x_vals, max_x, side="right")
        x_start = max(0, min(x_start, data.shape[1]))
        x_end   = max(0, min(x_end, data.shape[1]))

        y_start_rev = np.searchsorted(y_vals[::-1], max_y, side="left")
        y_end_rev   = np.searchsorted(y_vals[::-1], min_y, side="right")
        y_start = len(y_vals) - y_start_rev
        y_end   = len(y_vals) - y_end_rev
        y_start = max(0, min(y_start, data.shape[0]))
        y_end   = max(0, min(y_end, data.shape[0]))

        print(f"[DEBUG] Extracting pixels: x({x_start}:{x_end}), y({y_start}:{y_end})")

        subset = data[y_start:y_end, x_start:x_end]

        if subset.size == 0:
            print("[WARNING] Extracted region is empty due to out-of-bounds or narrow BBOX.")
            raise ValueError("Selected BBOX contains no data or is out of bounds.")

        if np.all(subset == fill_value):
            print("[WARNING] Selected BBOX contains only fill values.")
            raise ValueError("Selected BBOX contains only fill values.")

        # -------- Normalize and Save --------
        subset = np.ma.masked_equal(subset, fill_value)
        subset = np.ma.filled(subset, 0)
        norm = (subset - subset.min()) / (subset.max() - subset.min() + 1e-6)
        image = (norm * 255).astype(np.uint8)

        Image.fromarray(image).save(output_path)
        print(f"[INFO] Saved subset to {output_path}")

    os.remove(local_h5)
