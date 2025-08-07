from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List
from datetime import datetime, timedelta
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image
from pyproj import CRS, Transformer
from io import BytesIO
import os, sys, shutil, traceback, math, pytz, uuid, re, h5py, paramiko, hashlib, subprocess
import numpy as np
import cv2
from subprocess import run

# CONFIG

TIME_INTERVAL_MINUTES = 30
MAX_WORKERS = 8

SSH_HOST = "192.168.2.221"
SSH_PORT = 22
SSH_USERNAME = "sac"
SSH_PASSWORD = "sac123"

TEMP_SESSION_DIRS = set()
job_status = {}
job_lock = Lock()
transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

# FASTAPI INIT

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MODELS

class TileRequest(BaseModel):
    datetime: str
    endtime: str
    bbox: List[float]
    zoom: int

class InterpolationRequest(BaseModel):
    session_id: str
    job_id: str

# HELPERS

def bbox_hash(bbox: List[float]) -> str:
    return hashlib.md5(",".join(map(str, bbox)).encode()).hexdigest()[:8]

def create_temp_dir(session_id: str) -> str:
    temp_dir = os.path.join(os.path.dirname(__file__), "temp_stitched", session_id)
    os.makedirs(temp_dir, exist_ok=True)
    TEMP_SESSION_DIRS.add(temp_dir)
    return temp_dir

def build_remote_hdf_path(dt: datetime) -> str:
    date_path = dt.strftime("%Y/%m/%d")
    utc_time = dt.astimezone(pytz.utc).strftime('%H%M')
    filename = f"3RIMG_{dt.strftime('%d%b%Y').upper()}_{utc_time}_L1B_STD_V01R00.h5"
    return f"/mnt/infortrend_nas_nlsas3/RAW_DATA/INSAT3R/L1B_STD/{date_path}/{filename}"

def fetch_remote_h5(remote_path: str) -> str:
    local_h5 = os.path.join("/tmp", os.path.basename(remote_path))
    print(f"[DEBUG] Fetching HDF5: {remote_path}")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(SSH_HOST, port=SSH_PORT, username=SSH_USERNAME, password=SSH_PASSWORD)
    try:
        ssh.open_sftp().get(remote_path, local_h5)
    finally:
        ssh.close()
    return local_h5

# API: FETCH FRAMES

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
    temp_dir = create_temp_dir(session_id)

    with job_lock:
        job_status[job_id] = ["Job started..."]

    crop_bounds = None  # This will be set from the first frame and reused

    current_time = start_dt
    while current_time <= end_dt:
        timestamp_str = current_time.strftime("%H%M")
        frame_output_path = os.path.join(temp_dir, f"frame_{timestamp_str}.png")

        with job_lock:
            job_status[job_id].append(f"Fetching frame for time {current_time.strftime('%H:%M')}")

        try:
            remote_hdf_path = build_remote_hdf_path(current_time)
            
            # Extract and fix bounds from first timestamp
            if crop_bounds is None:
                crop_bounds = extract_region_from_hdf(
                    remote_hdf_path,
                    req.bbox,
                    frame_output_path
                )
            else:
                extract_region_from_hdf(
                    remote_hdf_path,
                    req.bbox,
                    frame_output_path,
                    fixed_bounds=crop_bounds
                )

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
        "session_id": session_id,
        "job_id": job_id
    }

# API: INTERPOLATION

@app.post("/interpolate-and-generate-video")
def interpolate_and_generate_video(req: InterpolationRequest):
    job_id = req.job_id
    session_dir = os.path.join(os.path.dirname(__file__), "temp_stitched", req.session_id)
    norm_dir = os.path.join(session_dir, "normalized")
    output_dir = os.path.join(session_dir, "video_frames")
    os.makedirs(output_dir, exist_ok=True)

    frame_files = sorted(
        [f for f in os.listdir(session_dir) if f.startswith("frame_") and f.endswith(".png")],
        key=lambda f: int(re.search(r"_(\d{4})", f).group(1))
    )

    if len(frame_files) < 2:
        raise HTTPException(status_code=400, detail="At least two stitched frames required.")

    normalizer = BrightnessNormalizer(session_dir, norm_dir, MAX_WORKERS)
    normalizer.compute_global_min_max()
    normalizer.normalize_images()

    frames = sorted(
        [f for f in os.listdir(norm_dir) if f.startswith("frame_")],
        key=lambda f: int(re.search(r"_(\d{4})", f).group(1))
    )

    rife_script = os.path.abspath(os.path.join("Practical-RIFE", "inference_img.py"))
    rife_model = os.path.abspath(os.path.join("Practical-RIFE", "train_log"))
    python_exec = sys.executable
    global_frame_index = 0

    for i in range(len(frames) - 1):
        time_a, time_b = frames[i], frames[i + 1]
        t1, t2 = re.search(r"_(\d{4})", time_a).group(1), re.search(r"_(\d{4})", time_b).group(1)
        with job_lock:
            job_status[job_id].append(f"Generating frames between {t1[:2]}:{t1[2:]} and {t2[:2]}:{t2[2:]}")

        tmp_dir = os.path.join(session_dir, "tmp_rife")
        os.makedirs(tmp_dir, exist_ok=True)
        shutil.copy(os.path.join(norm_dir, time_a), os.path.join(tmp_dir, "0.png"))
        shutil.copy(os.path.join(norm_dir, time_b), os.path.join(tmp_dir, "1.png"))

        run([python_exec, rife_script, "--img", "0.png", "1.png", "--exp", "5", "--model", rife_model],
            cwd=tmp_dir, check=True)

        shutil.copy(os.path.join(norm_dir, time_a), os.path.join(output_dir, f"img{global_frame_index}.png"))
        global_frame_index += 1

        for f in sorted(os.listdir(os.path.join(tmp_dir, "output")), key=lambda n: int(re.search(r"(\d+)", n).group(1))):
            shutil.move(os.path.join(tmp_dir, "output", f), os.path.join(output_dir, f"img{global_frame_index}.png"))
            global_frame_index += 1

        shutil.rmtree(tmp_dir, ignore_errors=True)

    shutil.copy(os.path.join(norm_dir, frames[-1]), os.path.join(output_dir, f"img{global_frame_index}.png"))

    video_path = os.path.join(session_dir, "interpolated_video.mp4")
    with job_lock:
        job_status[job_id].append("Combining frames into final video...")

    run([
        "ffmpeg", "-y", "-r", "15", "-f", "image2", "-i", "img%d.png",
        "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
        "-c:v", "libx264", "-pix_fmt", "yuv420p", "-q:v", "0", "-q:a", "0",
        os.path.basename(video_path)
    ], cwd=output_dir, check=True)

    shutil.move(os.path.join(output_dir, os.path.basename(video_path)), video_path)

    with job_lock:
        job_status[job_id].append("Video generation complete.")
    return FileResponse(path=video_path, media_type="video/mp4", filename="interpolated_video.mp4")

# API: JOB STATUS

@app.get("/job-status/{job_id}")
def get_job_status(job_id: str):
    with job_lock:
        if job_id not in job_status:
            raise HTTPException(status_code=404, detail="Job ID not found")
        return {"status": job_status[job_id]}

# API: PREVIEW FRAME

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

        timestamp = dt.strftime("%H%M")
        temp_dir = create_temp_dir(session_id)
        bbox_id = bbox_hash(bbox)
        preview_filename = f"preview_{timestamp}_{bbox_id}.png"
        preview_path = os.path.join(temp_dir, preview_filename)

        for f in os.listdir(temp_dir):
            if f.startswith(f"preview_{timestamp}_") and f != preview_filename:
                try:
                    os.remove(os.path.join(temp_dir, f))
                except Exception as e:
                    print(f"[WARN] Failed to delete old preview file: {f} -> {e}")

        if os.path.exists(preview_path):
            return FileResponse(preview_path, media_type="image/png")

        remote_hdf_path = build_remote_hdf_path(dt)
        extract_region_from_hdf(remote_hdf_path, bbox, preview_path)

        return FileResponse(preview_path, media_type="image/png")

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")

# UTIL: HDF REGION EXTRACTION

def extract_region_from_hdf(remote_path, bbox_4326, output_path, fixed_bounds=None):
    local_h5 = fetch_remote_h5(remote_path)

    try:
        with h5py.File(local_h5, "r") as f:
            data_key = "IMG_VIS"
            lat_key = "Latitude_VIS"
            lon_key = "Longitude_VIS"

            lat_ds = f[lat_key]
            lon_ds = f[lon_key]
            lat_scale = lat_ds.attrs["scale_factor"][0]
            lon_scale = lon_ds.attrs["scale_factor"][0]
            lats = lat_ds[:] * lat_scale
            lons = lon_ds[:] * lon_scale
            data = f[data_key][:].squeeze()

            fill_value = f[data_key].attrs.get("_FillValue")

            # ---- Compute pixel bounds from BBOX if not given ----
            if fixed_bounds is None:
                min_lon, min_lat, max_lon, max_lat = bbox_4326
                valid_mask = (lons >= min_lon) & (lons <= max_lon) & \
                             (lats >= min_lat) & (lats <= max_lat)
                if not np.any(valid_mask):
                    raise ValueError("Selected BBOX does not overlap with data.")
                rows, cols = np.where(valid_mask)
                y_min, y_max = rows.min(), rows.max()
                x_min, x_max = cols.min(), cols.max()
                print(f"[DEBUG] Computed bounds from BBOX: Y {y_min}-{y_max}, X {x_min}-{x_max}")
            else:
                y_min, y_max, x_min, x_max = fixed_bounds
                print(f"[DEBUG] Using fixed bounds: Y {y_min}-{y_max}, X {x_min}-{x_max}")

            subset = data[y_min:y_max + 1, x_min:x_max + 1]

            if fill_value is not None:
                subset = np.ma.masked_equal(subset, fill_value)
                if subset.count() == 0:
                    raise ValueError("Selected region contains only fill values.")
                subset = np.ma.filled(subset, 0)

            # Normalize
            vmin, vmax = subset.min(), subset.max()
            if vmax - vmin < 1e-6:
                normalized = np.zeros_like(subset, dtype=np.uint8)
            else:
                normalized = ((subset - vmin) / (vmax - vmin)) * 255

            Image.fromarray(normalized.astype(np.uint8)).save(output_path)
            print(f"[INFO] Saved extracted frame to {output_path}")

            return (y_min, y_max, x_min, x_max)

    finally:
        if os.path.exists(local_h5):
            os.remove(local_h5)

# CLASS: BRIGHTNESS NORMALIZER

class BrightnessNormalizer:
    def __init__(self, input_dir, output_dir, max_threads=8):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.max_threads = max_threads
        self.global_min = None
        self.global_max = None
        os.makedirs(self.output_dir, exist_ok=True)

    def _compute_file_min_max(self, filename):
        img = cv2.imread(os.path.join(self.input_dir, filename), cv2.IMREAD_GRAYSCALE)
        return np.percentile(img, 2), np.percentile(img, 98)

    def compute_global_min_max(self):
        files = [f for f in os.listdir(self.input_dir) if f.startswith("frame_") and f.endswith(".png")]
        if not files:
            raise ValueError("No input frames found for min/max computation.")

        min_vals, max_vals = [], []
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = {executor.submit(self._compute_file_min_max, f): f for f in files}
            for future in as_completed(futures):
                try:
                    min_val, max_val = future.result()
                    min_vals.append(min_val)
                    max_vals.append(max_val)
                except Exception as e:
                    print(f"[ERROR] Min/Max computation failed: {e}")

        if not min_vals or not max_vals:
            raise ValueError("Could not compute global brightness stats.")
        self.global_min, self.global_max = min(min_vals), max(max_vals)
        print(f"[INFO] Global min/max: {self.global_min:.2f}, {self.global_max:.2f}")

    def _normalize_file(self, filename):
        input_path = os.path.join(self.input_dir, filename)
        output_path = os.path.join(self.output_dir, filename)

        img = cv2.imread(input_path, cv2.IMREAD_UNCHANGED)
        if img is None:
            print(f"[WARN] Failed to load image: {input_path}")
            return

        if len(img.shape) == 2:
            img = cv2.cvtColor(img, cv2.COLOR_GRAY2RGB)

        norm = np.zeros_like(img)
        scale = 255.0 / (self.global_max - self.global_min)
        for c in range(3):
            norm[:, :, c] = np.clip((img[:, :, c] - self.global_min) * scale, 0, 255)
        cv2.imwrite(output_path, norm.astype(np.uint8))

    def normalize_images(self):
        files = [f for f in os.listdir(self.input_dir) if f.startswith("frame_") and f.endswith(".png")]
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = {executor.submit(self._normalize_file, f): f for f in files}
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"[ERROR] Normalization failed: {e}")

# SHUTDOWN CLEANUP

@app.on_event("shutdown")
def cleanup_temp_sessions():
    temp_root = os.path.join(os.path.dirname(__file__), "temp_stitched")
    if os.path.exists(temp_root):
        shutil.rmtree(temp_root, ignore_errors=True)
    TEMP_SESSION_DIRS.clear()