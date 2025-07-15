import os
import math
import requests
from PIL import Image
from io import BytesIO
from datetime import datetime, timedelta
import re
import pytz
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------------ CONFIG ------------------

TILE_PX = 256
MAX_WORKERS = 32
ZOOM_LEVEL = 5
NUM_PAST_DAYS = 10

def get_tile_size_m(zoom):
    return TILE_PX * 156543.03 / (2 ** zoom)

TILE_SIZE_METERS = get_tile_size_m(ZOOM_LEVEL)

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
    "WIDTH": "256",
    "HEIGHT": "256"
}

WMS_BASE_TEMPLATE = (
    "https://mosdac.gov.in/live_data/wms/live3RL1BSTD1km/products/Insat3r/"
    "3R_IMG/{folder_date}/3RIMG_{file_date}_{time}_L1B_STD_V01R00.h5"
)

FULL_DISK_BBOX = [
    -21000000,
    -21000000,
     21000000,
     21000000
]

# ------------------ LOGGING ------------------

log_file_path = "fetch.log"

def log_message(message):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
    full_message = timestamp + message
    print(full_message)
    with open(log_file_path, "a", encoding="utf-8") as log_file:
        log_file.write(full_message + "\n")

# ------------------ TILE UTILS ------------------

def snap_bbox_to_tile_grid(minx, miny, maxx, maxy, tile_size):
    snapped_minx = math.floor(minx / tile_size) * tile_size
    snapped_miny = math.floor(miny / tile_size) * tile_size
    snapped_maxx = math.ceil(maxx / tile_size) * tile_size
    snapped_maxy = math.ceil(maxy / tile_size) * tile_size
    return snapped_minx, snapped_miny, snapped_maxx, snapped_maxy

def generate_tiles(minx, miny, maxx, maxy, tile_size):
    minx, miny, maxx, maxy = snap_bbox_to_tile_grid(minx, miny, maxx, maxy, tile_size)
    tiles = []
    for x in range(int((maxx - minx) / tile_size)):
        for y in range(int((maxy - miny) / tile_size)):
            x0 = minx + x * tile_size
            y0 = miny + y * tile_size
            bbox = [x0, y0, x0 + tile_size, y0 + tile_size]
            tiles.append((x, y, bbox))
    return tiles

def validate_wms_availability(wms_url):
    params = COMMON_PARAMS.copy()

    # Sample a central tile (around the center of the full disk BBOX)
    center_x = (FULL_DISK_BBOX[0] + FULL_DISK_BBOX[2]) / 2
    center_y = (FULL_DISK_BBOX[1] + FULL_DISK_BBOX[3]) / 2
    test_bbox = [
        center_x,
        center_y,
        center_x + TILE_SIZE_METERS,
        center_y + TILE_SIZE_METERS
    ]

    params["BBOX"] = ",".join(map(str, test_bbox))
    try:
        response = requests.get(wms_url, params=params, timeout=10)
        if response.status_code == 200:
            img = Image.open(BytesIO(response.content))
            return img.getbbox() is not None
        return False
    except Exception:
        log_message(f"Validation failed for {wms_url}:\n{traceback.format_exc()}")
        return False

def fetch_and_save_tile(col, row, bbox, save_dir, wms_url, timestamp_ist, time_str_utc, file_date):
    params = COMMON_PARAMS.copy()
    bbox_str = ",".join(map(str, bbox))
    params["BBOX"] = bbox_str
    try:
        response = requests.get(wms_url, params=params, timeout=20)
        if response.status_code == 200:
            image = Image.open(BytesIO(response.content))
            ist_str = timestamp_ist.strftime("%Y%m%d%H%M%S")
            filename = f"3RIMG_{file_date}_{time_str_utc}_L1B_STD_V01R00_{ist_str}_BBOX={bbox_str.replace('.', '_').replace(',', '_')}.png"
            file_path = os.path.join(save_dir, filename)
            image.save(file_path)
            log_message(f"Tile ({col},{row}) saved: {filename}")
        else:
            log_message(f"Failed ({col},{row}) | Status: {response.status_code}")
    except Exception:
        log_message(f"Exception while fetching tile ({col},{row}):\n{traceback.format_exc()}")

def fetch_tiles_concurrently(tiles, save_dir, wms_url, timestamp_ist, time_str_utc, file_date):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(fetch_and_save_tile, col, row, bbox, save_dir, wms_url, timestamp_ist, time_str_utc, file_date)
            for col, row, bbox in tiles
        ]
        for _ in as_completed(futures):
            pass

# ------------------ MAIN ------------------

def main():
    try:
        date_input = input("Enter date (YYYY-MM-DD): ").strip()
        time_input = input("Enter time (HH:MM) [only :15 or :45]: ").strip()

        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_input):
            log_message("Invalid date format. Use YYYY-MM-DD.")
            return
        if not re.match(r"^\d{1,2}:\d{2}$", time_input):
            log_message("Invalid time format. Use HH:MM.")
            return

        time_input = time_input.zfill(5)
        hour, minute = map(int, time_input.split(":"))
        if minute not in [15, 45]:
            log_message("Only :15 and :45 minutes allowed.")
            return

        ist = pytz.timezone("Asia/Kolkata")
        input_dt = ist.localize(datetime.strptime(f"{date_input} {time_input}", "%Y-%m-%d %H:%M"))
        start_dt = input_dt - timedelta(days=NUM_PAST_DAYS)
        current_dt = start_dt

        while current_dt <= input_dt:
            try:
                if current_dt.minute not in [15, 45]:
                    current_dt += timedelta(minutes=30)
                    continue

                dt_utc = current_dt.astimezone(pytz.utc)
                folder_date = dt_utc.strftime("%Y/%d%b").upper()
                file_date = dt_utc.strftime("%d%b%Y").upper()
                time_str_utc = dt_utc.strftime("%H%M")

                wms_url = WMS_BASE_TEMPLATE.format(
                    folder_date=folder_date,
                    file_date=file_date,
                    time=time_str_utc
                )

                log_message(f"Checking availability: {current_dt.strftime('%Y-%m-%d %H:%M')} IST")

                if not validate_wms_availability(wms_url):
                    log_message(f"No data available at {current_dt.strftime('%Y-%m-%d %H:%M')} IST — skipped.")
                    current_dt += timedelta(minutes=30)
                    continue

                log_message(f"Requesting WMS tiles from: {wms_url}")
                date_parts = [current_dt.strftime("%Y"), current_dt.strftime("%m"), current_dt.strftime("%d")]
                tile_dir = os.path.join("RAW_DATA", "INSAT", *date_parts)
                os.makedirs(tile_dir, exist_ok=True)

                tiles = generate_tiles(*FULL_DISK_BBOX, TILE_SIZE_METERS)
                log_message(f"Total tiles to fetch: {len(tiles)}")

                fetch_tiles_concurrently(tiles, tile_dir, wms_url, current_dt, time_str_utc, file_date)
                log_message(f"All tiles saved for {current_dt.strftime('%Y-%m-%d %H:%M')} in: {tile_dir}")
            except Exception:
                log_message(f"Exception during processing {current_dt}:\n{traceback.format_exc()}")

            current_dt += timedelta(minutes=30)

    except Exception:
        log_message(f"Fatal error in main():\n{traceback.format_exc()}")

if __name__ == "__main__":
    main()
