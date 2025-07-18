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

"""FULL_DISK_BBOX = [
    -21000000,
    -21000000,
     21000000,
     21000000
]"""

"""INDIAN_SUBCONTINENT_BBOX = [
    6679169.45,     
    0.0,          
    11131949.08,  
    4865942.28     
]"""

INDIA_BBOX = [
    7591289.29,   # minX (68.1766°E)
    886131.27,    # minY (7.9655°N)
    10847228.94,  # maxX (97.4026°E)
    4218649.85    # maxY (35.4940°N)
]

# ------------------ LOGGING ------------------

log_file_path = "fetch.log"
file_count_log_path = "file_count.log"

def log_message(message):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
    full_message = timestamp + " => " + message
    print(full_message)

    with open(log_file_path, "a+", encoding="utf-8") as log_file:
        log_file.write(full_message + "\n")
        log_file.seek(0)
        lines = log_file.readlines()

    if len(lines) > 10000:
        with open(log_file_path, "w", encoding="utf-8") as log_file:
            log_file.writelines(lines[-10000:])

def log_file_count_block(timestamp_ist, total, success, failed):
    timestamp_str = timestamp_ist.strftime("%Y-%m-%d %H:%M")
    block = [
        f"=== Timestamp: {timestamp_str} IST ===",
        f"Total Tiles: {total}",
        f"Downloaded: {success}",
        f"Failed: {failed}",
        "----------------------------------------\n"
    ]

    with open(file_count_log_path, "a+", encoding="utf-8") as f:
        f.writelines(line + "\n" for line in block)
        f.seek(0)
        lines = f.readlines()

    if len(lines) > 10000:
        with open(file_count_log_path, "w", encoding="utf-8") as f:
            f.writelines(lines[-10000:])


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

    # Sample a central tile (around the center of the Full disk BBOX)
    """center_x = (FULL_DISK_BBOX[0] + FULL_DISK_BBOX[2]) / 2
    center_y = (FULL_DISK_BBOX[1] + FULL_DISK_BBOX[3]) / 2
    test_bbox = [
        center_x,
        center_y,
        center_x + TILE_SIZE_METERS,
        center_y + TILE_SIZE_METERS
    ]"""

    
    # Sample a central tile (around the center of the Indian Subcontinent BBOX)
    """center_x = (INDIAN_SUBCONTINENT_BBOX[0] + INDIAN_SUBCONTINENT_BBOX[2]) / 2
    center_y = (INDIAN_SUBCONTINENT_BBOX[1] + INDIAN_SUBCONTINENT_BBOX[3]) / 2
    test_bbox = [
        center_x,
        center_y,
        center_x + TILE_SIZE_METERS,
        center_y + TILE_SIZE_METERS
    ]"""

    # Sample a central tile (around the center of the India BBOX)
    center_x = (INDIA_BBOX[0] + INDIA_BBOX[2]) / 2
    center_y = (INDIA_BBOX[1] + INDIA_BBOX[3]) / 2
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
    except Exception as e:
        print(f"Error in validate_wms_availability : {e}")
        log_message(f"Error in validate_wms_availability : {e} \nValidation failed for {wms_url}:\n{traceback.format_exc()}")
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
            filename = f"3RIMG_{file_date}_{time_str_utc}_L1B_STD_V01R00_{ist_str}_BBOX_{bbox_str.replace('.', '_').replace(',', '_')}.png"
            file_path = os.path.join(save_dir, filename)
            image.save(file_path)
            print(f"Tile ({col},{row}) saved with filename : {filename}")
            log_message(f"Tile ({col},{row}) saved with filename : {filename}")
        else:
            log_message(f"Failed ({col},{row}) | Status: {response.status_code}")
    except Exception as e:
        print(f"Error in fetch_and_save_tile : {e}")
        log_message(f"Error in fetch_and_save_tile : {e} \nException while fetching tile ({col},{row}):\n{traceback.format_exc()}")

def fetch_tiles_concurrently(tiles, save_dir, wms_url, timestamp_ist, time_str_utc, file_date):
    success_count = 0
    failure_count = 0

    def wrapped_fetch(col, row, bbox):
        nonlocal success_count, failure_count
        try:
            fetch_and_save_tile(col, row, bbox, save_dir, wms_url, timestamp_ist, time_str_utc, file_date)
            success_count += 1
        except Exception:
            failure_count += 1

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(wrapped_fetch, col, row, bbox)
            for col, row, bbox in tiles
        ]
        for _ in as_completed(futures):
            pass

    total = len(tiles)
    log_file_count_block(timestamp_ist, total, success_count, total - success_count)

# ------------------ MAIN ------------------

def main():
    try:
        # --- Future support for start and end times ---
        # date_input = input("Enter date (YYYY-MM-DD): ").strip()
        # start_time_input = input("Enter start time (HH:MM) [only :15 or :45]: ").strip()
        # end_time_input = input("Enter end time (HH:MM) [only :15 or :45]: ").strip()

        # if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_input):
        #     log_message("Invalid date format. Use YYYY-MM-DD.")
        #     return
        # if not re.match(r"^\d{1,2}:\d{2}$", start_time_input) or not re.match(r"^\d{1,2}:\d{2}$", end_time_input):
        #     log_message("Time format must be HH:MM (e.g. 07:15)")
        #     return

        # start_time_input = start_time_input.zfill(5)
        # end_time_input = end_time_input.zfill(5)

        # ist = pytz.timezone("Asia/Kolkata")
        # start_dt = ist.localize(datetime.strptime(f"{date_input} {start_time_input}", "%Y-%m-%d %H:%M"))
        # end_dt = ist.localize(datetime.strptime(f"{date_input} {end_time_input}", "%Y-%m-%d %H:%M"))

        # if start_dt.minute not in [15, 45] or end_dt.minute not in [15, 45]:
        #     log_message("Frames only available at :15 or :45 of every hour.")
        #     return
        # if start_dt > end_dt:
        #     log_message("Start time must be before end time.")
        #     return

        # --- Current input: reference datetime (loop back 10 days) ---
        ist = pytz.timezone("Asia/Kolkata")
        now_ist = datetime.now(ist)

        # Round down to last available WMS time: either :15 or :45
        minute = now_ist.minute
        if minute < 15:
            rounded_minute = 45
            now_ist = now_ist - timedelta(hours=1)
        elif minute < 45:
            rounded_minute = 15
        else:
            rounded_minute = 45

        last_available_dt = now_ist.replace(minute=rounded_minute, second=0, microsecond=0)

        start_dt = last_available_dt - timedelta(days=NUM_PAST_DAYS)
        current_dt = start_dt

        print(f"Auto-calculated last available WMS timestamp: {last_available_dt.strftime('%Y-%m-%d %H:%M')} IST")
        print(f"Fetching frames from {start_dt.strftime('%Y-%m-%d %H:%M')} IST to {last_available_dt.strftime('%Y-%m-%d %H:%M')} IST")

        log_message(f"Auto-calculated last available WMS timestamp: {last_available_dt.strftime('%Y-%m-%d %H:%M')} IST")
        log_message(f"Fetching frames from {start_dt.strftime('%Y-%m-%d %H:%M')} IST to {last_available_dt.strftime('%Y-%m-%d %H:%M')} IST")

        while current_dt <= last_available_dt:
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

                print(f"Checking availability: {current_dt.strftime('%Y-%m-%d %H:%M')} IST")
                log_message(f"Checking availability: {current_dt.strftime('%Y-%m-%d %H:%M')} IST")

                if not validate_wms_availability(wms_url):
                    print(f"No data available at {current_dt.strftime('%Y-%m-%d %H:%M')} IST — skipped.")
                    log_message(f"No data available at {current_dt.strftime('%Y-%m-%d %H:%M')} IST — skipped.")
                    current_dt += timedelta(minutes=30)
                    continue

                print(f"Requesting WMS tiles from: {wms_url}")
                log_message(f"Requesting WMS tiles from: {wms_url}")
                date_parts = [current_dt.strftime("%Y"), current_dt.strftime("%m"), current_dt.strftime("%d")]
                tile_dir = os.path.join("RAW_DATA", "INSAT", *date_parts)
                os.makedirs(tile_dir, exist_ok=True)

                # tiles = generate_tiles(*FULL_DISK_BBOX, TILE_SIZE_METERS)
                # tiles = generate_tiles(*INDIAN_SUBCONTINENT_BBOX, TILE_SIZE_METERS)
                tiles = generate_tiles(*INDIA_BBOX, TILE_SIZE_METERS)
                print(f"Total tiles to fetch: {len(tiles)}")
                log_message(f"Total tiles to fetch: {len(tiles)}")

                fetch_tiles_concurrently(tiles, tile_dir, wms_url, current_dt, time_str_utc, file_date)
                print(f"All tiles saved for {current_dt.strftime('%Y-%m-%d %H:%M')} in: {tile_dir}")
                log_message(f"All tiles saved for {current_dt.strftime('%Y-%m-%d %H:%M')} in: {tile_dir}")
            except Exception as e:
                print(f"Error in main : {e}")
                log_message(f"Error in main : {e} \nException during processing {current_dt}:\n{traceback.format_exc()}")

            current_dt += timedelta(minutes=30)

    except Exception as e:
        print(f"Error in main : {e}")
        log_message(f"Error in main : {e} \nFatal error in main():\n{traceback.format_exc()}")


if __name__ == "__main__":
    main()
