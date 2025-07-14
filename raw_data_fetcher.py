import os
import math
import requests
from PIL import Image
from io import BytesIO
from datetime import datetime
import re
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------------ CONFIG ------------------

TILE_PX = 256
MAX_WORKERS = 16
ZOOM_LEVEL = 5  # Default zoom; changeable if needed

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

# Expanded full disk
FULL_DISK_BBOX = [
    -21000000,
    -21000000,
     21000000,
     21000000
]

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

def fetch_and_save_tile(col, row, bbox, save_dir, wms_url, timestamp_ist):
    params = COMMON_PARAMS.copy()
    bbox_str = ",".join(map(str, bbox))
    params["BBOX"] = bbox_str

    try:
        response = requests.get(wms_url, params=params, timeout=20)
        if response.status_code == 200:
            image = Image.open(BytesIO(response.content))
            date_prefix = timestamp_ist.strftime("%Y%m%d%H%M%S")
            sanitized_bbox = bbox_str.replace('.', '_').replace(',', '_')
            filename = f"{date_prefix}BBOX={sanitized_bbox}.png"
            file_path = os.path.join(save_dir, filename)

            image.save(file_path)
            print(f"Tile ({col},{row}) saved")
        else:
            print(f"Failed ({col},{row}) | Status: {response.status_code}")
    except Exception as e:
        print(f"Error fetching tile ({col},{row}): {e}")

def fetch_tiles_concurrently(tiles, save_dir, wms_url, timestamp_ist):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(fetch_and_save_tile, col, row, bbox, save_dir, wms_url, timestamp_ist)
            for col, row, bbox in tiles
        ]
        for _ in as_completed(futures):
            pass  # silent sync

# ------------------ MAIN ------------------

def main():
    date_input = input("Enter date (YYYY-MM-DD): ").strip()
    time_input = input("Enter time (HH:MM) [only :15 or :45]: ").strip()

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_input):
        print("Invalid date format. Use YYYY-MM-DD.")
        return
    if not re.match(r"^\d{1,2}:\d{2}$", time_input):
        print("Invalid time format. Use HH:MM.")
        return

    time_input = time_input.zfill(5)

    try:
        ist = pytz.timezone("Asia/Kolkata")
        dt_ist = ist.localize(datetime.strptime(f"{date_input} {time_input}", "%Y-%m-%d %H:%M"))
        if dt_ist.minute not in [15, 45]:
            print("Only :15 and :45 minutes allowed.")
            return
        dt_utc = dt_ist.astimezone(pytz.utc)
    except Exception as e:
        print(f"Invalid input: {e}")
        return

    # Compose WMS URL
    folder_date = dt_utc.strftime("%Y/%d%b").upper()
    file_date = dt_utc.strftime("%d%b%Y").upper()
    time_str = dt_utc.strftime("%H%M")

    wms_url = WMS_BASE_TEMPLATE.format(
        folder_date=folder_date,
        file_date=file_date,
        time=time_str
    )

    print(f"\nRequesting WMS tiles from: {wms_url}")

    # Output directory
    date_parts = [dt_ist.strftime("%Y"), dt_ist.strftime("%m"), dt_ist.strftime("%d")]
    tile_dir = os.path.join("RAW_DATA", "INSAT", *date_parts)
    os.makedirs(tile_dir, exist_ok=True)

    tiles = generate_tiles(*FULL_DISK_BBOX, TILE_SIZE_METERS)
    print(f"Total tiles to fetch: {len(tiles)}")

    fetch_tiles_concurrently(tiles, tile_dir, wms_url, dt_ist)
    print(f"\nAll tiles saved in: {tile_dir}")

if __name__ == "__main__":
    main()
