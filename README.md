index.html - Simple html for script
script.js - Handles map overlay, sends POST requests to main.py, handles bbox, date, time and other parameters
main.py - Backend for WMS URL logic, tile fetching, stitching, calling Interpolation model to produce intermediate frames and in the future, generate .mp4 video
raw_data_fetcher.py - Python script to fetch ALL tiles for a given date & timestamp using multithreading from WMS - INSAT (default zoom = 5)
