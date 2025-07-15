index.html - Simple html for script


script.js - Handles map overlay, sends POST requests to main.py, handles bbox, date, time and other parameters


main.py - Backend for WMS URL logic, tile fetching, stitching, calling Interpolation model to produce intermediate frames and in the future, generate .mp4 video


raw_data_fetcher.py - Python script to fetch ALL tiles for ALL timestamps (HH:15 and HH:45) upto 10 days prior (modifiable) to selected date using multithreading from WMS - INSAT (default zoom = 5)
