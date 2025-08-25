*index.html* - Simple html for script with progress bar, log and buttons

*script.js* - Handles map overlay, bbox, date, time and other parameters, sends POST requests to main.py

*main.py* - Backend for WMS GeoTIFF file logic, pixel and resolution calculation, previewing a frame, calling Interpolation model to produce intermediate frames and generate .mp4 video

Satellite Interpolation (Root)
|
|->Practical-RIFE/....
|
|->index.html
|
|->script.js
|
|->main.py

frontend - python3 -m http.server 8080
backend - uvicorn main::app
