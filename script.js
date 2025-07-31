let map;
let vectorLayer;
let drawInteraction = null;
let selectedExtent = null;
let overlayLayer = null;
let currentSessionId = null;

document.addEventListener("DOMContentLoaded", () => {
  const source = new ol.source.Vector({ wrapX: false });
  vectorLayer = new ol.layer.Vector({ source: source });

  map = new ol.Map({
    target: 'map',
    layers: [
      new ol.layer.Tile({ source: new ol.source.OSM() }),
      vectorLayer
    ],
    view: new ol.View({
      center: ol.proj.fromLonLat([78.9, 20.6]),
      zoom: 5
    })
  });

  const today = new Date();
  const yyyy = today.getFullYear();
  const mm = String(today.getMonth() + 1).padStart(2, '0');
  const dd = String(today.getDate()).padStart(2, '0');
  const todayStr = `${yyyy}-${mm}-${dd}`;

  document.getElementById("startDateSelect").value = todayStr;
  document.getElementById("endDateSelect").value = todayStr;
  document.getElementById("previewDateSelect").value = todayStr;

  document.getElementById("startDateSelect").max = todayStr;
  document.getElementById("endDateSelect").max = todayStr;
  document.getElementById("previewDateSelect").max = todayStr;

  populateTimeOptions("startTimeSelect");
  populateTimeOptions("endTimeSelect");
  populateTimeOptions("previewTimeSelect");

  document.getElementById("combinedBtn").addEventListener("click", fetchAndGenerateVideo);
  document.getElementById("previewBtn").addEventListener("click", previewSelectedFrame);
  document.getElementById("clearPreviewBtn").addEventListener("click", clearPreview);
  document.getElementById("drawBtn").addEventListener("click", enableBboxDrawing);
});

function populateTimeOptions(selectId) {
  const select = document.getElementById(selectId);
  select.innerHTML = "";

  for (let h = 0; h < 24; h++) {
    ["15", "45"].forEach(min => {
      const hh = String(h).padStart(2, '0');
      const option = document.createElement("option");
      option.value = `${hh}:${min}`;
      option.textContent = `${hh}:${min}`;
      select.appendChild(option);
    });
  }

  const now = new Date();
  const roundedMin = now.getMinutes() < 30 ? "15" : "45";
  const currentVal = `${String(now.getHours()).padStart(2, '0')}:${roundedMin}`;
  select.value = currentVal;
}

function enableBboxDrawing() {
  if (drawInteraction) map.removeInteraction(drawInteraction);

  drawInteraction = new ol.interaction.Draw({
    source: vectorLayer.getSource(),
    type: "Circle",
    geometryFunction: ol.interaction.Draw.createBox()
  });

  drawInteraction.on("drawstart", function () {
    // Remove any previously drawn BBOXes
    vectorLayer.getSource().clear();
  });

  drawInteraction.on("drawend", function (event) {
    const geometry = event.feature.getGeometry();
    selectedExtent = geometry.getExtent(); // EPSG:3857
  });

  map.addInteraction(drawInteraction);
}

function getSelectedBBox4326() {
  if (selectedExtent) {
    const bottomLeft = ol.proj.toLonLat([selectedExtent[0], selectedExtent[1]]);
    const topRight = ol.proj.toLonLat([selectedExtent[2], selectedExtent[3]]);
    return [bottomLeft[0], bottomLeft[1], topRight[0], topRight[1]];
  } else {
    alert("Please draw a bounding box using the 'Draw BBOX' button before proceeding.");
    throw new Error("BBOX not drawn.");
  }
}

function fetchAndGenerateVideo() {
  const startDate = document.getElementById("startDateSelect").value;
  const endDate = document.getElementById("endDateSelect").value;
  const startTime = document.getElementById("startTimeSelect").value;
  const endTime = document.getElementById("endTimeSelect").value;

  if (!startDate || !endDate || !startTime || !endTime) {
    alert("Please select all date and time fields.");
    return;
  }

  const startDateTime = new Date(`${startDate}T${startTime}:00`);
  const endDateTime = new Date(`${endDate}T${endTime}:00`);
  if (startDateTime > endDateTime) {
    alert("Start datetime must be before or equal to End datetime.");
    return;
  }

  const datetime = `${startDate} ${startTime}`;
  const endtime = `${endDate} ${endTime}`;
  const zoom = Math.floor(map.getView().getZoom());

  let bbox4326;
  try {
    bbox4326 = getSelectedBBox4326();
  } catch (e) {
    return;
  }

  updateProgressBar(5);
  document.getElementById("status").innerText = "Starting fetch...";

  fetch("http://localhost:8000/fetch-stitched-frames", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      datetime: datetime,
      endtime: endtime,
      bbox: bbox4326,
      zoom: zoom
    })
  })
    .then(resp => {
      if (!resp.ok) throw new Error("Fetch failed");
      return resp.json();
    })
    .then(data => {
      currentSessionId = data.directory.split("/").pop();
      const jobId = data.job_id;

      pollJobStatus(jobId);

      return fetch("http://localhost:8000/interpolate-and-generate-video", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          session_id: currentSessionId,
          job_id: jobId
        })
      });
    })
    .then(resp => {
      if (!resp.ok) throw new Error("Interpolation failed");
      return resp.blob();
    })
    .then(blob => {
      const videoUrl = URL.createObjectURL(blob);
      const htmlContent = `
        <html>
          <head><title>Interpolated Video</title></head>
          <body style="margin:0;background:#000;">
            <video src="${videoUrl}" autoplay loop controls style="width:100vw; height:100vh; object-fit:contain;"></video>
          </body>
        </html>
      `;

      const videoWindow = window.open();
      videoWindow.document.write(htmlContent);
      videoWindow.document.close();

      document.getElementById("status").innerText = "Video ready! Opening in new tab shortly...";
      updateProgressBar(100);
      setTimeout(() => updateProgressBar(0), 10000);
    })
    .catch(err => {
      console.error(err);
      document.getElementById("status").innerText = "Error during processing.";
      updateProgressBar(0);
    });
}

function previewSelectedFrame() {
  const date = document.getElementById("previewDateSelect").value;
  const time = document.getElementById("previewTimeSelect").value;

  if (!date || !time) {
    alert("Please select both date and time for preview.");
    return;
  }

  const datetime = `${date} ${time}`;
  const zoom = Math.floor(map.getView().getZoom());

  let bbox4326;
  try {
    bbox4326 = getSelectedBBox4326();
  } catch (e) {
    return;
  }

  const session = currentSessionId || `session_${Math.random().toString(36).substr(2, 8)}`;
  currentSessionId = session;

  const url = new URL("http://localhost:8000/preview-frame");
  url.searchParams.append("datetime", datetime);
  bbox4326.forEach(val => url.searchParams.append("bbox", val));
  url.searchParams.append("zoom", zoom);
  url.searchParams.append("session_id", session);
  url.searchParams.append("_ts", Date.now()); // <-- Prevent caching

  const bottomLeft = ol.proj.fromLonLat([bbox4326[0], bbox4326[1]]);
  const topRight = ol.proj.fromLonLat([bbox4326[2], bbox4326[3]]);
  const imageExtent = [...bottomLeft, ...topRight];

  // Remove existing overlay
  if (overlayLayer) {
    map.removeLayer(overlayLayer);
    overlayLayer = null;
  }

  // Always create a new ImageStatic source with unique URL
  const imageSource = new ol.source.ImageStatic({
    url: url.toString(),
    imageExtent: imageExtent,
    projection: map.getView().getProjection()
  });

  overlayLayer = new ol.layer.Image({
    source: imageSource,
    opacity: 0.95
  });

  map.addLayer(overlayLayer);
  const logList = document.getElementById("logList");

  const divider = document.createElement("div");
  divider.style.margin = "6px 0";
  divider.style.borderTop = "1px dashed #aaa";
  logList.appendChild(divider);
  
  const logEntry = document.createElement("div");
  const bboxStr = bbox4326.map(v => v.toFixed(2)).join(", ");
  logEntry.textContent = `Preview overlay added to map for BBOX = (${bboxStr}) & Time = ${time} IST`;
  logList.appendChild(logEntry);
  
  logList.scrollTop = logList.scrollHeight;  
}

function clearPreview() {
  if (overlayLayer) {
    map.removeLayer(overlayLayer);
    overlayLayer = null;
  }

  if (vectorLayer) {
    vectorLayer.getSource().clear();  // Clear BBOX from map
    selectedExtent = null;            // Reset selected extent
  }

  const logList = document.getElementById("logList");

  const divider = document.createElement("div");
  divider.style.margin = "6px 0";
  divider.style.borderTop = "1px dashed #aaa";
  logList.appendChild(divider);

  const logEntry = document.createElement("div");
  logEntry.textContent = "Preview and BBOX cleared.";
  logList.appendChild(logEntry);

  logList.scrollTop = logList.scrollHeight;
}


function updateProgressBar(percent) {
  document.getElementById("progressBar").style.width = `${percent}%`;
}

function pollJobStatus(jobId) {
  let lastCount = 0;
  const shownMessages = new Set();

  const interval = setInterval(() => {
    fetch(`http://localhost:8000/job-status/${jobId}`)
      .then(resp => {
        if (!resp.ok) throw new Error("Status fetch failed");
        return resp.json();
      })
      .then(data => {
        const messages = data.status;
        const logList = document.getElementById("logList");

        messages.forEach(msg => {
          if (!shownMessages.has(msg)) {
            const entry = document.createElement("div");
            entry.textContent = msg;
            logList.appendChild(entry);
            shownMessages.add(msg);
          }
        });

        const latest = messages[messages.length - 1];
        if (latest) document.getElementById("status").innerText = latest;

        if (messages.length !== lastCount) {
          lastCount = messages.length;
          updateProgressBar(Math.min(90, lastCount * 10));
        }

        if (latest.includes("complete") || latest.includes("Video ready!")) {
          updateProgressBar(100);
          clearInterval(interval);
        }
      })
      .catch(err => {
        console.error(err);
        clearInterval(interval);
        document.getElementById("status").innerText = "Failed to fetch job status.";
      });
  }, 2000);
}
