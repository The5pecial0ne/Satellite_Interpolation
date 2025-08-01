let map;
let vectorLayer;
let drawInteraction = null;
let selectedExtent = null;
let overlayLayer = null;
let currentSessionId = null;

document.addEventListener("DOMContentLoaded", () => {
  initMap();
  initDateControls();
  populateTimeOptions("startTimeSelect");
  populateTimeOptions("endTimeSelect");
  populateTimeOptions("previewTimeSelect");

  document.getElementById("combinedBtn").addEventListener("click", fetchAndGenerateVideo);
  document.getElementById("previewBtn").addEventListener("click", previewSelectedFrame);
  document.getElementById("clearPreviewBtn").addEventListener("click", clearPreview);
  document.getElementById("drawBtn").addEventListener("click", enableBboxDrawing);
});

function initMap() {
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
}

function initDateControls() {
  const today = new Date().toISOString().split("T")[0];
  ["startDateSelect", "endDateSelect", "previewDateSelect"].forEach(id => {
    const el = document.getElementById(id);
    el.value = today;
    el.max = today;
  });
}

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
  select.value = `${String(now.getHours()).padStart(2, '0')}:${roundedMin}`;
}

function enableBboxDrawing() {
  if (drawInteraction) map.removeInteraction(drawInteraction);

  drawInteraction = new ol.interaction.Draw({
    source: vectorLayer.getSource(),
    type: "Circle",
    geometryFunction: ol.interaction.Draw.createBox()
  });

  drawInteraction.on("drawstart", () => vectorLayer.getSource().clear());
  drawInteraction.on("drawend", event => {
    selectedExtent = event.feature.getGeometry().getExtent();
    addLog("BBOX drawn on map.");
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

  let bbox4326;
  try {
    bbox4326 = getSelectedBBox4326();
  } catch {
    return;
  }

  const datetime = `${startDate} ${startTime}`;
  const endtime = `${endDate} ${endTime}`;
  const zoom = Math.floor(map.getView().getZoom());
  const bboxStr = bbox4326.map(v => v.toFixed(2)).join(", ");

  addLog(`Started video generation for BBOX = (${bboxStr}) & Time = ${startTime} → ${endTime} IST`);
  updateProgressBar(5);

  fetch("http://localhost:8000/fetch-stitched-frames", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ datetime, endtime, bbox: bbox4326, zoom })
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
        body: JSON.stringify({ session_id: currentSessionId, job_id: jobId })
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
      const win = window.open();
      win.document.write(htmlContent);
      win.document.close();

      addLog("Interpolated video opened in new tab.");
      updateProgressBar(100);
      setTimeout(() => updateProgressBar(0), 10000);
    })
    .catch(err => {
      console.error(err);
      addLog("Error during video generation.");
      updateProgressBar(0);
    });
}

function previewSelectedFrame() {
  const date = document.getElementById("previewDateSelect").value;
  const time = document.getElementById("previewTimeSelect").value;
  if (!date || !time) return alert("Please select both date and time for preview.");

  let bbox4326;
  try {
    bbox4326 = getSelectedBBox4326();
  } catch {
    return;
  }

  const datetime = `${date} ${time}`;
  const zoom = Math.floor(map.getView().getZoom());
  const session = currentSessionId || `session_${Math.random().toString(36).substr(2, 8)}`;
  currentSessionId = session;

  const url = new URL("http://localhost:8000/preview-frame");
  url.searchParams.append("datetime", datetime);
  bbox4326.forEach(v => url.searchParams.append("bbox", v));
  url.searchParams.append("zoom", zoom);
  url.searchParams.append("session_id", session);
  url.searchParams.append("_ts", Date.now());

  const bottomLeft = ol.proj.fromLonLat([bbox4326[0], bbox4326[1]]);
  const topRight = ol.proj.fromLonLat([bbox4326[2], bbox4326[3]]);
  const imageExtent = [...bottomLeft, ...topRight];

  if (overlayLayer) map.removeLayer(overlayLayer);
  overlayLayer = new ol.layer.Image({
    source: new ol.source.ImageStatic({
      url: url.toString(),
      imageExtent: imageExtent,
      projection: map.getView().getProjection()
    }),
    opacity: 0.95
  });

  map.addLayer(overlayLayer);
  const bboxStr = bbox4326.map(v => v.toFixed(2)).join(", ");
  addLog(`Preview overlay added for BBOX = (${bboxStr}) @ ${time} IST`);
}

function clearPreview() {
  if (overlayLayer) {
    map.removeLayer(overlayLayer);
    overlayLayer = null;
  }
  if (vectorLayer) {
    vectorLayer.getSource().clear();
    selectedExtent = null;
  }
  addLog("Preview and BBOX cleared.");
}

function updateProgressBar(percent) {
  document.getElementById("progressBar").style.width = `${percent}%`;
}

function addLog(message) {
  const logList = document.getElementById("logList");

  const divider = document.createElement("div");
  divider.style.margin = "6px 0";
  divider.style.borderTop = "1px dashed #aaa";
  logList.appendChild(divider);

  const entry = document.createElement("div");
  entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
  logList.appendChild(entry);

  logList.scrollTop = logList.scrollHeight;
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
        messages.forEach(msg => {
          if (!shownMessages.has(msg)) {
            addLog(msg);
            shownMessages.add(msg);
          }
        });

        lastCount = messages.length;

        if (messages.some(m => m.includes("complete") || m.includes("Video"))) {
          updateProgressBar(100);
          clearInterval(interval);
        } else {
          updateProgressBar(Math.min(90, lastCount * 10));
        }
      })
      .catch(err => {
        console.error(err);
        clearInterval(interval);
        addLog("Failed to fetch job status.");
      });
  }, 2000);
}
