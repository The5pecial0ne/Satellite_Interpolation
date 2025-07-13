let map;
let overlayLayer = null;
let drawInteraction = null;
let vectorLayer;
let selectedExtent = null;
let selectedBBox4326 = null;
let drawAnchor = null;
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

  populateDateOptions("startDateSelect", 3);
  populateDateOptions("endDateSelect", 3);
  populateTimeOptions("startTimeSelect");
  populateTimeOptions("endTimeSelect");

  document.getElementById("fetchBtn").addEventListener("click", fetchStitchedImage);
  document.getElementById("drawBtn").addEventListener("click", activateDrawBox);
  document.getElementById("interpolateBtn").addEventListener("click", generateVideo);
});

function populateDateOptions(selectId, pastDays = 3) {
  const now = new Date();
  const select = document.getElementById(selectId);
  select.innerHTML = "";

  for (let i = 0; i <= pastDays; i++) {
    const date = new Date(now);
    date.setDate(now.getDate() - i);
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    const option = document.createElement("option");
    option.value = `${yyyy}-${mm}-${dd}`;
    option.textContent = `${dd}-${mm}-${yyyy}`;
    select.appendChild(option);
  }
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
  const currentVal = `${String(now.getHours()).padStart(2, '0')}:${roundedMin}`;
  select.value = currentVal;
}

function activateDrawBox() {
  if (drawInteraction) {
    map.removeInteraction(drawInteraction);
    vectorLayer.getSource().clear();
  }

  const tileSize = 256;
  const resolution = map.getView().getResolution();
  const snapSize = tileSize * resolution;
  map.getTargetElement().style.cursor = "crosshair";

  drawInteraction = new ol.interaction.Draw({
    source: vectorLayer.getSource(),
    type: 'Circle',
    geometryFunction: function (coordinates, geometry) {
      let [start, end] = coordinates;

      if (!drawAnchor) {
        drawAnchor = [...start]; // fixed point
      }

      const anchorX = drawAnchor[0];
      const anchorY = drawAnchor[1];

      const dx = end[0] - anchorX;
      const dy = end[1] - anchorY;

      const size = Math.max(Math.abs(dx), Math.abs(dy));
      const signX = dx < 0 ? -1 : 1;
      const signY = dy < 0 ? -1 : 1;

      const newX = anchorX + signX * size;
      const newY = anchorY + signY * size;

      const minX = Math.min(anchorX, newX);
      const minY = Math.min(anchorY, newY);
      const maxX = Math.max(anchorX, newX);
      const maxY = Math.max(anchorY, newY);

      const coords = [
        [minX, minY],
        [minX, maxY],
        [maxX, maxY],
        [maxX, minY],
        [minX, minY]
      ];

      if (!geometry) {
        geometry = new ol.geom.Polygon([coords]);
      } else {
        geometry.setCoordinates([coords]);
      }

      return geometry;
    }
  });

  drawInteraction.on('drawend', function (evt) {
    const geometry = evt.feature.getGeometry();
    const rawExtent = geometry.getExtent();

    const snap = (val) => Math.round(val / snapSize) * snapSize;
    const minX = snap(rawExtent[0]);
    const minY = snap(rawExtent[1]);
    const maxX = snap(rawExtent[2]);
    const maxY = snap(rawExtent[3]);

    selectedExtent = [minX, minY, maxX, maxY];

    const coords = [
      [minX, minY],
      [minX, maxY],
      [maxX, maxY],
      [maxX, minY],
      [minX, minY]
    ];

    const finalGeom = new ol.geom.Polygon([coords]);
    evt.feature.setGeometry(finalGeom);

    const bottomLeft = ol.proj.toLonLat([minX, minY]);
    const topRight = ol.proj.toLonLat([maxX, maxY]);
    selectedBBox4326 = [bottomLeft[0], bottomLeft[1], topRight[0], topRight[1]];

    drawAnchor = null;
    map.removeInteraction(drawInteraction);
    map.getTargetElement().style.cursor = "default";
    console.log("Selected BBOX (EPSG:4326):", selectedBBox4326);
  });

  map.addInteraction(drawInteraction);
}

function fetchStitchedImage() {
  const startDate = document.getElementById("startDateSelect").value;
  const endDate = document.getElementById("endDateSelect").value;
  const startTime = document.getElementById("startTimeSelect").value;
  const endTime = document.getElementById("endTimeSelect").value;

  if (!startDate || !endDate || !startTime || !endTime) {
    alert("Please select all date and time fields.");
    return;
  }

  if (!selectedBBox4326 || !selectedExtent) {
    alert("Please draw a bounding box.");
    return;
  }

  const datetime = `${startDate} ${startTime}`;
  const endtime = `${endDate} ${endTime}`;
  const zoom = Math.floor(map.getView().getZoom());

  document.getElementById("status").innerText = "Fetching stitched frames...";

  fetch("http://localhost:8000/fetch-stitched-frames", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      datetime: datetime,
      endtime: endtime,
      bbox: selectedBBox4326,
      zoom: zoom
    })
  })
    .then(resp => {
      if (!resp.ok) throw new Error("Fetch failed");
      return resp.json();
    })
    .then(data => {
      currentSessionId = data.directory.split("/").pop();
      document.getElementById("status").innerText = "Frames saved. Ready to interpolate.";
    })
    .catch(err => {
      console.error(err);
      document.getElementById("status").innerText = "Error fetching tiles.";
    });
}

function generateVideo() {
  if (!currentSessionId) {
    alert("No stitched frames found. Please fetch tiles first.");
    return;
  }

  document.getElementById("status").innerText = "Interpolating and generating video...";

  fetch("http://localhost:8000/interpolate-and-generate-video", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: currentSessionId })
  })
    .then(resp => {
      if (!resp.ok) throw new Error("Interpolation failed");
      return resp.json();
    })
    .then(data => {
      const videoUrl = `http://localhost:8000${data.video_path}`;
      window.open(videoUrl, "_blank");
      document.getElementById("status").innerText = "✅ Interpolation and video generation complete.";
    })
    .catch(err => {
      console.error(err);
      document.getElementById("status").innerText = "Error generating video.";
    });
}
