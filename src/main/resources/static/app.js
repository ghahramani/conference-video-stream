// State
let streamId = "";
let myPId = "user-" + Math.floor(Math.random() * 10000);
let isPresenter = false;
const activePresenters = new Set(); // To track who is on screen
const mediaBuffers = {}; // pId -> SourceBuffer

// --- Login Logic ---
function joinAsWatcher() {
    streamId = document.getElementById('streamInput').value;
    document.getElementById('login-overlay').style.display = 'none';
    startWatching();
}

async function joinAsPresenter() {
    streamId = document.getElementById('streamInput').value;
    isPresenter = true;
    document.getElementById('login-overlay').style.display = 'none';

    // 1. Setup Local Camera
    await startPublishing();

    // 2. Also watch others (but filter myself out)
    startWatching();
}

// --- PRESENTER LOGIC (Ingest) ---
async function startPublishing() {
    try {
        const stream = await navigator.mediaDevices.getUserMedia({video: true, audio: true});

        // Show local self-view
        createVideoTile(myPId, true);
        const localVideo = document.getElementById(`video-${myPId}`);
        localVideo.srcObject = stream;
        localVideo.muted = true; // Don't hear yourself

        // Connect to Raw WebSocket Handler
        // URL Format: /publish/{streamId}/{pId}/{quality}
        const wsUrl = `ws://localhost:8080/publish/${streamId}/${myPId}/HIGH`;
        const socket = new WebSocket(wsUrl);
        socket.binaryType = "arraybuffer";

        socket.onopen = () => {
            console.log("Connected to Ingest Server");
            const recorder = new MediaRecorder(stream, {mimeType: 'video/webm; codecs=vp8'});

            recorder.ondataavailable = async (event) => {
                if (event.data.size > 0 && socket.readyState === WebSocket.OPEN) {
                    // Send RAW bytes. The Handler parses URL to get pId/Quality.
                    socket.send(await event.data.arrayBuffer());
                }
            };

            // Send small chunks every 100ms for low latency
            recorder.start(100);
        };

        socket.onclose = () => alert("Disconnected from server");

    } catch (err) {
        console.error("Error accessing camera:", err);
        alert("Could not access camera");
    }
}

// --- WATCHER LOGIC (Egress) ---
async function startWatching() {
    console.log("Starting Watch Stream...");

    // Connect to NDJSON Controller
    try {
        const response = await fetch(`/watch/${streamId}?quality=HIGH`);
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
            const {value, done} = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, {stream: true});
            const lines = buffer.split("\n");

            // Handle the buffer edge-case (last line might be incomplete)
            buffer = lines.pop();

            for (const line of lines) {
                if (!line.trim()) continue;
                try {
                    const frame = JSON.parse(line);
                    handleFrame(frame);
                } catch (e) {
                    console.error("Error parsing JSON frame", e);
                }
            }
        }
    } catch (err) {
        console.error("Watch stream failed", err);
    }
}

function handleFrame(frameDTO) {
    // 1. Don't process my own frames (I see myself via local srcObject)
    if (frameDTO.pId === myPId) return;

    // 2. Setup tile if new presenter
    if (!activePresenters.has(frameDTO.pId)) {
        createVideoTile(frameDTO.pId, false);
    }

    // 3. Append Data
    const sb = mediaBuffers[frameDTO.pId];
    if (sb && !sb.updating) {
        // Convert Base64 back to Uint8Array
        const binaryString = window.atob(frameDTO.dataBase64);
        const len = binaryString.length;
        const bytes = new Uint8Array(len);
        for (let i = 0; i < len; i++) {
            bytes[i] = binaryString.charCodeAt(i);
        }

        try {
            sb.appendBuffer(bytes);
        } catch (e) {
            console.warn("Buffer full or error", e);
        }
    }
}

// --- UI / GRID LOGIC ---
function createVideoTile(pId, isLocal) {
    if (activePresenters.size >= 4) return; // Hard limit 4

    activePresenters.add(pId);

    const grid = document.getElementById('video-grid');
    const card = document.createElement('div');
    card.className = 'video-card';
    card.innerHTML = `
        <video id="video-${pId}" autoplay playsinline></video>
        <div class="label">${isLocal ? "Me" : pId}</div>
    `;
    grid.appendChild(card);
    updateGridLayout();

    // If it's NOT local, we need MediaSource to play the incoming stream
    if (!isLocal) {
        const video = card.querySelector('video');
        const ms = new MediaSource();
        video.src = URL.createObjectURL(ms);

        ms.onsourceopen = () => {
            // Must match the Recorder mimeType
            const sb = ms.addSourceBuffer('video/webm; codecs=vp8');
            mediaBuffers[pId] = sb;
        };
    }
}

function updateGridLayout() {
    const count = activePresenters.size;
    const grid = document.getElementById('video-grid');
    // Remove old classes
    grid.className = '';
    // Add new class (grid-1, grid-2, etc.)
    grid.classList.add(`grid-${Math.min(count, 4)}`);
}