// ==========================================
// 1. GLOBAL CONFIG (VIDEO ONLY MODE)
// ==========================================
const CONFIG = {
    chunkInterval: 100,      // Fast updates
    watchdogTimeout: 10000,
    // SIMPLEST CODEC POSSIBLE: No Audio, Just VP8 Video
    codec: 'video/webm; codecs=vp8'
};

let streamId = "";
let myPId = "user-" + Math.floor(Math.random() * 100000);
let isPresenter = false;
let isWatching = false;

const activePresenters = new Set();
const lastPacketTime = {};
const mediaBuffers = {};
const mediaSources = {};
const mediaQueues = {};
const mediaSourceReady = {};

// ==========================================
// 2. WATCHDOG
// ==========================================
setInterval(() => {
    const now = Date.now();
    activePresenters.forEach(pId => {
        if (pId === myPId) return;
        if (now - (lastPacketTime[pId] || 0) > CONFIG.watchdogTimeout) {
            console.error(`❌ User ${pId} TIMED OUT.`);
            removePresenter(pId);
        }
    });
}, 2000);

function removePresenter(pId) {
    if (!activePresenters.has(pId)) return;
    activePresenters.delete(pId);
    document.getElementById(`video-${pId}`)?.closest('.video-card')?.remove();
    delete mediaBuffers[pId];
    delete mediaQueues[pId];
    delete mediaSources[pId];
    delete lastPacketTime[pId];
    updateGridLayout();
}

// ==========================================
// 3. JOIN LOGIC
// ==========================================
function joinAsWatcher() {
    streamId = document.getElementById('streamInput').value;
    if(!streamId) return alert("Enter Room Name");
    document.getElementById('login-overlay').style.display = 'none';
    monitorConnection();
}

async function joinAsPresenter() {
    streamId = document.getElementById('streamInput').value;
    if(!streamId) return alert("Enter Room Name");
    isPresenter = true;
    document.getElementById('login-overlay').style.display = 'none';
    await startPublishing();
    monitorConnection(); // Also watch others
}

// ==========================================
// 4. PRESENTER (NO AUDIO)
// ==========================================
async function startPublishing() {
    preventBackgroundThrottling();

    try {
        // !!! VIDEO ONLY - NO AUDIO !!!
        // This prevents the "Sync Freeze" issue
        const constraints = {
            audio: false,
            video: {
                width: { ideal: 640, max: 1280 },
                height: { ideal: 480, max: 720 },
                frameRate: { ideal: 15, max: 30 } // Lower FPS for stability
            }
        };

        const stream = await navigator.mediaDevices.getUserMedia(constraints);

        // Local Preview
        createVideoTile(myPId, true);
        const localVideo = document.getElementById(`video-${myPId}`);
        localVideo.srcObject = stream;
        localVideo.muted = true;

        // WebSocket
        const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
        const host = window.location.host;
        const wsUrl = `${protocol}://${host}/publish/${streamId}/${myPId}/HIGH`;
        const socket = new WebSocket(wsUrl);
        socket.binaryType = "arraybuffer";

        socket.onopen = () => {
            console.log("🎥 PRESENTER (Video Only): Connected");

            const options = {
                mimeType: 'video/webm; codecs=vp8',
                videoBitsPerSecond: 800000 // 800 Kbps
            };

            // Mobile Safari/Chrome fallback
            if (!MediaRecorder.isTypeSupported(options.mimeType)) {
                console.warn("VP8 not supported, falling back to default");
                delete options.mimeType;
            }

            const recorder = new MediaRecorder(stream, options);

            recorder.ondataavailable = async (event) => {
                if (event.data.size > 0 && socket.readyState === WebSocket.OPEN) {
                    if (socket.bufferedAmount > 64 * 1024) return; // Drop if lagging
                    socket.send(await event.data.arrayBuffer());
                }
            };

            recorder.start(100); // 100ms chunks

            // Keyframe Loop
            setInterval(() => {
                if(recorder.state === "recording") recorder.requestData();
            }, 1000);
        };

        socket.onclose = () => setTimeout(startPublishing, 2000);

    } catch (err) {
        alert("Camera Failed: " + err.message);
    }
}

// Keep the hack just in case
function preventBackgroundThrottling() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);
        gain.gain.value = 0;
        osc.start();
    } catch(e) {}
}

// ==========================================
// 5. WATCHER
// ==========================================
async function monitorConnection() {
    if (isWatching) return;
    isWatching = true;

    while (isWatching) {
        try { await startWatching(); }
        catch (err) { console.error(err); }
        await new Promise(r => setTimeout(r, 2000));
    }
}

async function startWatching() {
    const response = await fetch(`/watch/${streamId}?quality=HIGH`);
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop();

        for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed) continue;
            try {
                const frame = JSON.parse(trimmed.startsWith("data:") ? trimmed.substring(5) : trimmed);
                handleIncomingFrame(frame);
            } catch (e) { }
        }
    }
}

function handleIncomingFrame(frameDTO) {
    if (frameDTO.pId === myPId) return;
    lastPacketTime[frameDTO.pId] = Date.now();

    if (!activePresenters.has(frameDTO.pId)) {
        createVideoTile(frameDTO.pId, false);
    }

    const binaryString = window.atob(frameDTO.dataBase64);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
        bytes[i] = binaryString.charCodeAt(i);
    }

    if (!mediaQueues[frameDTO.pId]) mediaQueues[frameDTO.pId] = [];
    mediaQueues[frameDTO.pId].push(bytes);
    processQueue(frameDTO.pId);
}

function processQueue(pId) {
    const sb = mediaBuffers[pId];
    const queue = mediaQueues[pId];
    const ms = mediaSources[pId];

    if (!sb || sb.updating || !queue || queue.length === 0 || !ms || ms.readyState !== 'open') return;

    try {
        const nextChunk = queue.shift();
        sb.appendBuffer(nextChunk);
    } catch (e) {
        // Reset Buffer if full
        if (e.name === 'QuotaExceededError') {
            const video = document.getElementById(`video-${pId}`);
            try { sb.remove(0, video.currentTime - 1); } catch(ex) {}
        }
    }
}

// ==========================================
// 6. UI
// ==========================================
function createVideoTile(pId, isLocal) {
    if (activePresenters.size >= 4) return;
    activePresenters.add(pId);

    const grid = document.getElementById('video-grid');
    const card = document.createElement('div');
    card.className = 'video-card';
    card.innerHTML = `
        <video id="video-${pId}" autoplay playsinline muted style="width:100%; height:100%; object-fit: cover; background: #000;"></video>
        <div class="label">${isLocal ? "Me" : pId}</div>
    `;
    grid.appendChild(card);
    updateGridLayout();

    if (!isLocal) {
        const video = card.querySelector('video');
        const ms = new MediaSource();
        video.src = URL.createObjectURL(ms);
        mediaSources[pId] = ms;

        ms.onsourceopen = () => {
            // GENERIC VP8 ONLY
            const mime = 'video/webm; codecs=vp8';
            if (MediaSource.isTypeSupported(mime)) {
                const sb = ms.addSourceBuffer(mime);
                sb.mode = 'sequence';
                mediaBuffers[pId] = sb;
                sb.addEventListener('updateend', () => processQueue(pId));
                if (mediaQueues[pId]?.length > 0) processQueue(pId);
            } else {
                console.error("Browser rejected VP8");
            }
        };
    }
}

function updateGridLayout() {
    const count = activePresenters.size;
    const grid = document.getElementById('video-grid');
    grid.className = '';
    grid.classList.add(`grid-${Math.min(count, 4)}`);
}