const CONFIG = {
    chunkInterval: 250,      // 250ms: Balance between low latency and stability
    watchdogTimeout: 10000,  // 20s: Tolerance for network lag
    codec: 'video/webm; codecs="vp8, opus"' // VP8 + Opus Audio
};

let streamId = "";
let myPId = "user-" + Math.floor(Math.random() * 100000);
let isPresenter = false;
let isWatching = false;

// State
const activePresenters = new Set();
const lastPacketTime = {};
const mediaBuffers = {};
const mediaSources = {};
const mediaQueues = {};
const mediaSourceReady = {};

// Auto cleanup
setInterval(() => {
    const now = Date.now();
    activePresenters.forEach(pId => {
        if (pId === myPId) return;

        const lastSeen = lastPacketTime[pId] || 0;
        const diff = now - lastSeen;

        // Log if we are getting close to timeout
        if (diff > 5000 && diff < CONFIG.watchdogTimeout) {
            console.warn(`User ${pId} lagging... last packet ${diff}ms ago`);
        }

        if (diff > CONFIG.watchdogTimeout) {
            console.error(`User ${pId} TIMED OUT. Removing.`);
            removePresenter(pId);
        }
    });
}, 2000);

function removePresenter(pId) {
    if (!activePresenters.has(pId)) return;
    activePresenters.delete(pId);

    // Cleanup DOM
    const videoEl = document.getElementById(`video-${pId}`);
    if (videoEl) videoEl.closest('.video-card')?.remove();

    // Cleanup Memory
    delete mediaBuffers[pId];
    delete mediaQueues[pId];
    delete mediaSources[pId];
    delete lastPacketTime[pId];

    updateGridLayout();
}

function joinAsWatcher() {
    const input = document.getElementById('streamInput').value;
    if (!input) return alert("Enter Room Name");
    streamId = input;
    document.getElementById('login-overlay').style.display = 'none';
    monitorConnection();
}

async function joinAsPresenter() {
    const input = document.getElementById('streamInput').value;
    if (!input) return alert("Enter Room Name");
    streamId = input;
    isPresenter = true;
    document.getElementById('login-overlay').style.display = 'none';

    // 1. Start Upload
    await startPublishing();
    // 2. Start Download (to see others)
    monitorConnection();
}

// Ingest
async function startPublishing() {
    preventBackgroundThrottling(); // Keep the audio hack

    try {
        // 1. MOBILE OPTIMIZATION: Constraint resolution to 720p or 480p
        // 4K/1080p is too heavy for mobile encoders/upload
        const constraints = {
            audio: true,
            video: {
                width: { ideal: 1280, max: 1280 }, // Limit width
                height: { ideal: 720, max: 720 },  // Limit height
                frameRate: { ideal: 24, max: 30 }  // Limit FPS to save bandwidth
            }
        };

        const stream = await navigator.mediaDevices.getUserMedia(constraints);

        // Local Preview
        createVideoTile(myPId, true);
        const localVideo = document.getElementById(`video-${myPId}`);
        localVideo.srcObject = stream;
        localVideo.muted = true;

        // Dynamic WebSocket URL (Local/Render compatible)
        const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
        const host = window.location.host;
        const wsUrl = `${protocol}://${host}/publish/${streamId}/${myPId}/HIGH`;

        const socket = new WebSocket(wsUrl);
        socket.binaryType = "arraybuffer";

        socket.onopen = () => {
            console.log("📱 Mobile Presenter Connected");

            // 2. LOWER BITRATE: 1 Mbps is much safer for mobile upload
            const options = {
                mimeType: CONFIG.codec,
                videoBitsPerSecond: 1000000 // 1 Mbps (was 2.5 Mbps)
            };

            if (!MediaRecorder.isTypeSupported(options.mimeType)) {
                console.warn("Preferred codec not supported, using default.");
                delete options.mimeType;
            }

            const recorder = new MediaRecorder(stream, options);

            recorder.ondataavailable = async (event) => {
                if (event.data.size > 0 && socket.readyState === WebSocket.OPEN) {

                    // 3. BACKPRESSURE CHECK (CRITICAL FOR MOBILE)
                    // If the browser has > 64KB buffered, it means the network is slow.
                    // DROP this packet to let the network catch up.
                    if (socket.bufferedAmount > 64 * 1024) {
                        console.warn(`🐢 Network slow! Dropping frame. Buffer: ${socket.bufferedAmount}`);
                        return; // <--- SKIP SENDING
                    }

                    socket.send(await event.data.arrayBuffer());
                }
            };

            // 250ms chunks are good balance
            recorder.start(250);

            // Keyframe generator
            setInterval(() => {
                if(recorder.state === "recording") recorder.requestData();
            }, 2000);
        };

        socket.onclose = (e) => {
            console.error("Socket died. Reconnecting...", e);
            setTimeout(startPublishing, 2000);
        };

        // 4. Handle Page Visibility (Switching apps)
        document.addEventListener("visibilitychange", () => {
            if (document.visibilityState === 'visible') {
                if (recorder.state === "paused") recorder.resume();
            }
        });

    } catch (err) {
        alert("Camera Error: " + err.message);
    }
}

// AUDIO CONTEXT HACK: Keeps the tab alive even if minimized
function preventBackgroundThrottling() {
    const AudioContext = window.AudioContext || window.webkitAudioContext;
    if (!AudioContext) return;
    const ctx = new AudioContext();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    gain.gain.value = 0; // Silent
    osc.start();
    console.log("Anti-Throttle Active");
}

// Egress
async function monitorConnection() {
    if (isWatching) return;
    isWatching = true;

    while (isWatching) {
        try {
            console.log("WATCHER: Connecting...");
            await startWatching();
        } catch (err) {
            console.error("WATCHER ERROR:", err);
        }
        console.log("Reconnecting in 2s...");
        await new Promise(r => setTimeout(r, 2000));
    }
}

async function startWatching() {
    const controller = new AbortController();
    const response = await fetch(`/watch/${streamId}?quality=HIGH`, {signal: controller.signal});

    if (!response.ok) throw new Error(`Server Status: ${response.status}`);

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    console.log("WATCHER: Stream Started");

    while (true) {
        const {value, done} = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, {stream: true});
        const lines = buffer.split("\n");
        buffer = lines.pop();

        for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed) continue;

            let jsonString = trimmed.startsWith("data:") ? trimmed.substring(5) : trimmed;

            try {
                const frame = JSON.parse(jsonString);
                handleIncomingFrame(frame);
            } catch (e) {
                // console.warn("Bad JSON", e);
            }
        }
    }
}

// Buffer management
function handleIncomingFrame(frameDTO) {
    if (frameDTO.pId === myPId) return;

    // 1. Reset Watchdog
    lastPacketTime[frameDTO.pId] = Date.now();

    // 2. Setup Tile
    if (!activePresenters.has(frameDTO.pId)) {
        createVideoTile(frameDTO.pId, false);
    }

    // 3. Decode
    const binaryString = window.atob(frameDTO.dataBase64);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
        bytes[i] = binaryString.charCodeAt(i);
    }

    // 4. Queue
    if (!mediaQueues[frameDTO.pId]) mediaQueues[frameDTO.pId] = [];
    mediaQueues[frameDTO.pId].push(bytes);

    // 5. Attempt Process
    processQueue(frameDTO.pId);
}

function processQueue(pId) {
    const sb = mediaBuffers[pId];
    const queue = mediaQueues[pId];
    const ms = mediaSources[pId];

    // DEADLOCK PREVENTION:
    // If SourceBuffer is stuck "updating" for too long, it might be bugged.
    // But usually 'updating' is true just while appending.
    if (!sb || sb.updating || !queue || queue.length === 0 || !ms || ms.readyState !== 'open') {
        return;
    }

    try {
        const nextChunk = queue.shift();
        sb.appendBuffer(nextChunk);
    } catch (e) {
        console.error("Append Error:", e);

        // Recover from QuotaExceeded
        if (e.name === 'QuotaExceededError') {
            const video = document.getElementById(`video-${pId}`);
            if (sb.buffered.length > 0) {
                const removeEnd = video.currentTime - 3;
                if (removeEnd > 0) {
                    try {
                        sb.remove(0, removeEnd);
                    } catch (ex) {
                    }
                }
            }
        } else {
            // If it's another error, putting it back might just loop error.
            // Better to drop it and wait for next keyframe.
            console.warn("Dropping bad chunk");
        }
    }
}

// UI and Grid
function createVideoTile(pId, isLocal) {
    if (activePresenters.size >= 4) return;
    activePresenters.add(pId);

    const grid = document.getElementById('video-grid');
    const card = document.createElement('div');
    card.className = 'video-card';
    card.innerHTML = `
        <video id="video-${pId}" autoplay playsinline muted></video>
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
            console.log(`MediaSource OPEN for ${pId}`);

            // Verify Codec Support
            if (MediaSource.isTypeSupported(CONFIG.codec)) {
                const sb = ms.addSourceBuffer(CONFIG.codec);
                sb.mode = 'sequence'; // Critical for live
                mediaBuffers[pId] = sb;
                mediaSourceReady[pId] = true;

                // TRIGGER LOOP
                sb.addEventListener('updateend', () => processQueue(pId));

                // UNBLOCK: If data arrived while we were initializing, flush it now
                if (mediaQueues[pId] && mediaQueues[pId].length > 0) {
                    processQueue(pId);
                }
            } else {
                console.error("Browser does not support VP8/Opus!");
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