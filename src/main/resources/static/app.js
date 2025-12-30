// ==========================================
// 1. GLOBAL CONFIG & STATE
// ==========================================
const CONFIG = {
    // 250ms is the "Sweet Spot" for mobile stability
    chunkInterval: 250,
    // 20s tolerance for bad mobile networks before removing a user
    watchdogTimeout: 10000,
    // Preferred Codec (Standard WebRTC profile)
    codec: 'video/webm; codecs="vp8, opus"'
};

let streamId = "";
let myPId = "user-" + Math.floor(Math.random() * 100000);
let isPresenter = false;
let isWatching = false;

// UI & Logic State
const activePresenters = new Set();
const lastPacketTime = {};
const stuckMonitors = {}; // Tracks if video is frozen

// Media Processing State
const mediaBuffers = {};   // pId -> SourceBuffer
const mediaSources = {};   // pId -> MediaSource
const mediaQueues = {};    // pId -> Array<Uint8Array>
const mediaSourceReady = {};

// ==========================================
// 2. HEALTH MONITORS (The Fixes)
// ==========================================

// A. WATCHDOG: Removes users who truly disconnected
setInterval(() => {
    const now = Date.now();
    activePresenters.forEach(pId => {
        if (pId === myPId) return;

        const lastSeen = lastPacketTime[pId] || 0;
        if (now - lastSeen > CONFIG.watchdogTimeout) {
            console.error(`❌ User ${pId} TIMED OUT. Removing.`);
            removePresenter(pId);
        }
    });
}, 2000);

// B. VIDEO CPR: Detects if video is stuck and jumpstarts it
// This fixes the "Frozen Image" issue on Watchers
setInterval(() => {
    activePresenters.forEach(pId => {
        const video = document.getElementById(`video-${pId}`);
        const sb = mediaBuffers[pId];

        if (!video || !sb || video.paused || sb.buffered.length === 0) return;

        if (!stuckMonitors[pId]) stuckMonitors[pId] = { lastTime: 0, stuckCount: 0 };
        const monitor = stuckMonitors[pId];

        // Check if playback time has moved since last second
        if (Math.abs(video.currentTime - monitor.lastTime) < 0.1) {
            monitor.stuckCount++;
        } else {
            monitor.stuckCount = 0;
            monitor.lastTime = video.currentTime;
        }

        // If stuck for > 3 seconds, NUDGE the player
        if (monitor.stuckCount > 3) {
            console.warn(`⚠️ Video ${pId} frozen! Jumpstarting...`);
            const end = sb.buffered.end(sb.buffered.length - 1);

            // If we are way behind live edge, jump to edge
            if (end - video.currentTime > 2) {
                video.currentTime = end - 0.1;
            } else {
                video.currentTime += 0.1; // Tiny nudge
            }
            // Reset counter logic to prevent infinite jumping
            if (monitor.stuckCount > 10) monitor.stuckCount = 0;
        }
    });
}, 1000);

function removePresenter(pId) {
    if (!activePresenters.has(pId)) return;
    activePresenters.delete(pId);

    const videoEl = document.getElementById(`video-${pId}`);
    if (videoEl) videoEl.closest('.video-card')?.remove();

    delete mediaBuffers[pId];
    delete mediaQueues[pId];
    delete mediaSources[pId];
    delete lastPacketTime[pId];
    delete stuckMonitors[pId];

    updateGridLayout();
}

// ==========================================
// 3. LOGIN / JOIN
// ==========================================
function joinAsWatcher() {
    const input = document.getElementById('streamInput').value;
    if(!input) return alert("Enter Room Name");
    streamId = input;
    document.getElementById('login-overlay').style.display = 'none';
    monitorConnection();
}

async function joinAsPresenter() {
    const input = document.getElementById('streamInput').value;
    if(!input) return alert("Enter Room Name");
    streamId = input;
    isPresenter = true;
    document.getElementById('login-overlay').style.display = 'none';

    await startPublishing();
    monitorConnection();
}

// ==========================================
// 4. PRESENTER (INGEST)
// ==========================================
async function startPublishing() {
    // 1. Hack to keep tab alive in background
    preventBackgroundThrottling();

    try {
        // 2. MOBILE OPTIMIZATION: Constraint resolution
        // 720p is max safe resolution for mobile web uploads
        const constraints = {
            audio: true,
            video: {
                width: { ideal: 1280, max: 1280 },
                height: { ideal: 720, max: 720 },
                frameRate: { ideal: 24, max: 30 }
            }
        };

        const stream = await navigator.mediaDevices.getUserMedia(constraints);

        // Local Preview
        createVideoTile(myPId, true);
        const localVideo = document.getElementById(`video-${myPId}`);
        localVideo.srcObject = stream;
        localVideo.muted = true;

        // 3. Dynamic WebSocket (WSS for Render/Prod)
        const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
        const host = window.location.host;
        const wsUrl = `${protocol}://${host}/publish/${streamId}/${myPId}/HIGH`;

        const socket = new WebSocket(wsUrl);
        socket.binaryType = "arraybuffer";

        socket.onopen = () => {
            console.log("🎥 PRESENTER: Connected");

            // 4. BITRATE: 1 Mbps is safe for mobile 4G/5G
            const options = {
                mimeType: CONFIG.codec,
                videoBitsPerSecond: 1000000
            };

            // Fallback if browser hates VP8
            if (!MediaRecorder.isTypeSupported(options.mimeType)) {
                console.warn(`Codec ${options.mimeType} not supported, using default.`);
                delete options.mimeType;
            }

            const recorder = new MediaRecorder(stream, options);

            recorder.ondataavailable = async (event) => {
                if (event.data.size > 0 && socket.readyState === WebSocket.OPEN) {

                    // 5. CLIENT-SIDE BACKPRESSURE (Crucial for Mobile)
                    // If buffer is > 64KB, network is lagging. Drop frame to stay live.
                    if (socket.bufferedAmount > 64 * 1024) {
                        console.warn(`🐢 Network slow! Dropping frame. Buffer: ${socket.bufferedAmount}`);
                        return;
                    }

                    socket.send(await event.data.arrayBuffer());
                }
            };

            recorder.start(CONFIG.chunkInterval);

            // Keyframe Generator (Force full frame every 2s)
            setInterval(() => {
                if(recorder.state === "recording") recorder.requestData();
            }, 2000);
        };

        socket.onclose = (e) => {
            console.error(`⚠️ Socket Closed: ${e.code}`);
            if (e.code === 1009) alert("Packet too big! Check server config.");
            else setTimeout(startPublishing, 2000); // Auto-retry
        };

    } catch (err) {
        console.error("Camera Error:", err);
        alert("Camera Failed: " + err.message);
    }
}

function preventBackgroundThrottling() {
    const AudioContext = window.AudioContext || window.webkitAudioContext;
    if (!AudioContext) return;
    const ctx = new AudioContext();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    gain.gain.value = 0;
    osc.start();
}

// ==========================================
// 5. WATCHER (EGRESS LOOP)
// ==========================================
async function monitorConnection() {
    if (isWatching) return;
    isWatching = true;

    while (isWatching) {
        try {
            console.log("🔌 WATCHER: Connecting...");
            await startWatching();
        } catch (err) {
            console.error("❌ WATCHER ERROR:", err);
        }
        console.log("♻️ Reconnecting Watcher in 2s...");
        await new Promise(r => setTimeout(r, 2000));
    }
}

async function startWatching() {
    const controller = new AbortController();
    const response = await fetch(`/watch/${streamId}?quality=HIGH`, { signal: controller.signal });

    if (!response.ok) throw new Error(`Server Status: ${response.status}`);

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    console.log("✅ WATCHER: Stream Started");

    while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop();

        for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed) continue;

            let jsonString = trimmed.startsWith("data:") ? trimmed.substring(5) : trimmed;

            try {
                const frame = JSON.parse(jsonString);
                handleIncomingFrame(frame);
            } catch (e) { }
        }
    }
}

// ==========================================
// 6. BUFFER & QUEUE MANAGEMENT
// ==========================================
function handleIncomingFrame(frameDTO) {
    if (frameDTO.pId === myPId) return;

    // Refresh Watchdog
    lastPacketTime[frameDTO.pId] = Date.now();

    // Create Tile if new
    if (!activePresenters.has(frameDTO.pId)) {
        createVideoTile(frameDTO.pId, false);
    }

    // Decode
    const binaryString = window.atob(frameDTO.dataBase64);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
        bytes[i] = binaryString.charCodeAt(i);
    }

    // Queue
    if (!mediaQueues[frameDTO.pId]) mediaQueues[frameDTO.pId] = [];
    mediaQueues[frameDTO.pId].push(bytes);

    processQueue(frameDTO.pId);
}

function processQueue(pId) {
    const sb = mediaBuffers[pId];
    const queue = mediaQueues[pId];
    const ms = mediaSources[pId];

    // If sourceBuffer is busy (updating), we must wait.
    // The 'updateend' listener will trigger this function again.
    if (!sb || sb.updating || !queue || queue.length === 0 || !ms || ms.readyState !== 'open') return;

    try {
        const nextChunk = queue.shift();
        sb.appendBuffer(nextChunk);
    } catch (e) {
        console.error("Append Error:", e);

        // Handle Full Buffer (Garbage Collection)
        if (e.name === 'QuotaExceededError') {
            const video = document.getElementById(`video-${pId}`);
            if (sb.buffered.length > 0) {
                const removeEnd = video.currentTime - 3;
                if (removeEnd > 0) {
                    try { sb.remove(0, removeEnd); } catch(ex) {}
                }
            }
        } else {
            // For other errors (InvalidState, Corrupt Chunk), DROP IT.
            // Do NOT unshift it back, or we loop forever on bad data.
            console.warn("🗑️ Dropping corrupt chunk");
        }
    }
}

// ==========================================
// 7. UI LOGIC
// ==========================================
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
            if (MediaSource.isTypeSupported(CONFIG.codec)) {
                const sb = ms.addSourceBuffer(CONFIG.codec);
                sb.mode = 'sequence';
                mediaBuffers[pId] = sb;
                mediaSourceReady[pId] = true;

                sb.addEventListener('updateend', () => processQueue(pId));

                // Flush any early data
                if (mediaQueues[pId] && mediaQueues[pId].length > 0) {
                    processQueue(pId);
                }
            } else {
                console.error("Browser does not support codec:", CONFIG.codec);
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