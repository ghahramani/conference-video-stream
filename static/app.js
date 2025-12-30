// ==========================================
// 1. GLOBAL CONFIG
// ==========================================
const CONFIG = {
    chunkInterval: 100,
    watchdogTimeout: 3000,
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
    if (!streamId) return alert("Enter Room Name");
    document.getElementById('login-overlay').style.display = 'none';
    monitorConnection();
}

async function joinAsPresenter() {
    streamId = document.getElementById('streamInput').value;
    if (!streamId) return alert("Enter Room Name");
    isPresenter = true;
    document.getElementById('login-overlay').style.display = 'none';
    await startPublishing();
    monitorConnection();
}

// ==========================================
// 4. PRESENTER
// ==========================================
async function startPublishing() {
    preventBackgroundThrottling();

    try {
        const constraints = {
            audio: true, // Audio ON
            video: {
                width: {ideal: 640, max: 1280},
                height: {ideal: 480, max: 720},
                frameRate: {ideal: 20, max: 30}
            }
        };

        const stream = await navigator.mediaDevices.getUserMedia(constraints);

        createVideoTile(myPId, true);
        const localVideo = document.getElementById(`video-${myPId}`);
        localVideo.srcObject = stream;
        localVideo.muted = true;

        const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
        const host = window.location.host;
        const wsUrl = `${protocol}://${host}/publish/${streamId}/${myPId}/HIGH`;
        const socket = new WebSocket(wsUrl);
        socket.binaryType = "arraybuffer";

        socket.onopen = () => {
            console.log("🎥 PRESENTER Connected");

            // Allow browser to choose its native format (VP8 or H.264)
            const options = {videoBitsPerSecond: 1000000};
            const recorder = new MediaRecorder(stream, options);

            console.log(`🎙️ Recording MimeType: ${recorder.mimeType}`);

            recorder.ondataavailable = async (event) => {
                if (event.data.size > 0 && socket.readyState === WebSocket.OPEN) {
                    if (socket.bufferedAmount > 64 * 1024) return;
                    socket.send(await event.data.arrayBuffer());
                }
            };

            recorder.start(CONFIG.chunkInterval);

            // Keyframe Loop
            setInterval(() => {
                if (recorder.state === "recording") recorder.requestData();
            }, 1000);
        };

        socket.onclose = () => setTimeout(startPublishing, 2000);

    } catch (err) {
        alert("Camera Failed: " + err.message);
    }
}

function preventBackgroundThrottling() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);
        gain.gain.value = 0;
        osc.start();
    } catch (e) {
    }
}

// ==========================================
// 5. WATCHER
// ==========================================
async function monitorConnection() {
    if (isWatching) return;
    isWatching = true;

    while (isWatching) {
        try {
            await startWatching();
        } catch (err) {
            console.error(err);
        }
        await new Promise(r => setTimeout(r, 2000));
    }
}

async function startWatching() {
    const response = await fetch(`/watch/${streamId}?quality=HIGH`);
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
        const {value, done} = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, {stream: true});
        const lines = buffer.split("\n");
        buffer = lines.pop();

        for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed) continue;
            try {
                const frame = JSON.parse(trimmed.startsWith("data:") ? trimmed.substring(5) : trimmed);
                handleIncomingFrame(frame);
            } catch (e) {
            }
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

    // Lazy Init: We have data, now let's set up the player
    if (!mediaBuffers[frameDTO.pId]) {
        setupPlayerBruteForce(frameDTO.pId, bytes);
    }

    if (!mediaQueues[frameDTO.pId]) mediaQueues[frameDTO.pId] = [];
    mediaQueues[frameDTO.pId].push(bytes);
    processQueue(frameDTO.pId);
}

// ==========================================
// 6. CODEC BRUTE FORCE (THE FIX)
// ==========================================
function setupPlayerBruteForce(pId, firstChunk) {
    const ms = mediaSources[pId];
    if (!ms || ms.readyState !== 'open') return;

    // Log the magic bytes to help debugging if this fails
    const hex = Array.from(firstChunk.slice(0, 8)).map(b => b.toString(16).padStart(2, '0')).join(' ');
    console.log(`🔍 First Packet Bytes: [ ${hex} ]`);

    // 1. Define the candidates
    // Note: iOS Safari produces MP4/H264. Chrome/Android produces WebM/VP8.
    const candidates = [
        'video/webm; codecs="vp8, opus"',           // Preferred WebM
        'video/mp4; codecs="avc1.42E01E, mp4a.40.2"', // Standard H.264
        'video/webm',                               // Generic WebM
        'video/mp4'                                 // Generic MP4
    ];

    let sb = null;

    // 2. Try them one by one
    for (const mime of candidates) {
        if (MediaSource.isTypeSupported(mime)) {
            try {
                console.log(`🧪 Trying Codec: ${mime}`);
                sb = ms.addSourceBuffer(mime);
                // If we get here, the browser accepted the codec string!
                console.log(`✅ Success! Using ${mime}`);
                break;
            } catch (e) {
                console.warn(`❌ Browser rejected ${mime}`);
            }
        }
    }

    if (sb) {
        sb.mode = 'sequence';
        mediaBuffers[pId] = sb;
        sb.addEventListener('updateend', () => processQueue(pId));
    } else {
        console.error("💥 All codecs failed! Browser cannot play this stream.");
        alert("Video format not supported by this browser.");
    }
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
        // Recover from full buffer
        if (e.name === 'QuotaExceededError') {
            const video = document.getElementById(`video-${pId}`);
            try {
                sb.remove(0, video.currentTime - 1);
            } catch (ex) {
            }
        }
    }
}

// ==========================================
// 7. UI
// ==========================================
function createVideoTile(pId, isLocal) {
    if (activePresenters.size >= 4) return;
    activePresenters.add(pId);

    const grid = document.getElementById('video-grid');
    const card = document.createElement('div');
    card.className = 'video-card';
    card.innerHTML = `
        <div style="position: relative; width: 100%; height: 100%;">
            <video id="video-${pId}" autoplay playsinline muted style="width:100%; height:100%; object-fit: cover; background: #000;"></video>
            ${!isLocal ? `<button id="btn-${pId}" style="position: absolute; top: 10px; right: 10px; z-index: 10; padding: 5px;">🔇 Unmute</button>` : ''}
        </div>
        <div class="label">${isLocal ? "Me" : pId}</div>
    `;
    grid.appendChild(card);
    updateGridLayout();

    if (!isLocal) {
        const video = card.querySelector('video');
        const btn = card.querySelector(`#btn-${pId}`);
        if (btn) btn.onclick = () => {
            video.muted = !video.muted;
            btn.innerText = video.muted ? "🔇 Unmute" : "🔊 On";
        };

        const ms = new MediaSource();
        video.src = URL.createObjectURL(ms);
        mediaSources[pId] = ms;
        // Wait for data to call setupPlayerBruteForce
    }
}

function updateGridLayout() {
    const count = activePresenters.size;
    const grid = document.getElementById('video-grid');
    grid.className = '';
    grid.classList.add(`grid-${Math.min(count, 4)}`);
}