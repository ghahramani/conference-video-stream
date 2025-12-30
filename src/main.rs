use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, State, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use base64::{engine::general_purpose, Engine as _};
use dashmap::DashMap;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tower_http::services::ServeDir;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
// Import for logging

// ==========================================
// 1. STATE MANAGEMENT
// ==========================================

struct Room {
    tx: broadcast::Sender<VideoFrame>,
    last_frame: Arc<VideoFrame>,
    last_heartbeat: Instant,
}

struct AppState {
    rooms: DashMap<String, Room>,
}

#[derive(Clone, Serialize, Debug)]
struct VideoFrame {
    #[serde(rename = "pId")]
    p_id: String,
    #[serde(rename = "dataBase64")]
    data_base64: String,
}

// ==========================================
// 2. MAIN SERVER
// ==========================================

#[tokio::main]
async fn main() {
    // ---------------------------------------------------------
    // 1. ENABLE LOGGING (Explicitly set to INFO)
    // ---------------------------------------------------------
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()), // Force INFO level
        )
        .init();

    let state = Arc::new(AppState {
        rooms: DashMap::new(),
    });

    let cleaner_state = state.clone();
    tokio::spawn(async move {
        cleanup_loop(cleaner_state).await;
    });

    let app = Router::new()
        .route(
            "/publish/{stream_id}/{user_id}/{quality}",
            get(ws_publish_handler),
        )
        .route("/watch/{stream_id}", get(watch_handler))
        .fallback_service(ServeDir::new("static"))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    tracing::info!("🚀 Rust Server listening on 0.0.0.0:8080"); // You should see this now!
    axum::serve(listener, app).await.unwrap();
}

// ==========================================
// 3. PRESENTER
// ==========================================

async fn ws_publish_handler(
    ws: WebSocketUpgrade,
    Path((stream_id, user_id, _quality)): Path<(String, String, String)>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_publisher(socket, stream_id, user_id, state))
}

async fn handle_publisher(
    mut socket: WebSocket,
    stream_id: String,
    user_id: String,
    state: Arc<AppState>,
) {
    tracing::info!("🎥 Presenter joined: {}/{}", stream_id, user_id);

    let tx = {
        let mut room_entry = state.rooms.entry(stream_id.clone()).or_insert_with(|| {
            let (tx, _rx) = broadcast::channel(64);
            Room {
                tx,
                last_frame: Arc::new(VideoFrame {
                    p_id: "".to_string(),
                    data_base64: "".to_string(),
                }),
                last_heartbeat: Instant::now(),
            }
        });
        room_entry.last_heartbeat = Instant::now();
        room_entry.tx.clone()
    };

    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Binary(data) = msg {
            if let Some(mut room) = state.rooms.get_mut(&stream_id) {
                room.last_heartbeat = Instant::now();
            }

            let b64_data = general_purpose::STANDARD.encode(&data);
            let data_len = data.len();

            let frame = VideoFrame {
                p_id: user_id.clone(),
                data_base64: b64_data,
            };

            // =========================================================
            // FIX: STRICT HEADER LOCKING
            // =========================================================
            if let Some(mut room) = state.rooms.get_mut(&stream_id) {
                // Check if the room currently has NO header
                let needs_header = room.last_frame.p_id.is_empty();

                // We ONLY save if we need a header AND this packet is big enough (>100 bytes)
                // We REMOVED the "|| is_owner" check. The owner is NOT allowed to overwrite
                // the header with subsequent video frames.
                if needs_header && data_len > 100 {
                    tracing::info!("💾 LOCKING Header ({} bytes) for {}", data_len, stream_id);
                    room.last_frame = Arc::new(frame.clone());
                }
            }

            let _ = tx.send(frame);
        } else if let Message::Close(_) = msg {
            break;
        }
    }

    // Cleanup: Clear header when I leave, so the next person can set a NEW header
    if let Some(mut room) = state.rooms.get_mut(&stream_id) {
        if room.last_frame.p_id == user_id {
            tracing::info!("🗑️ Unlocking Header for {}", stream_id);
            room.last_frame = Arc::new(VideoFrame {
                p_id: "".to_string(),
                data_base64: "".to_string(),
            });
        }
    }

    tracing::info!("❌ Presenter left: {}", user_id);
}
// ==========================================
// 4. WATCHER
// ==========================================

async fn watch_handler(
    Path(stream_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    use axum::body::Body;
    use axum::response::Response;

    let (mut rx, last_frame) = match state.rooms.get(&stream_id) {
        Some(room) => (room.tx.subscribe(), room.last_frame.clone()),
        None => {
            return Response::builder()
                .status(404)
                .body(Body::from("Stream not found"))
                .unwrap()
        }
    };

    let stream = async_stream::stream! {
        // Send Header
        if !last_frame.p_id.is_empty() {
             let json = serde_json::to_string(&*last_frame).unwrap();
             yield Ok::<_, std::io::Error>(format!("data:{}\n\n", json));
        }

        while let Ok(frame) = rx.recv().await {
            let json = serde_json::to_string(&frame).unwrap();
            yield Ok(format!("data:{}\n\n", json));
        }
    };

    Response::builder()
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("Connection", "keep-alive")
        .header("Access-Control-Allow-Origin", "*")
        .body(Body::from_stream(stream))
        .unwrap()
}

// ==========================================
// 5. CLEANUP
// ==========================================

async fn cleanup_loop(state: Arc<AppState>) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let now = Instant::now();
        state.rooms.retain(|id, room| {
            let active = now.duration_since(room.last_heartbeat) < Duration::from_secs(30);
            if !active {
                tracing::info!("🗑️ Garbage Collecting Room: {}", id);
            }
            active
        });
    }
}
