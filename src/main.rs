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
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tower_http::services::ServeDir;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

struct Room {
    tx: broadcast::Sender<VideoFrame>,
    headers: HashMap<String, Arc<VideoFrame>>,
    last_heartbeat: Instant,
}

struct AppState {
    rooms: DashMap<String, Room>,
}

#[derive(Clone, Serialize, Debug)]
struct VideoFrame {
    #[serde(rename = "pId")]
    p_id: String,

    // NEW: Field for the user's real name
    #[serde(rename = "name")]
    name: String,

    #[serde(rename = "dataBase64")]
    data_base64: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
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
        // UPDATED ROUTE: Now accepts /publish/{room}/{id}/{quality}/{NAME}
        // Note: Name must be URL encoded in JS
        .route(
            "/publish/{stream_id}/{user_id}/{quality}/{name}",
            get(ws_publish_handler),
        )
        .route("/watch/{stream_id}", get(watch_handler))
        .fallback_service(ServeDir::new("static"))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    tracing::info!("🚀 Rust Server listening on 0.0.0.0:8080");
    axum::serve(listener, app).await.unwrap();
}

async fn ws_publish_handler(
    ws: WebSocketUpgrade,
    // Extract Name from URL path
    Path((stream_id, user_id, _quality, name)): Path<(String, String, String, String)>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // Decode URL-encoded name (e.g. "Navid%20M" -> "Navid M")
    let decoded_name = urlencoding::decode(&name).unwrap_or_default().to_string();
    ws.on_upgrade(move |socket| handle_publisher(socket, stream_id, user_id, decoded_name, state))
}

async fn handle_publisher(
    mut socket: WebSocket,
    stream_id: String,
    user_id: String,
    user_name: String,
    state: Arc<AppState>,
) {
    tracing::info!("🎥 Presenter joined: {} ({})", user_id, user_name);

    let tx = {
        let mut room_entry = state.rooms.entry(stream_id.clone()).or_insert_with(|| {
            let (tx, _rx) = broadcast::channel(128);
            Room {
                tx,
                headers: HashMap::new(),
                last_heartbeat: Instant::now(),
            }
        });
        room_entry.last_heartbeat = Instant::now();
        room_entry.tx.clone()
    };

    let mut header_accumulator: Vec<u8> = Vec::new();

    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Binary(data) = msg {
            if let Some(mut room) = state.rooms.get_mut(&stream_id) {
                room.last_heartbeat = Instant::now();
            }

            if data.is_empty() {
                continue;
            }

            let my_header_exists = if let Some(room) = state.rooms.get(&stream_id) {
                room.headers.contains_key(&user_id)
            } else {
                false
            };

            if my_header_exists {
                let b64 = general_purpose::STANDARD.encode(&data);
                let frame = VideoFrame {
                    p_id: user_id.clone(),
                    name: user_name.clone(), // Include Name in every frame
                    data_base64: b64,
                };
                let _ = tx.send(frame);
            } else {
                header_accumulator.extend(data);

                if header_accumulator.len() >= 4 {
                    let is_webm = header_accumulator[0] == 0x1A
                        && header_accumulator[1] == 0x45
                        && header_accumulator[2] == 0xDF;
                    let is_mp4 = header_accumulator.len() > 8
                        && header_accumulator[4] == 0x66
                        && header_accumulator[5] == 0x74
                        && header_accumulator[6] == 0x79;

                    if is_webm || is_mp4 {
                        let b64 = general_purpose::STANDARD.encode(&header_accumulator);
                        let frame = VideoFrame {
                            p_id: user_id.clone(),
                            name: user_name.clone(),
                            data_base64: b64,
                        };

                        tracing::info!(
                            "💾 SAVED Header for {} ({} bytes)",
                            user_name,
                            header_accumulator.len()
                        );

                        if let Some(mut room) = state.rooms.get_mut(&stream_id) {
                            room.headers
                                .insert(user_id.clone(), Arc::new(frame.clone()));
                        }

                        let _ = tx.send(frame);
                        header_accumulator.clear();
                    } else if header_accumulator.len() > 2000 {
                        header_accumulator.clear();
                    }
                }
            }
        } else if let Message::Close(_) = msg {
            break;
        }
    }

    if let Some(mut room) = state.rooms.get_mut(&stream_id) {
        if room.headers.remove(&user_id).is_some() {
            tracing::info!("🗑️ Removed Header for {}", user_name);
        }
    }
}

async fn watch_handler(
    Path(stream_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    use axum::body::Body;
    use axum::response::Response;

    let (mut rx, headers_snapshot) = match state.rooms.get(&stream_id) {
        Some(room) => {
            let headers: Vec<Arc<VideoFrame>> = room.headers.values().cloned().collect();
            (room.tx.subscribe(), headers)
        }
        None => {
            return Response::builder()
                .status(404)
                .body(Body::from("Stream not found"))
                .unwrap()
        }
    };

    let stream = async_stream::stream! {
        for header in headers_snapshot {
             let json = serde_json::to_string(&*header).unwrap();
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

async fn cleanup_loop(state: Arc<AppState>) {
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let now = Instant::now();
        state.rooms.retain(|id, room| {
            let active = now.duration_since(room.last_heartbeat) < Duration::from_secs(5);
            if !active {
                tracing::info!("🗑️ Garbage Collecting Room: {}", id);
            }
            active
        });
    }
}
