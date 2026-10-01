mod chat;
mod models;
mod websocket;

use chat::room::RoomManager;
use rocket::{launch, routes};
use std::sync::Arc;
use tokio::sync::RwLock;

#[launch]
fn rocket() -> _ {
    let manager = Arc::new(RwLock::new(RoomManager::new()));

    rocket::build()
        .manage(manager)
        .mount("/", routes![websocket::handler::websocket])
}
