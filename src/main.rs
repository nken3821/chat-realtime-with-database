#[macro_use] extern crate rocket;

mod models;
mod chat;
mod websocket;

use std::sync::{Arc};
use rocket::{launch, routes};
use chat::room::RoomManager;
use tokio::sync::RwLock;




#[launch]
fn rocket() -> _ {
    let manager = Arc::new(RwLock::new(RoomManager::new()));

    rocket::build()
    .manage(manager)
    .mount("/", routes![websocket::handler::websocket])
}
