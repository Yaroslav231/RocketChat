mod chat_api;
mod request_manager;
mod object_transport;
mod client_events_handler;
mod console_interface;
mod cryptographer;
mod encryption_installer;

use console_interface::ConsoleInterface;

#[tokio::main]
async fn main() {
    ConsoleInterface::new().start().await;
}
