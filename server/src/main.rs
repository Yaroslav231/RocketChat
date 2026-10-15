pub(crate) type ServerResult<T> = anyhow::Result<T>;

mod chat_server_api;
mod client_connection;
mod clients_data_handler;
mod console_interface;
mod object_transport;
mod server_events_handler;


#[tokio::main]
async fn main() {
    console_interface::ServerConsoleInterface::new().start().await;
}
