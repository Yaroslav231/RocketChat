use std::{
    io::{Error as IoError, ErrorKind},
    sync::Arc,
};
use std::net::SocketAddr;

use tokio::{
    net::TcpListener,
    sync::{oneshot, Mutex},
    task::JoinHandle,
};

use crate::{
    clients_data_handler::ClientsDataHandler,
    ServerResult
};

struct RunningServer {
    shutdown_tx: oneshot::Sender<()>,
    accept_task: JoinHandle<()>,
}

pub struct ChatServerApi {
    clients_data_handler: Arc<ClientsDataHandler>,
    running: Mutex<Option<RunningServer>>,
}

impl ChatServerApi {
    pub fn new() -> Self {
        Self {
            clients_data_handler:
            ClientsDataHandler::new(),

            running: Mutex::new(None),
        }
    }

    pub async fn start(
        &self,
        port: u16,
    ) -> ServerResult<SocketAddr> {
        let mut running = self.running.lock().await;

        if running
            .as_ref()
            .is_some_and(|server| !server.accept_task.is_finished())
        {
            return Err(
                IoError::new(
                    ErrorKind::AlreadyExists,
                    "Server is already started",
                )
                    .into(),
            );
        }

        // Если предыдущая задача завершилась, очищаем её состояние.
        if let Some(server) = running.take() {
            let _ = server.shutdown_tx.send(());

            if let Err(error) = server.accept_task.await {
                eprintln!("Помилка закінченої задачі серверу: {error}");
            }

            self.clients_data_handler
                .close_all_connections_and_clear_data()
                .await;
        }

        let listener = TcpListener::bind(("0.0.0.0", port)).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let clients_handler = Arc::clone(&self.clients_data_handler);

        let accept_task = tokio::spawn(async move {
            accept_clients(
                listener,
                shutdown_rx,
                clients_handler,
            )
                .await;
        });

        *running = Some(RunningServer {
            shutdown_tx,
            accept_task,
        });

        Ok(addr)
    }

    pub async fn is_running(&self) -> bool {
        self.running
            .lock()
            .await
            .as_ref()
            .is_some_and(|server| !server.accept_task.is_finished())
    }

    pub async fn stop_and_clear(&self) {
        let mut running = self.running.lock().await;

        if let Some(server) = running.take() {
            let _ = server.shutdown_tx.send(());

            if let Err(e) = server.accept_task.await {
                eprintln!("Помилка задачі при зупинці сервера: {e}");
            }
        }

        drop(running);

        self.clients_data_handler
            .close_all_connections_and_clear_data()
            .await;
    }
}

async fn accept_clients(
    listener: TcpListener,
    mut shutdown_rx: oneshot::Receiver<()>,
    clients_handler: Arc<ClientsDataHandler>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                break;
            }

            result = listener.accept() => {
                match result {
                    Ok((socket, address)) => {
                        println!("Підключення: {address}");

                        if let Err(e) =
                            clients_handler
                                .handle_new_client(socket)
                                .await
                        {
                            eprintln!(
                                "Не вдалося підключити клієнта: {e}"
                            );
                        }
                    }

                    Err(e) => {
                        eprintln!(
                            "Помилка прийняття з'єднання: {e}"
                        );
                        break;
                    }
                }
            }
        }
    }
}