use std::{
    collections::HashMap,
    io::{Error as IoError, ErrorKind},
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
};

use tokio::{
    net::TcpStream,
    sync::RwLock,
};

use common::{connection::ConnectionHandler, protocol::{
    ActiveUsers,
    Answer,
    Confirm,
    DataPacket,
    Error as ProtocolError,
    LoginConfirm,
    LogoutConfirm,
    Packet,
    PacketHeader,
    PublicKey as PublicKeyAnswer,
    Request,
    User,
    RequestActiveUsers,
    Login,
    RegisterPublicKey,
    RequestPublicKey,
    Logout,
}, RequestUserById, UserById};

use crate::{
    client_connection::ClientConnection,
    server_events_handler::ServerEventsHandler,
    ServerResult,
};

struct ServerState {
    connections: HashMap<u64, Arc<ClientConnection>>,
    clients: HashMap<i64, Arc<ClientConnection>>,
    users: HashMap<i64, User>,
    user_names: HashMap<String, i64>,
    public_keys: HashMap<i64, Vec<u8>>,
}

impl ServerState {
    fn new() -> Self {
        Self {
            connections: HashMap::new(),
            clients: HashMap::new(),
            users: HashMap::new(),
            user_names: HashMap::new(),
            public_keys: HashMap::new(),
        }
    }
}

pub struct ClientsDataHandler {
    next_user_id: AtomicI64,
    next_connection_id: AtomicU64,
    state: RwLock<ServerState>,
}

impl ClientsDataHandler {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_user_id: AtomicI64::new(1),
            next_connection_id: AtomicU64::new(1),
            state: RwLock::new(ServerState::new()),
        })
    }

    pub async fn handle_new_client(
        self: &Arc<Self>,
        socket: TcpStream,
    ) -> ServerResult<()> {
        let connection_handler =
            Arc::new(ConnectionHandler::new());

        connection_handler.attach(socket).await?;

        let incoming =
            connection_handler.take_incoming().await;

        let connection_id =
            self.next_connection_id.fetch_add(
                1,
                Ordering::Relaxed,
            );

        let client = Arc::new(
            ClientConnection::new(
                connection_id,
                connection_handler,
            )
        );

        self.state
            .write()
            .await
            .connections
            .insert(
                connection_id,
                Arc::clone(&client),
            );

        let events_handler =
            ServerEventsHandler::new(
                Arc::clone(self),
                Arc::clone(&client),
                incoming,
            );

        let cleanup_handler = Arc::clone(self);
        let cleanup_client = Arc::clone(&client);

        tokio::spawn(async move {
            if let Err(e) = events_handler.run().await {
                eprintln!(
                    "З'єднання {} завершилося з помилкою: {e}",
                    cleanup_client.connection_id
                );
            }

            cleanup_handler
                .disconnect_client(cleanup_client)
                .await;
        });

        Ok(())
    }

    pub async fn handle_request(
        &self,
        client: Arc<ClientConnection>,
        request: Request,
    ) -> ServerResult<()> {
        match request {
            Request::Login(request) => {
                self.handle_login(client, request).await
            }

            Request::RegisterPublicKey(request) => {
                self.handle_register_public_key(
                    client,
                    request,
                )
                    .await
            }

            Request::RequestUserById(request) => {
                self.handle_request_user_by_id(
                    client,
                    request
                ).await
            }

            Request::RequestPublicKey(request) => {
                self.handle_request_public_key(
                    client,
                    request,
                )
                    .await
            }

            Request::RequestActiveUsers(request) => {
                self.handle_request_active_users(
                    client,
                    request,
                )
                    .await
            }

            Request::Logout(request) => {
                self.handle_logout(
                    client,
                    request,
                )
                    .await
            }

            Request::SendEncryptedData(request) => {
                self.send_error(
                    &client,
                    &request.request_id,
                    "SendEncryptedData requires recipient_id",
                )
                    .await
            }
        }
    }

    async fn handle_request_user_by_id(
        &self,
        client: Arc<ClientConnection>,
        request: RequestUserById,
    ) -> ServerResult<()> {
        if client.user_id().is_none() {
            return self
                .send_error(
                    &client,
                    &request.request_id,
                    "Client is not logged in",
                )
                .await;
        }

        let user_name = {
            self.state
                .read()
                .await
                .users
                .get(&request.user_id)
                .map(|user| user.name.clone())
        };

        let Some(user_name) = user_name else {
            return self
                .send_error(
                    &client,
                    &request.request_id,
                    "User not found",
                )
                .await;
        };

        client
            .object_transport
            .send_packet(
                Packet::Answer(Answer::UserById(UserById {
                    request_id: request.request_id,
                    user_name,
                })),
                PacketHeader {
                    code: Answer::USER_BY_ID_CODE,
                    recipient_id: None,
                },
            )
            .await
    }

    async fn handle_login(
        &self,
        client: Arc<ClientConnection>,
        request: Login,
    ) -> ServerResult<()> {
        println!(
            "[LOGIN] Запрос на вхід: connection_id={}, username='{}'",
            client.connection_id,
            request.user_name
        );

        if client.user_id().is_some() {
            println!(
                "[LOGIN] Відмова: клієнт connection_id={} вже авторизований",
                client.connection_id
            );

            return self.send_error(
                &client,
                &request.request_id,
                "Client is already logged in",
            ).await;
        }

        let name = request.user_name.trim().to_owned();
        let attempted_name = name.clone();

        if name.is_empty() {
            println!(
                "[LOGIN] Відмова: пустое імя користувача, connection_id={}",
                client.connection_id
            );

            return self.send_error(
                &client,
                &request.request_id,
                "Username cannot be empty",
            ).await;
        }

        let user = {
            let mut state = self.state.write().await;

            if state.user_names.contains_key(&name) {
                None
            } else {
                let id = self.next_user_id.fetch_add(
                    1,
                    Ordering::Relaxed,
                );

                let user = User {
                    name: name.clone(),
                    id,
                };

                state.user_names.insert(name, id);
                state.users.insert(id, user.clone());
                state.clients.insert(id, Arc::clone(&client));

                client.set_user_id(Some(id));

                Some(user)
            }
        };

        let Some(user) = user else {
            println!(
                "[LOGIN] Відмова: ім'я '{}' вже використовується",
                attempted_name
            );

            return self.send_error(
                &client,
                &request.request_id,
                "Username is already in use",
            ).await;
        };

        let user_id = user.id;
        let username = user.name.clone();

        client.object_transport.send_packet(
            Packet::Answer(
                Answer::LoginConfirm(
                    LoginConfirm {
                        request_id: request.request_id,
                        user,
                    }
                )
            ),
            PacketHeader {
                code: Answer::LOGIN_CONFIRM_CODE,
                recipient_id: None,
            },
        ).await?;

        println!(
            "[LOGIN] Успішний вхід: username='{}', user_id={}, connection_id={}",
            username,
            user_id,
            client.connection_id
        );

        Ok(())
    }

    async fn handle_register_public_key(
        &self,
        client: Arc<ClientConnection>,
        request: RegisterPublicKey,
    ) -> ServerResult<()> {
        println!(
            "[PUBLIC_KEY] Реєстрація ключа: user_id={:?}, bytes={}",
            client.user_id(),
            request.public_key.len()
        );


        let Some(user_id) = client.user_id() else {
            return self.send_error(
                &client,
                &request.request_id,
                "Client is not logged in",
            ).await;
        };

        self.state
            .write()
            .await
            .public_keys
            .insert(
                user_id,
                request.public_key,
            );

        client.object_transport.send_packet(
            Packet::Answer(
                Answer::Confirm(
                    Confirm {
                        request_id: request.request_id,
                    }
                )
            ),
            PacketHeader {
                code: Answer::CONFIRM_CODE,
                recipient_id: None,
            },
        ).await
    }

    async fn handle_request_public_key(
        &self,
        client: Arc<ClientConnection>,
        request: RequestPublicKey,
    ) -> ServerResult<()> {
        println!(
            "[PUBLIC_KEY] Запрос ключа: request_id={}, requester={:?}, recipient_id={}",
            request.request_id,
            client.user_id(),
            request.recipient_id
        );

        if client.user_id().is_none() {
            return self.send_error(
                &client,
                &request.request_id,
                "Client is not logged in",
            ).await;
        }

        let public_key = {
            self.state
                .read()
                .await
                .public_keys
                .get(&request.recipient_id)
                .cloned()
        };

        println!(
            "[PUBLIC_KEY] Ключ получателя {}: {}",
            request.recipient_id,
            if public_key.is_some() {
                "найден"
            } else {
                "не найден"
            }
        );

        let Some(public_key) = public_key else {
            return self.send_error(
                &client,
                &request.request_id,
                "Public key not found",
            ).await;
        };

        let request_id = request.request_id;

        println!(
            "[PUBLIC_KEY] Відправка ключа: request_id={request_id}, recipient_id={}",
            request.recipient_id
        );

        let result = client
            .object_transport
            .send_packet(
                Packet::Answer(Answer::PublicKey(PublicKeyAnswer {
                    request_id: request_id.clone(),
                    public_key,
                })),
                PacketHeader {
                    code: Answer::PUBLIC_KEY_CODE,
                    recipient_id: None,
                },
            )
            .await;

        match result {
            Ok(()) => {
                println!(
                    "[PUBLIC_KEY] Відповідь відправлена: request_id={request_id}"
                );
                Ok(())
            }
            Err(error) => {
                eprintln!(
                    "[PUBLIC_KEY] Помилка відправки: request_id={request_id}: {error}"
                );
                Err(error)
            }
        }
    }

    async fn handle_request_active_users(
        &self,
        client: Arc<ClientConnection>,
        request: RequestActiveUsers,
    ) -> ServerResult<()> {
        if client.user_id().is_none() {
            return self.send_error(
                &client,
                &request.request_id,
                "Client is not logged in",
            ).await;
        }

        let active_users = {
            let state = self.state.read().await;

            state.clients
                .keys()
                .filter_map(|id| state.users.get(id).cloned())
                .collect()
        };

        client.object_transport.send_packet(
            Packet::Answer(
                Answer::ActiveUsers(
                    ActiveUsers {
                        request_id: request.request_id,
                        active_users,
                    }
                )
            ),
            PacketHeader {
                code: Answer::ACTIVE_USERS_CODE,
                recipient_id: None,
            },
        ).await
    }

    pub async fn route_packet(
        &self,
        client: Arc<ClientConnection>,
        packet: DataPacket,
    ) -> ServerResult<()> {
        if client.user_id().is_none() {
            return Err(IoError::new(
                ErrorKind::PermissionDenied,
                "Client is not logged in",
            ).into());
        }

        let recipient_id =
            packet.header.recipient_id.ok_or_else(|| {
                IoError::new(
                    ErrorKind::InvalidInput,
                    "Recipient ID is missing",
                )
            })?;

        let recipient = {
            self.state
                .read()
                .await
                .clients
                .get(&recipient_id)
                .cloned()
        };

        let Some(recipient) = recipient else {
            eprintln!(
                "Отримувач {recipient_id} не в мережі; пакет відкинено"
            );
            return Ok(());
        };

        if let Err(e) =
            recipient
                .connection_handler
                .send_packet(packet)
                .await
        {
            eprintln!(
                "Помилка відправки клієнту {recipient_id}: {e}"
            );

            self.disconnect_client(recipient).await;
        }

        Ok(())
    }

    async fn handle_logout(
        &self,
        client: Arc<ClientConnection>,
        request: Logout,
    ) -> ServerResult<()> {
        println!(
            "[LOGOUT] Запрос на вихід: connection_id={}, user_id={}",
            client.connection_id,
            request.user_id
        );

        let Some(user_id) = client.user_id() else {
            println!(
                "[LOGOUT] Відмова: клієнт connection_id={} не авторизований",
                client.connection_id
            );

            return self.send_error(
                &client,
                &request.request_id,
                "Client is not logged in",
            ).await;
        };

        if request.user_id != user_id {
            println!(
                "[LOGOUT] Відмова: невірний user_id={}, фактичний user_id={}, connection_id={}",
                request.user_id,
                user_id,
                client.connection_id
            );

            return self.send_error(
                &client,
                &request.request_id,
                "Invalid userId",
            ).await;
        }

        client.object_transport.send_packet(
            Packet::Answer(
                Answer::LogoutConfirm(
                    LogoutConfirm {
                        request_id: request.request_id,
                        user_id,
                    }
                )
            ),
            PacketHeader {
                code: Answer::LOGOUT_CONFIRM_CODE,
                recipient_id: None,
            },
        ).await?;

        self.remove_logged_in_user(&client).await;

        println!(
            "[LOGOUT] Успішний вихід: user_id={}, connection_id={}",
            user_id,
            client.connection_id
        );

        Ok(())
    }

    async fn send_error(
        &self,
        client: &ClientConnection,
        request_id: &str,
        message: &str,
    ) -> ServerResult<()> {
        client.object_transport.send_packet(
            Packet::Answer(
                Answer::Error(
                    ProtocolError {
                        request_id: request_id.to_owned(),
                        message: message.to_owned(),
                    }
                )
            ),
            PacketHeader {
                code: Answer::ERROR_CODE,
                recipient_id: None,
            },
        ).await
    }

    async fn remove_logged_in_user(
        &self,
        client: &ClientConnection,
    ) {
        let Some(user_id) = client.take_user_id() else {
            return;
        };

        let mut state = self.state.write().await;

        state.clients.remove(&user_id);
        state.public_keys.remove(&user_id);

        if let Some(user) = state.users.remove(&user_id) {
            state.user_names.remove(&user.name);
        }
    }

    pub async fn disconnect_client(
        &self,
        client: Arc<ClientConnection>,
    ) {
        let user_id = client.take_user_id();

        {
            let mut state = self.state.write().await;

            state.connections.remove(&client.connection_id);

            if let Some(user_id) = user_id {
                state.clients.remove(&user_id);
                state.public_keys.remove(&user_id);

                if let Some(user) = state.users.remove(&user_id) {
                    state.user_names.remove(&user.name);
                }
            }
        }

        client.connection_handler.close().await;
    }

    pub async fn close_all_connections_and_clear_data(&self) {
        let active_clients = {
            let mut state = self.state.write().await;

            let active_clients =
                state.connections.values().cloned().collect::<Vec<_>>();

            state.connections.clear();
            state.clients.clear();
            state.users.clear();
            state.user_names.clear();
            state.public_keys.clear();

            for client in &active_clients {
                client.set_user_id(None);
            }

            active_clients
        };

        for client in active_clients {
            client.connection_handler.close().await;
        }
    }
}