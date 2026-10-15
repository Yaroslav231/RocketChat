use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use common::connection::{ConnectionHandler, ConnectionState};
use common::protocol::{
    Data, Login, Logout, Message, Packet, PacketHeader,
    Request, RequestActiveUsers, SendEncryptedData, User, MAX_PACKET_SIZE
};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock, broadcast};
use uuid::Uuid;
use common::RequestUserById;
use crate::client_events_handler::ClientEventsHandler;
use crate::encryption_installer::EncryptionInstaller;
use crate::object_transport::ObjectTransport;
use crate::request_manager::RequestManager;

#[derive(Debug, Clone)]
pub enum ChatEvent {
    MessageReceived { sender_id: i64, text: String },
    DeliveryConfirmed,
    ConnectionClosed,
    Error(String),
}

struct ClientState {
    user: RwLock<Option<User>>,
}

impl ClientState {
    fn new() -> Self {
        Self {
            user: RwLock::new(None),
        }
    }
}

pub struct ChatApi {
    connection: Mutex<Option<Arc<ConnectionHandler>>>,
    transport: Mutex<Option<Arc<ObjectTransport>>>,
    state: Arc<ClientState>,
    encryption_installer: Arc<EncryptionInstaller>,
    request_manager: Arc<RequestManager>,
    events_tx: broadcast::Sender<ChatEvent>,
}

impl ChatApi {
    pub fn new() -> Self {
        let (events_tx, _) = broadcast::channel(128);

        Self {
            connection: Mutex::new(None),
            transport: Mutex::new(None),
            state: Arc::new(ClientState::new()),
            encryption_installer: Arc::new(EncryptionInstaller::new()),
            request_manager: Arc::new(RequestManager::new()),
            events_tx
        }
    }

    pub fn set_response_timeout(&self, timeout_ms: u64) {
        self.request_manager
            .set_response_timeout(timeout_ms);
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<ChatEvent> {
        self.events_tx.subscribe()
    }

    pub async fn is_connected(&self) -> bool {
        let connection = self.connection.lock().await.clone();

        let Some(connection) = connection else {
            return false;
        };

        let state = connection.connection_state();

        *state.borrow() == ConnectionState::Connected
    }

    pub async fn current_user(&self) -> Option<User> {
        self.state.user.read().await.clone()
    }

    pub async fn connect(
        self: &Arc<Self>,
        host: &str,
        port: u16,
    ) -> Result<()> {
        if self.is_connected().await {
            bail!("Вже є активне підключення до сервера");
        }

        let address = format!("{host}:{port}");

        let stream = TcpStream::connect(&address)
            .await
            .with_context(|| {
                format!("Не вдалося підключитися до {address}")
            })?;

        let connection = Arc::new(ConnectionHandler::new());

        connection.attach(stream).await?;

        let incoming = connection.take_incoming().await;
        let state_rx = connection.connection_state();

        let transport = Arc::new(
            ObjectTransport::new(Arc::clone(&connection))
        );

        *self.connection.lock().await = Some(Arc::clone(&connection));
        *self.transport.lock().await = Some(Arc::clone(&transport));

        let handler = Arc::new(
            ClientEventsHandler::new(
                transport,
                Arc::clone(&self.request_manager),
                Arc::clone(&self.encryption_installer),
                self.events_tx.clone(),
                Arc::downgrade(self),
                Arc::clone(&connection),
            )
        );

        handler.start(incoming, state_rx);

        Ok(())
    }

    pub async fn request_user_by_id(&self, user_id: i64) -> Result<String> {
        let answer = self
            .request(
                |request_id| {
                    Packet::Request(Request::RequestUserById(
                        RequestUserById {
                            request_id,
                            user_id,
                        },
                    ))
                },
                Request::REQUEST_USER_BY_ID_CODE,
                "UserById",
            )
            .await
            .with_context(|| {
                format!("Не вдалося отримати ім'я користувача {user_id}")
            })?;

        let user_name = answer
            .pointer("/data/user_name")
            .or_else(|| answer.pointer("/data/userName"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                anyhow!("У відповіді UserById відсутнє поле user_name")
            })?;

        Ok(user_name.to_owned())
    }

    pub async fn disconnect(&self) -> Result<()> {
        self.request_manager
            .fail_all("З'єднання із сервером закрито")
            .await;

        let connection = self.connection.lock().await.take();

        self.transport.lock().await.take();

        if let Some(connection) = connection {
            connection.close().await;
        }

        *self.state.user.write().await = None;

        self.encryption_installer
            .clear_session_keys()
            .await;

        Ok(())
    }

    pub async fn login(&self, username: &str) -> Result<()> {
        if !self.is_connected().await {
            bail!("Немає підключення до сервера");
        }

        if self.state.user.read().await.is_some() {
            bail!("Користувач уже авторизований");
        }

        let answer = self.request(
            |request_id| {
                Packet::Request(
                    Request::Login(Login {
                        request_id,
                        user_name: username.to_owned(),
                    })
                )
            },
            Request::LOGIN_CODE,
            "LoginConfirm",
        ).await?;

        let user = parse_user_from_answer(&answer)?;

        *self.state.user.write().await = Some(user);

        if let Err(error) =
            self.encryption_installer.send_public_key(self).await
        {
            let _ = self.disconnect().await;
            return Err(error);
        }

        Ok(())
    }

    pub async fn logout(&self) -> Result<()> {
        let user = self.state.user.read().await.clone()
            .ok_or_else(|| {
                anyhow!("Користувач не авторизований")
            })?;

        self.request(
            |request_id| {
                Packet::Request(
                    Request::Logout(Logout {
                        request_id,
                        user_id: user.id,
                    })
                )
            },
            Request::LOGOUT_CODE,
            "LogoutConfirm",
        ).await?;

        *self.state.user.write().await = None;

        self.encryption_installer
            .clear_session_keys()
            .await;

        Ok(())
    }

    pub async fn get_active_users(
        &self,
    ) -> Result<HashMap<i64, User>> {
        let answer = self.request(
            |request_id| {
                Packet::Request(
                    Request::RequestActiveUsers(
                        RequestActiveUsers {
                            request_id,
                        }
                    )
                )
            },
            Request::REQUEST_ACTIVE_USERS_CODE,
            "ActiveUsers",
        ).await?;

        let users_value = answer
            .pointer("/data/active_users")
            .or_else(|| answer.pointer("/data/activeUsers"))
            .or_else(|| answer.pointer("/data/users"))
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "У відповіді ActiveUsers немає поля active_users"
                )
            })?;

        let mut users = HashMap::new();

        match users_value {
            Value::Array(items) => {
                for item in items {
                    let user: User = serde_json::from_value(item)
                        .context(
                            "Не вдалося прочитати користувача зі списку активних"
                        )?;

                    users.insert(user.id, user);
                }
            }

            Value::Object(items) => {
                for (_, item) in items {
                    let user: User = serde_json::from_value(item)
                        .context(
                            "Не вдалося прочитати користувача зі списку активних"
                        )?;

                    users.insert(user.id, user);
                }
            }

            _ => {
                bail!(
                    "Поле active_users має бути масивом або об'єктом"
                );
            }
        }

        Ok(users)
    }

    pub async fn send_message(
        &self,
        recipient_id: i64,
        text: &str,
    ) -> Result<()> {
        let sender = self.state.user.read().await.clone()
            .ok_or_else(|| {
                anyhow!("Спочатку увійдіть до облікового запису")
            })?;

        let message = Message {
            sender_id: sender.id,
            payload: text.as_bytes().to_vec(),
        };

        let data = Data::Message(message);

        let plaintext = serde_json::to_vec(&data)?;

        let encrypted = self.encryption_installer
            .encrypt_for_user(
                recipient_id,
                &plaintext,
                self,
            )
            .await?;

        self.request_with_header(
            move |request_id| {
                Packet::Request(
                    Request::SendEncryptedData(
                        SendEncryptedData {
                            request_id,
                            sender_id: sender.id,
                            ciphertext: encrypted.ciphertext,
                            nonce: encrypted.nonce.to_vec(),
                        }
                    )
                )
            },
            Request::SEND_ENCRYPTED_DATA_CODE,
            "EncryptedDataDelivered",
            Some(recipient_id),
        ).await?;

        Ok(())
    }

    pub(crate) async fn request<F>(
        &self,
        make_packet: F,
        code: i32,
        expected_answer: &str,
    ) -> Result<Value>
    where
        F: FnOnce(String) -> Packet,
    {
        self.request_with_header(
            make_packet,
            code,
            expected_answer,
            None,
        ).await
    }

    async fn request_with_header<F>(
        &self,
        make_packet: F,
        code: i32,
        expected_answer: &str,
        recipient_id: Option<i64>,
    ) -> Result<Value>
    where
        F: FnOnce(String) -> Packet
    {
        let request_id = Uuid::new_v4().to_string();
        let packet = make_packet(request_id.clone());
        let transport = self.get_transport().await?;

        let serialized_size = serde_json::to_vec(&packet)?.len();

        if serialized_size > MAX_PACKET_SIZE {
            bail!("Пакет перевищує максимально дозволений розмір");
        }

        let receiver = self.request_manager
            .create_request(request_id.clone())
            .await?;

        let header = PacketHeader {
            code,
            recipient_id,
        };

        if let Err(error) =
            transport.send_packet(&packet, header).await
        {
            self.request_manager
                .cancel(&request_id)
                .await;

            return Err(error);
        }

        self.request_manager
            .wait_and_get_packet(
                &request_id,
                receiver,
                expected_answer,
            )
            .await
    }

    async fn get_transport(
        &self,
    ) -> Result<Arc<ObjectTransport>> {
        self.transport.lock().await.clone()
            .ok_or_else(|| {
                anyhow!("Немає підключення до сервера")
            })
    }

    pub(crate) async fn handle_connection_lost(
        &self,
        lost_connection: &Arc<ConnectionHandler>,
    ) {
        let mut current = self.connection.lock().await;

        let is_current = current
            .as_ref()
            .is_some_and(|connection| {
                Arc::ptr_eq(connection, lost_connection)
            });

        if !is_current {
            return;
        }

        current.take();
        drop(current);

        self.transport.lock().await.take();

        self.request_manager
            .fail_all("З'єднання із сервером закрито")
            .await;

        *self.state.user.write().await = None;

        self.encryption_installer
            .clear_session_keys()
            .await;
    }
}

impl Default for ChatApi {
    fn default() -> Self {
        Self::new()
    }
}

// Из ответа LoginConfirm извлекаем пользователя.
fn parse_user_from_answer(answer: &Value) -> Result<User> {
    let user = answer
        .get("data")
        .and_then(|data| data.get("user"))
        .cloned()
        .ok_or_else(|| {
            anyhow!("У LoginConfirm немає поля user")
        })?;

    serde_json::from_value(user)
        .context("Не вдалося прочитати User із LoginConfirm")
}