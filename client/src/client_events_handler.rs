use std::sync::{Arc, Weak};

use anyhow::{Context, Result, anyhow, bail};
use common::connection::ConnectionState;
use common::protocol::{Data, DataPacket, Packet, PacketHeader, MAX_PACKET_SIZE, Answer, Request};
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc, watch};
use common::ConnectionHandler;
use crate::chat_api::{ChatApi, ChatEvent};
use crate::encryption_installer::EncryptionInstaller;
use crate::object_transport::ObjectTransport;
use crate::request_manager::{ReceivedResponse, RequestManager};

pub struct ClientEventsHandler {
    transport: Arc<ObjectTransport>,
    request_manager: Arc<RequestManager>,
    encryption_installer: Arc<EncryptionInstaller>,
    events_tx: broadcast::Sender<ChatEvent>,
    api: Weak<ChatApi>,
    connection: Arc<ConnectionHandler>,
}

impl ClientEventsHandler {
    pub fn new(
        transport: Arc<ObjectTransport>,
        request_manager: Arc<RequestManager>,
        encryption_installer: Arc<EncryptionInstaller>,
        events_tx: broadcast::Sender<ChatEvent>,
        api: Weak<ChatApi>,
        connection: Arc<ConnectionHandler>
    ) -> Self {
        Self { transport, request_manager, encryption_installer, events_tx, api, connection }
    }

    pub fn start(
        self: Arc<Self>,
        mut incoming: mpsc::Receiver<DataPacket>,
        mut state_rx: watch::Receiver<ConnectionState>,
    ) {
        tokio::spawn(async move {
            let mut packet_tasks = tokio::task::JoinSet::new();

            loop {
                tokio::select! {
                packet = incoming.recv() => {
                    let Some(packet) = packet else {
                        break;
                    };

                    let handler = Arc::clone(&self);

                    packet_tasks.spawn(async move {
                        if let Err(error) = handler.handle_packet(packet).await {
                            let _ = handler.events_tx.send(
                                ChatEvent::Error(
                                    format!(
                                        "Помилка обробки пакета: {error:#}"
                                    )
                                )
                            );
                        }
                    });
                }

                changed = state_rx.changed() => {
                    if changed.is_err()
                        || *state_rx.borrow() == ConnectionState::Disconnected
                    {
                        break;
                    }
                }

                Some(result) = packet_tasks.join_next(),
                    if !packet_tasks.is_empty() =>
                {
                    if let Err(error) = result {
                        let _ = self.events_tx.send(
                            ChatEvent::Error(
                                format!(
                                    "Задача обробки пакета завершилася аварійно: {error}"
                                )
                            )
                        );
                    }
                }
            }
            }
            packet_tasks.abort_all();

            while packet_tasks.join_next().await.is_some() {}
            if let Some(api) = self.api.upgrade() {
                api.handle_connection_lost(&self.connection).await;
            }
            let _ = self.events_tx.send(ChatEvent::ConnectionClosed);
        });
    }

    async fn handle_packet(&self, packet: DataPacket) -> Result<()> {
        if packet.data.len() > MAX_PACKET_SIZE {
            bail!("Отриманий пакет перевищує максимально дозволений розмір");
        }

        if packet.header.recipient_id.is_some() {
            return self.handle_routed_packet(packet).await;
        }

        let parsed = self.transport.deserialize_packet(&packet)?;
        let value = self.transport.packet_to_value(&parsed)?;
        let category = value.get("kind").and_then(Value::as_str).unwrap_or_default();
        if category != "Answer" {
            bail!("Клієнт отримав локальний пакет категорії '{category}', очікувався Answer");
        }

        let data = value
            .get("data")
            .ok_or_else(|| anyhow!("У відповіді відсутнє поле data"))?;

        let request_id = read_string(data, &["request_id", "requestId"])
            .ok_or_else(|| anyhow!("У відповіді відсутній request_id"))?
            .to_owned();


        let _ = self.request_manager.complete(
            &request_id,
            ReceivedResponse { code: packet.header.code, packet: value },
        ).await;
        Ok(())
    }

    async fn handle_routed_packet(&self, packet: DataPacket) -> Result<()> {
        match packet.header.code {
            Answer::ENCRYPTED_DATA_DELIVERED_CODE => {
                let parsed = self.transport.deserialize_packet(&packet)?;
                let value = self.transport.packet_to_value(&parsed)?;
                let data = value
                    .get("data")
                    .ok_or_else(|| anyhow!("У відповіді відсутнє поле data"))?;
                let request_id = read_string(data, &["request_id", "requestId"])
                    .ok_or_else(|| anyhow!("У відповіді відсутній request_id"))?
                    .to_owned();
                let completed = self.request_manager.complete(
                    &request_id,
                    ReceivedResponse { code: packet.header.code, packet: value },
                ).await;
                if completed {
                    let _ = self.events_tx.send(ChatEvent::DeliveryConfirmed);
                }
                Ok(())
            }
            Request::SEND_ENCRYPTED_DATA_CODE => self.handle_encrypted_message(packet).await,
            other => {
                let _ = self.events_tx.send(ChatEvent::Error(format!(
                    "Пакет з недозволеним маршрутизованим кодом {other} відхилено"
                )));
                Ok(())
            }
        }
    }

    async fn handle_encrypted_message(&self, packet: DataPacket) -> Result<()> {
        let parsed = self.transport.deserialize_packet(&packet)?;
        let value = self.transport.packet_to_value(&parsed)?;
        let data = value.get("data").ok_or_else(|| anyhow!("У пакеті відсутнє поле data"))?;
        let actual_type = data.get("type").and_then(Value::as_str).unwrap_or_default();
        if actual_type != "SendEncryptedData" {
            bail!("Очікувався SendEncryptedData, отримано '{actual_type}'");
        }

        let sender_id = read_i64(data, &["sender_id", "senderId"])
            .ok_or_else(|| anyhow!("SendEncryptedData не містить sender_id"))?;
        let request_id = read_string(data, &["request_id", "requestId"])
            .ok_or_else(|| anyhow!("SendEncryptedData не містить request_id"))?;
        let nonce: Vec<u8> = serde_json::from_value(
            data.get("nonce").cloned().ok_or_else(|| anyhow!("SendEncryptedData не містить nonce"))?
        ).context("nonce має бути масивом байтів")?;
        let ciphertext: Vec<u8> = serde_json::from_value(
            data.get("ciphertext").cloned().ok_or_else(|| anyhow!("SendEncryptedData не містить ciphertext"))?
        ).context("ciphertext має бути масивом байтів")?;

        let api = self.api.upgrade().ok_or_else(|| anyhow!("ChatApi вже закрито"))?;
        let plaintext = self.encryption_installer
            .decrypt_from_user(sender_id, &nonce, &ciphertext, &api)
            .await?;
        let decoded: Data = serde_json::from_slice(&plaintext)
            .context("Розшифровані дані не відповідають моделі Data")?;

        match decoded {
            Data::Message(message) => {
                if message.sender_id != sender_id {
                    bail!("sender_id у Message не збігається з відправником пакета");
                }
                let text = String::from_utf8(message.payload)
                    .context("Повідомлення містить некоректний UTF-8")?;
                let _ = self.events_tx.send(ChatEvent::MessageReceived { sender_id, text });
            }
        }

        let ack_value = json!({
            "kind": "Answer",
            "data": { "type": "EncryptedDataDelivered", "request_id": request_id }
        });
        let ack: Packet = serde_json::from_value(ack_value)
            .context("Не вдалося створити Packet::Answer::EncryptedDataDelivered")?;
        self.transport.send_packet(
            &ack,
            PacketHeader {
                code: Answer::ENCRYPTED_DATA_DELIVERED_CODE,
                recipient_id: Some(sender_id),
            },
        ).await?;
        Ok(())
    }
}

fn read_i64(value: &Value, names: &[&str]) -> Option<i64> {
    names.iter().find_map(|name| value.get(*name).and_then(Value::as_i64))
}

fn read_string<'a>(value: &'a Value, names: &[&str]) -> Option<&'a str> {
    names.iter().find_map(|name| value.get(*name).and_then(Value::as_str))
}
