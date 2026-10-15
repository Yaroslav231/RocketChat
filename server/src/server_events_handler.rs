use std::sync::Arc;

use tokio::sync::mpsc;

use common::{
    connection::ConnectionState,
    protocol::{
        Answer,
        DataPacket,
        Packet,
        is_routable
    },
    serialization::SerializationService,
};

use crate::{
    client_connection::ClientConnection,
    clients_data_handler::ClientsDataHandler,
    ServerResult,
};

pub struct ServerEventsHandler {
    clients_handler: Arc<ClientsDataHandler>,
    client_connection: Arc<ClientConnection>,
    incoming: mpsc::Receiver<DataPacket>,
}

impl ServerEventsHandler {
    pub fn new(
        clients_handler: Arc<ClientsDataHandler>,
        client_connection: Arc<ClientConnection>,
        incoming: mpsc::Receiver<DataPacket>,
    ) -> Self {
        Self {
            clients_handler,
            client_connection,
            incoming,
        }
    }

    pub async fn run(mut self) -> ServerResult<()> {
        let mut state_rx =
            self.client_connection
                .connection_handler
                .connection_state();

        loop {
            if *state_rx.borrow() == ConnectionState::Disconnected {
                break;
            }

            tokio::select! {
                packet = self.incoming.recv() => {
                    let Some(packet) = packet else {
                        break;
                    };

                    self.handle(packet).await?;
                }

                changed = state_rx.changed() => {
                    if changed.is_err()
                        || *state_rx.borrow()
                            == ConnectionState::Disconnected
                    {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle(
        &self,
        packet: DataPacket,
    ) -> ServerResult<()> {
        if packet.header.recipient_id.is_some() {
            if !is_routable(packet.header.code) {
                return Err(anyhow::anyhow!(
                "Packet code {} cannot be routed",
                packet.header.code
            ));
            }

            return self
                .clients_handler
                .route_packet(
                    Arc::clone(&self.client_connection),
                    packet,
                )
                .await;
        }

        let obj: Packet =
            SerializationService::deserialize(&packet.data)?;

        match obj {
            Packet::Request(request) => {
                self.clients_handler
                    .handle_request(
                        Arc::clone(&self.client_connection),
                        request,
                    )
                    .await
            }

            Packet::Answer(Answer::EncryptedDataDelivered(_)) => {
                Err(anyhow::anyhow!(
                "EncryptedDataDelivered must include recipient_id"
            ))
            }

            Packet::Answer(_) => {
                Err(anyhow::anyhow!(
                "Client sent an Answer directly to the server"
            ))
            }
        }
    }
}