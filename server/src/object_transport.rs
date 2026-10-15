use std::sync::Arc;

use common::{
    connection::ConnectionHandler,
    protocol::{
        DataPacket,
        Packet,
        PacketHeader,
    },
    serialization::SerializationService,
};

use crate::ServerResult;

#[derive(Clone)]
pub struct ObjectTransport {
    connection_handler:
        Arc<ConnectionHandler>,
}

impl ObjectTransport {
    pub fn new(
        connection_handler: Arc<ConnectionHandler>,
    ) -> Self {
        Self {
            connection_handler,
        }
    }

    pub async fn send_packet(
        &self,
        packet: Packet,
        header: PacketHeader,
    ) -> ServerResult<()> {
        let data =
            SerializationService::serialize(&packet)?;

        self.connection_handler
            .send_packet(
                DataPacket {
                    data,
                    header,
                }
            )
            .await?;

        Ok(())
    }
}