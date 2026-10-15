use std::sync::Arc;

use anyhow::{Context, Result};
use common::connection::ConnectionHandler;
use common::protocol::{DataPacket, Packet, PacketHeader};

pub struct ObjectTransport {
    connection: Arc<ConnectionHandler>,
}

impl ObjectTransport {
    pub fn new(connection: Arc<ConnectionHandler>) -> Self {
        Self { connection }
    }
    pub async fn send_packet(&self, packet: &Packet, header: PacketHeader) -> Result<()> {
        let data = serde_json::to_vec(packet).context("Не вдалося серіалізувати Packet")?;
        self.send_data_packet(DataPacket { data, header }).await
    }
    pub async fn send_data_packet(&self, packet: DataPacket) -> Result<()> {
        self.connection
            .send_packet(packet)
            .await
            .context("Не вдалося відправити TCP-пакет")
    }
    pub fn deserialize_packet(&self, packet: &DataPacket) -> Result<Packet> {
        serde_json::from_slice::<Packet>(&packet.data)
            .context("TCP-пакет не відповідає моделі Packet")
    }
    pub fn packet_to_value(&self, packet: &Packet) -> Result<serde_json::Value> {
        serde_json::to_value(packet).context("Не вдалося перетворити Packet у JSON value")
    }
}
