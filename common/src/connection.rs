use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use crate::{DataPacket, PacketHeader, MAX_PACKET_SIZE};

const CHANNEL_CAPACITY: usize = 128;
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
}

impl ::core::cmp::Eq for ConnectionState {}

pub struct ConnectionHandler {
    state_tx: watch::Sender<ConnectionState>,

    incoming_rx: Mutex<Option<mpsc::Receiver<DataPacket>>>,
    outgoing_tx: RwLock<Option<mpsc::Sender<DataPacket>>>,

    attached: AtomicBool,
}

impl ConnectionHandler {
    pub fn new() -> Self {
        let (state_tx, _) = watch::channel(ConnectionState::Disconnected);

        Self {
            state_tx,
            incoming_rx: Mutex::new(None),
            outgoing_tx: RwLock::new(None),
            attached: AtomicBool::new(false),
        }
    }

    pub async fn attach(&self, stream: TcpStream) -> io::Result<()> {
        if self
            .attached
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "TCP stream is already attached",
            ));
        }

        let (mut reader, mut writer) = stream.into_split();

        let (incoming_tx, incoming_rx) = mpsc::channel::<DataPacket>(CHANNEL_CAPACITY);
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<DataPacket>(CHANNEL_CAPACITY);

        *self.incoming_rx.lock().await = Some(incoming_rx);
        *self.outgoing_tx.write().await = Some(outgoing_tx);

        self.state_tx.send_replace(ConnectionState::Connected);

        let mut reader_state_rx = self.state_tx.subscribe();
        let mut writer_state_rx = self.state_tx.subscribe();

        let reader_state_tx = self.state_tx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = reader_state_rx.changed() => {
                        if changed.is_err()
                            || *reader_state_rx.borrow() == ConnectionState::Disconnected
                        {
                            break;
                        }
                    }

                    result = read_packet(&mut reader) => {
                        match result {
                            Ok(packet) => {
                                if incoming_tx.send(packet).await.is_err() {
                                    break;
                                }
                            }

                            Err(error) => {
                                if error.kind() != io::ErrorKind::UnexpectedEof {
                                    eprintln!("TCP read error: {error}");
                                }

                                break;
                            }
                        }
                    }
                }
            }

            reader_state_tx.send_replace(ConnectionState::Disconnected);
        });

        let writer_state_tx = self.state_tx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = writer_state_rx.changed() => {
                        if changed.is_err()
                            || *writer_state_rx.borrow() == ConnectionState::Disconnected
                        {
                            break;
                        }
                    }

                    packet = outgoing_rx.recv() => {
                        let Some(packet) = packet else {
                            break;
                        };

                        if let Err(error) = write_packet(&mut writer, &packet).await {
                            eprintln!("TCP write error: {error}");
                            break;
                        }
                    }
                }
            }

            writer_state_tx.send_replace(ConnectionState::Disconnected);
        });

        Ok(())
    }

    pub async fn take_incoming(&self) -> mpsc::Receiver<DataPacket> {
        let mut guard = self.incoming_rx.lock().await;

        guard
            .take()
            .expect("take_incoming() must be called after attach() and only once")
    }

    pub async fn send_packet(&self, packet: DataPacket) -> io::Result<()> {
        let sender = self
            .outgoing_tx
            .read()
            .await
            .clone()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "TCP connection is not attached",
                )
            })?;

        if *self.state_tx.subscribe().borrow() != ConnectionState::Connected {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "TCP connection is not connected",
            ));
        }

        sender.send(packet).await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "TCP writer task has stopped",
            )
        })
    }

    pub fn connection_state(&self) -> watch::Receiver<ConnectionState> {
        self.state_tx.subscribe()
    }

    pub async fn close(&self) {
        self.state_tx.send_replace(ConnectionState::Disconnected);
        self.outgoing_tx.write().await.take();
        self.attached.store(false, Ordering::Release);
    }
}

impl Default for ConnectionHandler {
    fn default() -> Self {
        Self::new()
    }
}

async fn read_packet(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
) -> io::Result<DataPacket> {
    let mut length_buf = [0u8; 4];
    reader.read_exact(&mut length_buf).await?;

    let data_len = u32::from_be_bytes(length_buf) as usize;

    if data_len > MAX_PACKET_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Packet payload exceeds the maximum allowed size",
        ));
    }

    let mut code_buf = [0u8; 4];
    reader.read_exact(&mut code_buf).await?;

    let code = i32::from_be_bytes(code_buf);

    let mut recipient_flag = [0u8; 1];
    reader.read_exact(&mut recipient_flag).await?;

    let recipient_id = match recipient_flag[0] {
        0 => None,
        1 => {
            let mut recipient_buf = [0u8; 8];
            reader.read_exact(&mut recipient_buf).await?;
            Some(i64::from_be_bytes(recipient_buf))
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid recipient flag",
            ));
        }
    };

    let mut data = vec![0u8; data_len];
    reader.read_exact(data.as_mut_slice()).await?;

    Ok(DataPacket {
        data,
        header: PacketHeader {
            code,
            recipient_id,
        },
    })
}

async fn write_packet(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    packet: &DataPacket,
) -> io::Result<()> {
    let data_len = u32::try_from(packet.data.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Packet payload is too large",
        )
    })?;

    if packet.data.len() > MAX_PACKET_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Packet payload exceeds the maximum allowed size",
        ));
    }

    writer.write_all(&data_len.to_be_bytes()).await?;
    writer.write_all(&packet.header.code.to_be_bytes()).await?;

    match packet.header.recipient_id {
        Some(recipient_id) => {
            writer.write_all(&[1u8]).await?;
            writer.write_all(&recipient_id.to_be_bytes()).await?;
        }
        None => {
            writer.write_all(&[0u8]).await?;
        }
    }

    writer.write_all(&packet.data).await?;
    writer.flush().await?;

    Ok(())
}