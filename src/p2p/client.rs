use std::collections::HashMap;
use crate::p2p::channel::{create_client_channel, create_signal_channel};
use crate::p2p::message::Message;
use crate::p2p::message::Message::PingMessage;
use crate::utils::sleep;
use local_ip_address::local_ip;
use std::ops::Deref;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::{io, select, spawn};

type ChannelSignalSender = Arc<mpsc::Sender<Message>>;
type ChannelSignalReceiver = mpsc::Receiver<Message>;
type ClientConMap = Arc<Mutex<HashMap<String, ChannelSignalSender>>>;

pub struct P2PClient {
    client_con_map: ClientConMap,
}

impl P2PClient {
    pub fn new() -> Self {
        Self {
            client_con_map: Arc::new(Default::default()),
        }
    }

    pub async fn add_con(&self, server_url: &str) -> crate::Result<()> {
        let (signal_channel_tx, signal_channel_rx) = create_signal_channel();
        let con = ClientCon::new(
            server_url.to_string(),
            signal_channel_rx
        );
        con.start()?;
        self.client_con_map.lock().await.insert(server_url.to_string(), signal_channel_tx);
        Ok(())
    }

    pub async fn call(&self, server_url: &str, message: Message) -> crate::Result<()> {
        if let Some(sender) = self.client_con_map.lock().await.get(server_url) {
            sender.send(message).await?
        }
        Ok(())
    }
}

impl Default for P2PClient {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ClientCon {
    server_url: String,
    signal_channel_rx: ChannelSignalReceiver,
}

impl ClientCon {
    pub fn new(
        server_url: String,
        signal_channel_rx: ChannelSignalReceiver,
    ) -> Self {
        Self {
            server_url,
            signal_channel_rx,
        }
    }

    pub fn start(mut self) -> crate::Result<()> {
        spawn(async move {
            loop {
                match self.connect().await {
                    Ok(_) => println!("Connection closed"),
                    Err(_) => println!("Connection exception"),
                }
                sleep(5000).await;
            }
        });
        Ok(())
    }

    async fn connect(&mut self) -> crate::Result<()> {
        let socket = TcpSocket::new_v4()?;
        let addr = self.server_url.parse()?;
        let stream = socket.connect(addr).await?;
        let (r, w) = io::split(stream);
        println!("Client connected to {}", self.server_url);

        let (socket_close_tx, mut socket_close_rx) = broadcast::channel(1);
        let (channel_tx, channel_rx) = create_client_channel();
        let ping_channel_tx = channel_tx.clone();

        let socket_close_write_rx = socket_close_tx.subscribe();
        let socket_close_ping_rx = socket_close_tx.subscribe();

        self.start_socket_reader(r, socket_close_tx);
        self.start_socket_writer(w, channel_rx, socket_close_write_rx);
        self.start_pinger(ping_channel_tx, socket_close_ping_rx);

        loop {
            select! {
                Some(signal) = self.signal_channel_rx.recv() => {
                    channel_tx.send(signal).await.unwrap_or_default();
                }
                _ = socket_close_rx.recv() => {
                    break;
                }
            }
        }

        Ok(())
    }

    fn start_socket_reader(
        &self,
        mut r: ReadHalf<TcpStream>,
        socket_close_tx: broadcast::Sender<()>,
    ) {
        // Socket read handler thread, to handle message sent by server
        spawn(async move {
            let mut buf = vec![0; 1024];
            loop {
                match r.read(&mut buf).await {
                    Ok(0) => {
                        socket_close_tx.send(()).unwrap_or_default();
                        println!("Socket closed by server");
                        return;
                    }
                    Ok(_n) => {
                        let message: Message = String::from_utf8_lossy(&buf).deref().into();
                        println!("{:?}", message);
                    }
                    Err(_) => {
                        socket_close_tx.send(()).unwrap_or_default();
                        println!("Socket exception");
                        return;
                    }
                }
            }
        });
    }

    fn start_socket_writer(
        &self,
        mut w: WriteHalf<TcpStream>,
        mut channel_rx: mpsc::Receiver<Message>,
        mut socket_close_write_rx: broadcast::Receiver<()>,
    ) {
        spawn(async move {
            loop {
                select! {
                    Some(signal) = channel_rx.recv() => {
                        let message_bytes: Vec<u8> = signal.into();
                        w.write_all(&message_bytes).await.unwrap();
                        println!("Message sent");
                    }
                    _ = socket_close_write_rx.recv() => {
                        break;
                    }
                }
            }
        });
    }

    fn start_pinger(
        &self,
        ping_channel_tx: Arc<mpsc::Sender<Message>>,
        mut socket_close_ping_rx: broadcast::Receiver<()>,
    ) {
        // Ping recycle thread
        spawn(async move {
            select! {
                _ = socket_close_ping_rx.recv() => {
                    println!("Ping stopped");
                }
                _ = ping(ping_channel_tx) => {
                    println!("Ping over");
                }
            }
        });
    }
}

async fn ping(channel_tx: ChannelSignalSender) {
    loop {
        sleep(5000).await;
        println!("===Ping start");
        let ip_address = local_ip().unwrap();
        let ping_message = PingMessage {
            address: ip_address.to_string(),
        };
        channel_tx.send(ping_message).await.unwrap();
    }
}
