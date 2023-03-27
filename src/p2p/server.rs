use crate::config::config_ring_port_or_default;
use crate::p2p::channel::ChannelSignal::{ConnectionClose, ConnectionError, RemoteMessage};
use crate::p2p::channel::{create_connection_channel, create_server_channel, ChannelSignal};
use crate::p2p::message::Message;
use local_ip_address::linux::local_ip;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::{io, spawn};

type ConnectionReceiver = Arc<Mutex<Receiver<Vec<u8>>>>;
type ServerConMap = Arc<Mutex<HashMap<String, ServerCon>>>;

pub struct P2PServer {
    server_con_map: ServerConMap,
}

impl P2PServer {
    pub fn new() -> Self {
        Self {
            server_con_map: Arc::new(Default::default()),
        }
    }

    pub async fn start(&self) -> crate::Result<()> {
        let (tx, _rx) = create_server_channel();
        let local_ip = local_ip()?.to_string();
        let server_url = format!("{}:{}", local_ip, config_ring_port_or_default());
        let listener = TcpListener::bind(server_url).await?;
        self.start_con_dispatcher(listener, tx);

        Ok(())
    }

    fn start_con_dispatcher(&self, listener: TcpListener, tx: Sender<ChannelSignal>) {
        let con_map = self.server_con_map.clone();
        spawn(async move {
            loop {
                let (socket, addr) = listener.accept().await.unwrap();
                let peer_addr = format!("{}", addr);
                println!("Server gets new connection, {:?}", peer_addr);

                let connection = ServerCon::new();
                connection
                    .start(tx.clone(), socket)
                    .await
                    .unwrap_or_default();
                con_map.lock().await.insert(peer_addr, connection);
                println!("ServerConMap size, {:?}", con_map.lock().await.keys().len());
            }
        });
    }

    fn start_channel_handler(&self, mut rx: Receiver<ChannelSignal>) {
        let con_map = self.server_con_map.clone();
        spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    ConnectionClose(peer_addr) => {
                        println!("Close socket {:?}", peer_addr);
                        con_map.lock().await.remove(&peer_addr);
                    }
                    ConnectionError(peer_addr) => {
                        con_map.lock().await.remove(&peer_addr);
                    }
                    RemoteMessage { peer_addr, message } => {
                        // TODO
                        println!("Server gets remote message {:?}, {:?}", peer_addr, message);
                    }
                }
            }
        });
    }
}

impl Default for P2PServer {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ServerCon {
    con_tx: Sender<Vec<u8>>,
    con_rx: ConnectionReceiver,
}

impl ServerCon {
    pub fn new() -> Self {
        let (con_tx, con_rx) = create_connection_channel();
        Self {
            con_tx,
            con_rx: Arc::new(Mutex::new(con_rx)),
        }
    }

    pub async fn start(
        &self,
        server_channel_tx: Sender<ChannelSignal>,
        socket: TcpStream,
    ) -> crate::Result<()> {
        let peer_addr = format!("{}", &socket.peer_addr()?);
        let (r, w) = io::split(socket);
        self.start_channel_handler(w);
        self.start_socket_reader(r, server_channel_tx, &peer_addr);
        Ok(())
    }

    fn start_channel_handler(&self, mut w: WriteHalf<TcpStream>) {
        let con_rx = self.con_rx.clone();
        spawn(async move {
            while let Some(message) = con_rx.lock().await.recv().await {
                w.write_all(message.as_slice()).await.unwrap_or_default();
            }
        });
    }

    fn start_socket_reader(
        &self,
        mut r: ReadHalf<TcpStream>,
        server_channel_tx: Sender<ChannelSignal>,
        peer_addr: &str,
    ) {
        // Serve the socket read
        let peer_addr = peer_addr.to_string();
        spawn(async move {
            let mut buf = vec![0; 1024];
            loop {
                match r.read(&mut buf).await {
                    Ok(0) => {
                        server_channel_tx
                            .send(ConnectionClose(peer_addr.clone()))
                            .await
                            .unwrap_or_default();
                    }
                    Ok(n) => {
                        handle_message(n, &buf, &server_channel_tx, peer_addr.as_str()).await;
                    }
                    Err(_) => {
                        server_channel_tx
                            .send(ConnectionError(peer_addr.clone()))
                            .await
                            .unwrap_or_default();
                        return;
                    }
                }
            }
        });
    }
}

impl Default for ServerCon {
    fn default() -> Self {
        Self::new()
    }
}

async fn handle_message(
    _n: usize,
    buf: &[u8],
    server_channel_tx: &Sender<ChannelSignal>,
    addr: &str,
) {
    let message: Message = String::from_utf8_lossy(buf).deref().into();
    server_channel_tx
        .send(RemoteMessage {
            peer_addr: addr.to_string(),
            message,
        })
        .await
        .unwrap();
}
