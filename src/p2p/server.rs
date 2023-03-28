use crate::config::config_ring_port_or_default;
use crate::p2p::message::Message;
use local_ip_address::linux::local_ip;
use std::collections::HashMap;

use crate::p2p::server::ServerConSignal::{ConnectionClose, ConnectionError};
use crate::utils::now_timestamp_in_millis;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};
use tokio::{io, spawn};

type ConnectionReceiver = Arc<Mutex<Receiver<Message>>>;
type ServerConMap = Arc<Mutex<HashMap<String, ServerCon>>>;

#[derive(Debug)]
pub enum ServerConSignal {
    ConnectionClose(String),
    ConnectionError(String),
}

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
        let (tx, rx) = mpsc::channel(1024);
        let local_ip = local_ip()?.to_string();
        let server_url = format!("{}:{}", local_ip, config_ring_port_or_default());
        let listener = TcpListener::bind(server_url).await?;
        self.start_con_dispatcher(listener, tx);
        self.start_channel_handler(rx);
        Ok(())
    }

    fn start_con_dispatcher(&self, listener: TcpListener, tx: Sender<ServerConSignal>) {
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

    fn start_channel_handler(&self, mut rx: Receiver<ServerConSignal>) {
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
    con_tx: Sender<Message>,
    con_rx: ConnectionReceiver,
}

impl ServerCon {
    pub fn new() -> Self {
        let (con_tx, con_rx) = mpsc::channel(1024);
        Self {
            con_tx,
            con_rx: Arc::new(Mutex::new(con_rx)),
        }
    }

    pub async fn start(
        &self,
        server_channel_tx: Sender<ServerConSignal>,
        socket: TcpStream,
    ) -> crate::Result<()> {
        let peer_addr = format!("{}", &socket.peer_addr()?);
        let (r, w) = io::split(socket);
        self.start_socket_writer(w);
        self.start_socket_reader(r, server_channel_tx, &peer_addr);
        Ok(())
    }

    fn start_socket_writer(&self, mut w: WriteHalf<TcpStream>) {
        let con_rx = self.con_rx.clone();
        spawn(async move {
            while let Some(message) = con_rx.lock().await.recv().await {
                let message_bytes: Vec<u8> = message.into();
                w.write_all(&message_bytes).await.unwrap_or_default();
            }
        });
    }

    fn start_socket_reader(
        &self,
        mut r: ReadHalf<TcpStream>,
        server_channel_tx: Sender<ServerConSignal>,
        peer_addr: &str,
    ) {
        // Serve the socket read
        let peer_addr = peer_addr.to_string();
        let sender = self.con_tx.clone();
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
                        let message: Message = buf[..n].into();
                        let sender = sender.clone();
                        if let Message::CmdReqMessage {
                            address,
                            cmd,
                            ts: _,
                            req_id,
                        } = message
                        {
                            spawn(async move {
                                let res = cmd.execute_for_remote().await;
                                if let Ok(frame) = res {
                                    let resp_message = Message::CmdRespMessage {
                                        address,
                                        frame,
                                        ts: now_timestamp_in_millis(),
                                        req_id,
                                    };
                                    sender.send(resp_message).await.unwrap_or_default();
                                }
                            });
                        }
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
