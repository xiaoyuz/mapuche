use crate::p2p::message::Message;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[derive(Debug)]
pub enum ChannelSignal {
    ConnectionClose(String),
    ConnectionError(String),
    RemoteMessage { peer_addr: String, message: Message },
}

pub fn create_connection_channel() -> (Sender<Vec<u8>>, Receiver<Vec<u8>>) {
    channel(1024)
}

pub fn create_server_channel() -> (Sender<ChannelSignal>, Receiver<ChannelSignal>) {
    channel(1024)
}

pub fn create_client_channel() -> (Arc<Sender<Message>>, Receiver<Message>) {
    let (tx, rx) = channel(1024);
    (Arc::new(tx), rx)
}

pub fn create_signal_channel() -> (Arc<Sender<Message>>, Receiver<Message>) {
    let (tx, rx) = channel(1024);
    (Arc::new(tx), rx)
}