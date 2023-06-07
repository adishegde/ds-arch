use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub type ProtocolID = u32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<T> {
    pub id: ProtocolID,
    pub val: T,
}

#[derive(Clone)]
pub struct Network<T> {
    tx_mssg: mpsc::UnboundedSender<Message<T>>,
    tx_reg: mpsc::UnboundedSender<(ProtocolID, oneshot::Sender<Message<T>>)>,
}

impl<T: Debug> Network<T> {
    pub async fn send(&self, mssg: Message<T>) {
        self.tx_mssg.send(mssg).unwrap();
    }

    pub async fn recv(&self, id: ProtocolID) -> Message<T> {
        let (tx, rx) = oneshot::channel();
        self.tx_reg.send((id, tx)).unwrap();
        rx.await.unwrap()
    }
}

pub async fn setup_network<T: Debug + Serialize + DeserializeOwned + Send + 'static>(
    pid: u8,
) -> Network<T> {
    let (rx_tcp, tx_tcp) = {
        if pid == 0 {
            let listener = TcpListener::bind("0.0.0.0:8000").await.unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            stream.set_nodelay(true).unwrap();
            stream.into_split()
        } else {
            let stream = loop {
                if let Ok(stream) = TcpStream::connect("127.0.0.1:8000").await {
                    break stream;
                }

                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            };

            stream.set_nodelay(true).unwrap();
            stream.into_split()
        }
    };

    let (tx_mssg, rx_mssg) = mpsc::unbounded_channel();
    tokio::spawn(send_router(rx_mssg, tx_tcp));

    let (tx_reg, rx_reg) = mpsc::unbounded_channel();
    tokio::spawn(recv_router(rx_tcp, rx_reg));

    Network { tx_mssg, tx_reg }
}

async fn send_to_socket<T: Serialize>(tx_tcp: &mut OwnedWriteHalf, mssg: Message<T>) {
    let mut bytes = VecDeque::new();
    bincode::serialize_into(&mut bytes, &mssg).unwrap();

    let len_bytes: u32 = bytes.len().try_into().unwrap();
    let mut len_bytes = len_bytes.to_be_bytes();
    len_bytes.reverse();
    for v in len_bytes {
        bytes.push_front(v);
    }

    bytes.make_contiguous();
    tx_tcp.write_all(bytes.as_slices().0).await.unwrap();
    tx_tcp.flush().await.unwrap();
}

async fn recv_from_socket<T: DeserializeOwned>(
    rx_tcp: &mut OwnedReadHalf,
) -> std::io::Result<Message<T>> {
    let mut len_bytes = [0u8; 4];
    rx_tcp.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes);

    let mut bytes = vec![0u8; len as usize];
    rx_tcp.read_exact(&mut bytes).await?;

    Ok(bincode::deserialize(&bytes).unwrap())
}

async fn send_router<T: Serialize>(
    mut rx_mssg: mpsc::UnboundedReceiver<Message<T>>,
    mut tx_tcp: OwnedWriteHalf,
) {
    while let Some(mssg) = rx_mssg.recv().await {
        send_to_socket(&mut tx_tcp, mssg).await;
    }
}

async fn recv_router<T: Debug + DeserializeOwned + Send + 'static>(
    mut rx_tcp: OwnedReadHalf,
    mut rx_reg: mpsc::UnboundedReceiver<(ProtocolID, oneshot::Sender<Message<T>>)>,
) {
    let mut mssg_pool: HashMap<ProtocolID, Message<T>> = HashMap::new();
    let mut handle_pool: HashMap<ProtocolID, oneshot::Sender<Message<T>>> = HashMap::new();
    let (tx, mut rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        while let Ok(mssg) = recv_from_socket(&mut rx_tcp).await {
            tx.send(mssg).unwrap();
        }
    });

    loop {
        tokio::select! {
            Some(mssg) = rx.recv() => {
                match handle_pool.remove(&mssg.id) {
                    Some(handle) => {
                        handle.send(mssg).unwrap();
                    },
                    None => {
                        mssg_pool.insert(mssg.id, mssg);
                    }
                }
            },
            Some((id, handle)) = rx_reg.recv() => {
                match mssg_pool.remove(&id) {
                    Some(mssg) => {
                        handle.send(mssg).unwrap();
                    },
                    None => {
                        handle_pool.insert(id, handle);
                    }
                }
            },
            else => break
        }
    }

    while !mssg_pool.is_empty() {
        match rx_reg.recv().await {
            Some((id, handle)) => match mssg_pool.remove(&id) {
                Some(mssg) => {
                    handle.send(mssg).unwrap();
                }
                None => {
                    panic!("Illegal protocol ID");
                }
            },
            None => break,
        }
    }
}

pub async fn sync<T: Debug>(net: &Network<T>, val: T) {
    net.send(Message { id: u32::MAX, val }).await;
    net.recv(u32::MAX).await;
}
