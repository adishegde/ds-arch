mod network;

use network::{setup_network, sync, Message, Network, ProtocolID};

use rand::{thread_rng, Rng};
use std::time::Instant;

async fn add_protocol(pid: u8, id: ProtocolID, net: Network<u32>) -> u32 {
    if pid == 0 {
        let val: u32 = {
            let mut rng = thread_rng();
            rng.gen_range(0..1000)
        };

        net.send(Message { id, val }).await;

        net.recv(id).await.val
    } else {
        let mssg = net.recv(id).await;
        let res = mssg.val * 10;
        net.send(Message {
            id: mssg.id,
            val: res,
        })
        .await;

        res
    }
}

async fn mult_protocol(pid: u8, id: ProtocolID, net: Network<u32>) -> u32 {
    if pid == 1 {
        let val: u32 = {
            let mut rng = thread_rng();
            rng.gen_range(0..1000)
        };

        net.send(Message { id, val }).await;

        net.recv(id).await.val
    } else {
        let mssg = net.recv(id).await;
        let res = mssg.val + 10;
        net.send(Message {
            id: mssg.id,
            val: res,
        })
        .await;

        res
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        println!("Usage: {} <threads> <pid> <num>", args[0]);
        return;
    }

    let threads: usize = args[1].parse().unwrap();
    let pid: u8 = args[2].parse().unwrap();
    let num: usize = args[3].parse().unwrap();

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let net = setup_network(pid).await;

            let mut handles = Vec::with_capacity(2 * num);
            let start = Instant::now();
            for i in 0..num {
                handles.push(tokio::spawn(add_protocol(pid, (2 * i) as u32, net.clone())));

                handles.push(tokio::spawn(mult_protocol(
                    pid,
                    (2 * i + 1) as u32,
                    net.clone(),
                )));
            }

            let mut res = Vec::with_capacity(2 * num);
            for handle in handles {
                res.push(handle.await.unwrap());
            }
            let end = start.elapsed();

            println!("Final output len: {}", res.len());
            println!("Time: {} ms", end.as_millis());

            sync(&net, 0).await;
        });
}
