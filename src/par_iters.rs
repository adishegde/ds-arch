mod network;
use network::{setup_network, sync, Message, Network, ProtocolID};
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use std::time::Instant;

async fn mult_protocol(pid: u8, id: ProtocolID, net: Network<Vec<u32>>, num: usize) -> Vec<u32> {
    if pid == 0 {
        let val: Vec<u32> = (0..num)
            .into_par_iter()
            .map_init(thread_rng, |rng, _| rng.gen_range(0..1000))
            .collect();

        net.send(Message { id, val }).await;

        net.recv(id).await.val
    } else {
        let mssg = net.recv(id).await;
        let res: Vec<u32> = mssg.val.into_par_iter().map(|x| x * 10).collect();

        net.send(Message {
            id: mssg.id,
            val: res.clone(),
        })
        .await;

        res
    }
}

async fn add_protocol(pid: u8, id: ProtocolID, net: Network<Vec<u32>>, num: usize) -> Vec<u32> {
    if pid == 1 {
        let val: Vec<u32> = (0..num)
            .into_par_iter()
            .map_init(thread_rng, |rng, _| rng.gen_range(0..1000))
            .collect();

        net.send(Message { id, val }).await;

        net.recv(id).await.val
    } else {
        let mssg = net.recv(id).await;
        let res: Vec<u32> = mssg.val.into_par_iter().map(|x| x + 10).collect();

        net.send(Message {
            id: mssg.id,
            val: res.clone(),
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

    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .unwrap();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let net = setup_network(pid).await;

            let start = Instant::now();
            let handles = (
                tokio::spawn(mult_protocol(pid, 0, net.clone(), num)),
                tokio::spawn(add_protocol(pid, 1, net.clone(), num)),
            );
            let res1 = handles.0.await.unwrap();
            let res2 = handles.1.await.unwrap();
            let end = start.elapsed();
            println!("Final output len: {}", res1.len() + res2.len());
            println!("Time: {} ms", end.as_millis());

            sync(&net, Vec::new()).await;
        });
}
