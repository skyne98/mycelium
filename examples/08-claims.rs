//! Prototype 8 — the three claims, measured over one real link.
//!
//! 1. conflict resolution latency  (loro merge, N real conflicts)
//! 2. message latency              (QUIC stream RTT and QUIC datagram RTT)
//! 3. bandwidth                    (upload and download, MB/s)
//!
//! Two processes, one address:
//!   cargo run --example 08-claims -- listen 0.0.0.0:7777
//!   cargo run --example 08-claims -- connect 100.x.y.z:7777
//!
//! Loopback numbers prove the harness works, not the network.

mod util;

use bytes::Bytes;
use loro::{ExportMode, LoroDoc};
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

const PINGS: usize = 100;
const CONFLICTS: usize = 500;
const BULK: usize = 8 << 20;

const PING: u8 = 1;
const MERGE: u8 = 2;
const DOWN: u8 = 3;
const UP: u8 = 4;

fn digest(doc: &LoroDoc) -> String {
    let s = format!("{:?}", doc.get_deep_value());
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}/{}", h.finish(), s.len())
}

fn conflict_writes(doc: &LoroDoc, tag: &str) {
    let map = doc.get_map("shared");
    for i in 0..CONFLICTS {
        map.insert(&format!("k{i}"), format!("{tag}-{i}")).unwrap();
    }
    doc.commit();
}

/// Base snapshot plus a client side that is already in conflict with the server.
fn staged() -> (LoroDoc, Vec<u8>, Vec<u8>) {
    let base = LoroDoc::new();
    base.set_peer_id(1).unwrap();
    base.get_text("log").insert(0, "base").unwrap();
    base.commit();
    let s0 = base.export(ExportMode::Snapshot).unwrap();

    conflict_writes(&base, "client");
    let s1 = base.export(ExportMode::Snapshot).unwrap();
    (base, s0, s1)
}

fn pct(mut v: Vec<Duration>, p: f64) -> Duration {
    v.sort();
    v[((v.len() as f64 - 1.0) * p) as usize]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

async fn frame(
    kind: u8,
    parts: &[&[u8]],
    send: &mut quinn::SendStream,
) -> Result<(), Box<dyn Error>> {
    send.write_all(&[kind]).await?;
    for p in parts {
        send.write_all(&(p.len() as u32).to_be_bytes()).await?;
        send.write_all(p).await?;
    }
    Ok(())
}

async fn read_blob(recv: &mut quinn::RecvStream) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut b = [0u8; 4];
    recv.read_exact(&mut b).await?;
    let n = u32::from_be_bytes(b) as usize;
    let mut v = vec![0u8; n];
    recv.read_exact(&mut v).await?;
    Ok(v)
}

async fn serve_once(addr: SocketAddr) -> Result<(), Box<dyn Error>> {
    util::install_crypto();
    let (endpoint, _cert) = util::server(addr)?;
    println!("[server] listening on {}", endpoint.local_addr()?);

    let conn = endpoint.accept().await.unwrap().await.unwrap();
    println!("[server] {} connected", conn.remote_address());

    // Echo every datagram: that is the whole datagram-side protocol.
    {
        let c = conn.clone();
        tokio::spawn(async move {
            while let Ok(d) = c.read_datagram().await {
                let _ = c.send_datagram(d);
            }
        });
    }

    loop {
        let Ok((mut send, mut recv)) = conn.accept_bi().await else {
            break;
        };
        let mut kind = [0u8; 1];
        if recv.read_exact(&mut kind).await.is_err() {
            break;
        }
        match kind[0] {
            PING => {
                while let Ok(blob) = read_blob(&mut recv).await {
                    send.write_all(&(blob.len() as u32).to_be_bytes()).await?;
                    send.write_all(&blob).await?;
                }
            }
            MERGE => {
                let s0 = read_blob(&mut recv).await?;
                let s1 = read_blob(&mut recv).await?;
                let doc = LoroDoc::new();
                doc.set_peer_id(2)?;
                doc.import(&s0)?;
                conflict_writes(&doc, "server");
                let t = Instant::now();
                doc.import(&s1)?;
                doc.commit();
                let dt = t.elapsed();
                println!("[server] merged {CONFLICTS} conflicts in {:.3} ms", ms(dt));

                let out = doc.export(ExportMode::Snapshot)?;
                send.write_all(&(out.len() as u32).to_be_bytes()).await?;
                send.write_all(&out).await?;
                let d = digest(&doc);
                send.write_all(&(d.len() as u32).to_be_bytes()).await?;
                send.write_all(d.as_bytes()).await?;
            }
            DOWN => {
                let n = u32::from_be_bytes(
                    read_blob(&mut recv)
                        .await?
                        .try_into()
                        .map_err(|_| "bad len")?,
                ) as usize;
                let buf = vec![9u8; 64 * 1024];
                let mut left = n;
                while left > 0 {
                    let take = left.min(buf.len());
                    send.write_all(&buf[..take]).await?;
                    left -= take;
                }
            }
            UP => {
                let n = u32::from_be_bytes(
                    read_blob(&mut recv)
                        .await?
                        .try_into()
                        .map_err(|_| "bad len")?,
                ) as usize;
                let mut buf = vec![0u8; 64 * 1024];
                let mut left = n;
                while left > 0 {
                    let take = left.min(buf.len());
                    recv.read_exact(&mut buf[..take]).await?;
                    left -= take;
                }
                send.write_all(&[1u8]).await?;
            }
            _ => break,
        }
        let _ = send.finish();
    }
    println!("[server] done");
    Ok(())
}

async fn client(addr: SocketAddr) -> Result<(), Box<dyn Error>> {
    util::install_crypto();
    println!(
        "[client] target {addr} (loopback: {})",
        addr.ip().is_loopback()
    );

    let endpoint = util::insecure_client()?;
    let conn = endpoint.connect(addr, "mycelium")?.await?;
    println!("[client] connected, handshake rtt {:?}", conn.rtt());

    // ---- latency: stream ping-pong ---------------------------------------
    let mut rtts = Vec::with_capacity(PINGS);
    {
        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(&[PING]).await?;
        for i in 0..PINGS {
            let blob = vec![(i % 251) as u8; 64];
            let t = Instant::now();
            send.write_all(&(blob.len() as u32).to_be_bytes()).await?;
            send.write_all(&blob).await?;
            let _ = read_blob(&mut recv).await?;
            rtts.push(t.elapsed());
        }
        let _ = send.finish();
    }
    let (stream_p50, stream_p95) = (pct(rtts.clone(), 0.50), pct(rtts, 0.95));

    // ---- latency: datagram ping-pong -------------------------------------
    let mut drtts = Vec::with_capacity(PINGS);
    for i in 0..PINGS as u32 {
        let mut payload = vec![0u8; 64];
        payload[..4].copy_from_slice(&i.to_be_bytes());
        let t = Instant::now();
        conn.send_datagram(Bytes::from(payload))?;
        loop {
            let d = tokio::time::timeout(Duration::from_secs(5), conn.read_datagram())
                .await
                .map_err(|_| "datagram echo timed out")??;
            if d.len() >= 4 && u32::from_be_bytes(d[..4].try_into()?) == i {
                drtts.push(t.elapsed());
                break;
            }
        }
    }
    let (dgram_p50, dgram_p95) = (pct(drtts.clone(), 0.50), pct(drtts, 0.95));

    // ---- bandwidth: download ---------------------------------------------
    let down = {
        let (mut send, mut recv) = conn.open_bi().await?;
        frame(DOWN, &[&(BULK as u32).to_be_bytes()], &mut send).await?;
        let mut buf = vec![0u8; 64 * 1024];
        let mut got = 0usize;
        let t = Instant::now();
        while got < BULK {
            let n = recv.read(&mut buf).await?.ok_or("download truncated")?;
            got += n;
        }
        let dt = t.elapsed();
        let _ = send.finish();
        (BULK as f64 / dt.as_secs_f64() / 1e6, dt)
    };

    // ---- bandwidth: upload -----------------------------------------------
    let up = {
        let (mut send, mut recv) = conn.open_bi().await?;
        let payload = vec![9u8; 64 * 1024];
        let t = Instant::now();
        frame(UP, &[&(BULK as u32).to_be_bytes()], &mut send).await?;
        let mut left = BULK;
        while left > 0 {
            let take = left.min(payload.len());
            send.write_all(&payload[..take]).await?;
            left -= take;
        }
        let mut ack = [0u8; 1];
        recv.read_exact(&mut ack).await?;
        let dt = t.elapsed();
        let _ = send.finish();
        (BULK as f64 / dt.as_secs_f64() / 1e6, dt)
    };

    // ---- conflict resolution ---------------------------------------------
    let (doc, s0, s1) = staged();
    let merge = {
        let (mut send, mut recv) = conn.open_bi().await?;
        frame(MERGE, &[&s0, &s1], &mut send).await?;
        let snapshot = read_blob(&mut recv).await?;
        let server_digest = String::from_utf8(read_blob(&mut recv).await?)?;

        let t = Instant::now();
        doc.import(&snapshot)?;
        doc.commit();
        let dt = t.elapsed();
        let _ = send.finish();
        (dt, digest(&doc), server_digest)
    };

    println!();
    println!("  path            {addr}  (loopback = harness check only)");
    println!(
        "  stream   rtt    p50 {:>8.2} ms   p95 {:>8.2} ms",
        ms(stream_p50),
        ms(stream_p95)
    );
    println!(
        "  datagram rtt    p50 {:>8.2} ms   p95 {:>8.2} ms",
        ms(dgram_p50),
        ms(dgram_p95)
    );
    println!(
        "  download        {:>8.1} MB/s  ({:.2} ms for {} MiB)",
        down.0,
        ms(down.1),
        BULK >> 20
    );
    println!(
        "  upload          {:>8.1} MB/s  ({:.2} ms for {} MiB)",
        up.0,
        ms(up.1),
        BULK >> 20
    );
    println!(
        "  merge           {:>8.3} ms  ({CONFLICTS} conflicting writes, {:.1} us each)",
        ms(merge.0),
        merge.0.as_secs_f64() * 1e6 / CONFLICTS as f64
    );
    println!(
        "  converged       {}",
        if merge.1 == merge.2 { "yes" } else { "NO" }
    );
    println!();

    assert!(
        merge.1 == merge.2,
        "replicas diverged: {} vs {}",
        merge.1,
        merge.2
    );
    assert!(down.0 > 0.0 && up.0 > 0.0);
    println!("ok: latency, bandwidth and merge all measured on one link");
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str).unwrap_or("") {
        "listen" => {
            let addr: SocketAddr = args
                .get(2)
                .map(String::as_str)
                .unwrap_or("0.0.0.0:7777")
                .parse()?;
            tokio::runtime::Runtime::new()?.block_on(serve_once(addr))
        }
        "connect" => {
            let addr: SocketAddr = args
                .get(2)
                .map(String::as_str)
                .unwrap_or("127.0.0.1:7777")
                .parse()?;
            tokio::runtime::Runtime::new()?.block_on(client(addr))
        }
        _ => {
            eprintln!("usage: 08-claims listen [addr] | connect <addr>");
            Ok(())
        }
    }
}
