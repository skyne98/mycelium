//! Prototype 5 — the two channels, measured.
//!
//! Stream = ordered, reliable, head-of-line blocked: a tiny message queued
//! behind a big one cannot overtake it. Datagram = unreliable, independent:
//! the tiny message arrives whenever it arrives.
//! Run: cargo run --example 05-channels

mod util;

use bytes::Bytes;
use std::error::Error;
use std::time::{Duration, Instant};
use tokio::time::sleep;

const BIG: usize = 512 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    util::install_crypto();
    let (endpoint, cert) = util::server("127.0.0.1:0".parse()?)?;
    let addr = endpoint.local_addr()?;

    let server = tokio::spawn(async move {
        let conn = endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut control) = conn.accept_bi().await.unwrap();

        // Stream arm: big payload then tiny payload, on one stream.
        let writer = tokio::spawn(async move {
            send.write_all(&vec![7u8; BIG]).await.unwrap();
            send.write_all(b"TINY").await.unwrap();
            send.finish().unwrap();
        });

        // Datagram arm: same big payload, sent concurrently as datagrams.
        let mut dropped = 0usize;
        let mut max_dgram = 0;
        if let Some(m) = conn.max_datagram_size() {
            max_dgram = m;
            for (i, chunk) in vec![7u8; BIG].chunks(m.min(1000)).enumerate() {
                if conn.send_datagram(Bytes::copy_from_slice(chunk)).is_err() {
                    dropped += 1;
                }
                if i.is_multiple_of(16) {
                    tokio::task::yield_now().await;
                }
            }
            let _ = conn.send_datagram(Bytes::from_static(b"TINY"));
        }

        writer.await.unwrap();
        // Stay alive until the client says it is done, so datagrams are not cut off.
        let _ = control.read_to_end(64).await;
        (dropped, max_dgram)
    });

    let client_ep = util::client(cert)?;
    let conn = client_ep.connect(addr, "localhost")?.await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    // quinn only puts the stream on the wire once it carries data.
    send.write_all(b"hi").await?;
    let t0 = Instant::now();

    // Datagram reader runs independently of the stream reader.
    let dgram = {
        let conn = conn.clone();
        tokio::spawn(async move {
            let mut n = 0usize;
            loop {
                match tokio::time::timeout(Duration::from_secs(5), conn.read_datagram()).await {
                    Ok(Ok(d)) => {
                        n += 1;
                        if &d[..] == b"TINY" {
                            return Some((t0.elapsed(), n));
                        }
                    }
                    _ => return None,
                }
            }
        })
    };

    // Slow stream reader: pause on the first 64 KiB so TINY genuinely waits.
    let mut seen = 0usize;
    let mut buf = vec![0u8; 1024];
    let mut stream_tiny = None;
    // TINY is the byte right after BIG: the reader cannot reach it before draining the bulk.
    while let Ok(Some(n)) = recv.read(&mut buf).await {
        seen += n;
        if seen >= BIG {
            stream_tiny = Some(t0.elapsed());
            break;
        }
        if seen < 64 * 1024 {
            sleep(Duration::from_micros(300)).await;
        }
    }

    let (dgram_tiny, dgrams) = dgram.await?.expect("tiny datagram arrived");
    let _ = send.finish();
    let stream_tiny = stream_tiny.expect("tiny stream bytes arrived");
    let (dropped, max_dgram) = server.await?;

    println!("max datagram payload: {max_dgram} bytes");
    println!(
        "stream  : {BIG} B then TINY -> tiny at {stream_tiny:?}, after {seen} B queued ahead of it"
    );
    println!("datagram: {BIG} B then TINY -> tiny at {dgram_tiny:?} ({dgrams} received, {dropped} chunks shed)");

    assert!(
        seen >= BIG,
        "on a stream TINY is stuck behind every preceding byte"
    );
    assert!(
        dgram_tiny < stream_tiny,
        "datagrams do not queue behind the bulk payload"
    );
    assert!(dropped > 0 || dgrams > 0, "datagram arm ran");

    println!("ok: stream = ordered + blocking, datagram = independent + lossy");
    Ok(())
}
