//! Prototype 4 — state sync over QUIC on one socket.
//!
//! Only the *server* binds and listens. The client dials out. That is the whole
//! NAT story on a tailnet: one reachable node, everyone else dials it.
//! Run: cargo run --example 04-quic

mod util;

use loro::{ExportMode, LoroDoc};
use std::error::Error;

fn frame(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in parts {
        out.extend_from_slice(&(p.len() as u32).to_be_bytes());
        out.extend_from_slice(p);
    }
    out
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    util::install_crypto();
    let (endpoint, cert) = util::server("127.0.0.1:0".parse()?)?;
    let addr = endpoint.local_addr()?;
    println!("server listening on {addr} (only this side needs an open port)");

    let server = tokio::spawn(async move {
        let conn = endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();

        let mut buf = vec![0u8; 64 * 1024];
        let n = recv.read(&mut buf).await.unwrap().unwrap();
        let doc = LoroDoc::new();
        doc.set_peer_id(2).unwrap();
        doc.import(&buf[4..4 + u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize])
            .unwrap();
        doc.get_text("notes").insert(0, "[server] ").unwrap();
        doc.commit();

        let reply = doc.export(ExportMode::Snapshot).unwrap();
        send.write_all(&frame(&[&reply])).await.unwrap();
        send.finish().unwrap();
        let _ = n;
        endpoint.wait_idle().await;
        doc.get_text("notes").to_string()
    });

    let client_ep = util::client(cert)?;
    let conn = client_ep.connect(addr, "localhost")?.await?;
    println!("client dialed out -> {}", conn.remote_address());

    let doc = LoroDoc::new();
    doc.set_peer_id(1)?;
    doc.get_text("notes").insert(0, "[client] hello")?;
    doc.commit();

    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&frame(&[&doc.export(ExportMode::Snapshot)?]))
        .await?;
    send.finish()?;

    let mut buf = vec![0u8; 64 * 1024];
    let n = recv.read(&mut buf).await?.unwrap();
    let len = u32::from_be_bytes(buf[..4].try_into()?) as usize;
    doc.import(&buf[4..4 + len])?;

    let client_text = doc.get_text("notes").to_string();
    let server_text = server.await?;
    println!("client: {client_text:?}");
    println!("server: {server_text:?}");

    assert_eq!(
        client_text, server_text,
        "both sides hold the same document"
    );
    assert!(client_text.contains("[server]") && client_text.contains("[client]"));
    println!("ok: one open port carried a full CRDT sync ({n} bytes back)");
    Ok(())
}
