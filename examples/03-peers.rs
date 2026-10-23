//! Prototype 3 — membership and liveness, sourced from the tailnet.
//!
//! No gossip protocol, no health checks: `tailscale status --json` already
//! knows every peer, its tailnet IP, and whether it is up.
//! Run: cargo run --example 03-peers

use std::process::Command;

#[derive(Debug, Clone)]
struct Peer {
    host: String,
    ip: String,
    online: bool,
    /// DERP region name when the connection is relayed (i.e. NAT punched through).
    relay: String,
    direct: String,
}

fn peers() -> Result<Vec<Peer>, String> {
    let out = Command::new("tailscale")
        .args(["status", "--json"])
        .output()
        .map_err(|e| format!("cannot run tailscale: {e}"))?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).to_string());
    }
    let v: serde_json::Value =
        serde_json::from_slice(&out.stdout).map_err(|e| format!("bad json: {e}"))?;

    let mut list = Vec::new();
    if let Some(map) = v["Peer"].as_object() {
        for p in map.values() {
            list.push(Peer {
                host: p["HostName"].as_str().unwrap_or("?").to_string(),
                ip: p["TailscaleIPs"][0].as_str().unwrap_or("?").to_string(),
                online: p["Online"].as_bool().unwrap_or(false),
                relay: p["Relay"].as_str().unwrap_or("").to_string(),
                direct: p["CurAddr"].as_str().unwrap_or("").to_string(),
            });
        }
    }
    list.sort_by(|a, b| b.online.cmp(&a.online).then(a.host.cmp(&b.host)));
    Ok(list)
}

fn main() {
    let me = Command::new("tailscale")
        .args(["status", "--json"])
        .output()
        .ok()
        .and_then(|o| serde_json::from_slice::<serde_json::Value>(&o.stdout).ok())
        .and_then(|v| v["Self"]["HostName"].as_str().map(str::to_string))
        .unwrap_or_default();
    println!("self: {me}");

    match peers() {
        Ok(list) => {
            for p in &list {
                let path = if !p.direct.is_empty() {
                    format!("direct {}", p.direct)
                } else if !p.relay.is_empty() {
                    format!("relay {}", p.relay)
                } else {
                    "no path".into()
                };
                println!(
                    "  {:<24} {:<16} {:<7} {path}",
                    p.host,
                    p.ip,
                    if p.online { "up" } else { "down" }
                );
            }
            let up = list.iter().filter(|p| p.online).count();
            println!("ok: {} peers, {up} reachable right now", list.len());
            assert!(!me.is_empty(), "should know our own name");
        }
        Err(e) => println!("tailnet unavailable ({e}); node would fall back to seeds"),
    }
}
