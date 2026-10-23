//! QUIC plumbing, identities and framing.
//!
//! `Cert`: real certificates (e.g. from `tailscale cert`), verified against the
//! system trust store. Nodes also present their certificate when they dial a
//! peer, so a peer is recognised by the name on its certificate.
//! `Local`: self-signed, unverified. Loopback only, enforced by the callers.

use quinn::{
    ClientConfig, Connection, Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig,
    VarInt,
};
use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DigitallySignedStruct, DistinguishedName, RootCertStore, SignatureScheme};
use std::error::Error as StdError;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub type Error = Box<dyn StdError + Send + Sync>;
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Largest frame accepted from the wire.
pub(crate) const MAX_FRAME: usize = 64 << 20;

pub(crate) fn install_crypto() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

fn transport_config() -> TransportConfig {
    let mut t = TransportConfig::default();
    t.max_concurrent_bidi_streams(VarInt::from_u32(1024));
    // Tailnet paths are tens of milliseconds, not quinn's 333 ms guess. This
    // also shortens the drain after close, so a restart finds its port free.
    t.initial_rtt(Duration::from_millis(100));
    // A quiet link stays up; a dead one is noticed within the idle timeout.
    t.keep_alive_interval(Some(Duration::from_secs(2)));
    t.max_idle_timeout(Duration::from_secs(10).try_into().ok());
    t
}

/// This node's certificate chain and key.
pub(crate) struct Identity {
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

impl Identity {
    pub fn load(cert: &Path, key: &Path) -> Result<Self> {
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut BufReader::new(File::open(cert)?))
                .collect::<std::result::Result<_, _>>()?;
        if certs.is_empty() {
            return Err(format!("no certificate in {}", cert.display()).into());
        }
        let key = rustls_pemfile::private_key(&mut BufReader::new(File::open(key)?))?
            .ok_or_else(|| format!("no private key in {}", key.display()))?;
        Ok(Self { certs, key })
    }
}

fn system_roots() -> Result<Arc<RootCertStore>> {
    let native = rustls_native_certs::load_native_certs();
    let mut roots = RootCertStore::empty();
    for cert in native.certs {
        let _ = roots.add(cert);
    }
    if roots.is_empty() {
        return Err("no system root certificates available".into());
    }
    Ok(Arc::new(roots))
}

fn server(crypto: rustls::ServerConfig, bind: SocketAddr) -> Result<Endpoint> {
    let quic = quinn::crypto::rustls::QuicServerConfig::try_from(crypto)?;
    let mut cfg = ServerConfig::with_crypto(Arc::new(quic));
    cfg.transport_config(Arc::new(transport_config()));
    Ok(Endpoint::server(cfg, bind)?)
}

/// Serves `id`, and checks any certificate a caller offers against the system
/// roots. Callers without one are still admitted, as plain clients.
pub(crate) fn server_endpoint_cert(bind: SocketAddr, id: &Identity) -> Result<Endpoint> {
    install_crypto();
    let verifier = Arc::new(PeerCerts {
        roots: system_roots()?,
        provider: provider(),
    });
    let crypto = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(id.certs.clone(), id.key.clone_key())?;
    server(crypto, bind)
}

/// Self-signed, unverified. Only for loopback; callers enforce that.
pub(crate) fn server_endpoint_local(bind: SocketAddr) -> Result<Endpoint> {
    install_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let der = cert.cert.der().clone();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    let crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![der], key.into())?;
    server(crypto, bind)
}

/// `verified`: check the server against the system roots, and present `id` if
/// given. Otherwise accept any server, which callers allow on loopback only.
pub(crate) fn client_config(verified: bool, id: Option<&Identity>) -> Result<ClientConfig> {
    install_crypto();
    let inner = if verified {
        let b = rustls::ClientConfig::builder().with_root_certificates(system_roots()?);
        match id {
            Some(id) => b.with_client_auth_cert(id.certs.clone(), id.key.clone_key())?,
            None => b.with_no_client_auth(),
        }
    } else {
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipVerify(provider())))
            .with_no_client_auth()
    };
    let mut cfg = ClientConfig::new(Arc::new(quinn::crypto::rustls::QuicClientConfig::try_from(
        inner,
    )?));
    cfg.transport_config(Arc::new(transport_config()));
    Ok(cfg)
}

/// A dial-only endpoint in the right address family.
pub(crate) fn client_endpoint(target: SocketAddr) -> Result<Endpoint> {
    let any: SocketAddr = if target.is_ipv6() {
        "[::]:0"
    } else {
        "0.0.0.0:0"
    }
    .parse()?;
    Ok(Endpoint::client(any)?)
}

/// Whether the caller presented a (root-verified) certificate for `name`.
pub(crate) fn caller_is(conn: &Connection, name: &str) -> bool {
    let Some(certs) = conn
        .peer_identity()
        .and_then(|i| i.downcast::<Vec<CertificateDer<'static>>>().ok())
    else {
        return false;
    };
    let (Some(leaf), Ok(want)) = (certs.first(), ServerName::try_from(name.to_string())) else {
        return false;
    };
    rustls::server::ParsedCertificate::try_from(leaf)
        .is_ok_and(|c| rustls::client::verify_server_name(&c, &want).is_ok())
}

pub(crate) async fn put(send: &mut SendStream, bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_FRAME {
        return Err(format!("frame of {} bytes exceeds {MAX_FRAME}", bytes.len()).into());
    }
    send.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    send.write_all(bytes).await?;
    Ok(())
}

pub(crate) async fn get(recv: &mut RecvStream) -> Result<Vec<u8>> {
    let mut l = [0u8; 4];
    recv.read_exact(&mut l).await?;
    let n = u32::from_be_bytes(l) as usize;
    if n > MAX_FRAME {
        return Err(format!("frame of {n} bytes exceeds {MAX_FRAME}").into());
    }
    let mut v = vec![0u8; n];
    recv.read_exact(&mut v).await?;
    Ok(v)
}

/// A message body is a sequence of length-prefixed blobs.
pub(crate) fn parts(pieces: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(pieces.iter().map(|p| 4 + p.len()).sum());
    for p in pieces {
        out.extend_from_slice(&(p.len() as u32).to_be_bytes());
        out.extend_from_slice(p);
    }
    out
}

pub(crate) struct Blobs<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Blobs<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, at: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.at >= self.buf.len()
    }

    pub fn take(&mut self) -> Result<&'a [u8]> {
        let head = self
            .buf
            .get(self.at..self.at + 4)
            .ok_or("truncated message")?;
        let n = u32::from_be_bytes(head.try_into()?) as usize;
        let start = self.at + 4;
        let out = self.buf.get(start..start + n).ok_or("truncated message")?;
        self.at = start + n;
        Ok(out)
    }

    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take()?.try_into().map_err(|_| "bad integer")?,
        ))
    }

    pub fn text(&mut self) -> Result<String> {
        Ok(String::from_utf8(self.take()?.to_vec())?)
    }
}

/// Verifies a caller's chain like a server certificate (tailnet certificates
/// carry the serverAuth usage, not clientAuth). Which name it carries is
/// checked per connection, against the peer list.
#[derive(Debug)]
struct PeerCerts {
    roots: Arc<RootCertStore>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ClientCertVerifier for PeerCerts {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        false
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> std::result::Result<ClientCertVerified, rustls::Error> {
        let cert = rustls::server::ParsedCertificate::try_from(end_entity)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &cert,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            m,
            c,
            d,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            m,
            c,
            d,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Accepts any certificate. Reachable only through `client_config(false, _)`,
/// which callers gate on loopback.
#[derive(Debug)]
struct SkipVerify(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for SkipVerify {
    fn verify_server_cert(
        &self,
        _e: &CertificateDer<'_>,
        _i: &[CertificateDer<'_>],
        _s: &ServerName<'_>,
        _o: &[u8],
        _n: UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blobs_roundtrip_and_reject_truncation() {
        let body = parts(&[b"name", &7u64.to_be_bytes(), b""]);
        let mut b = Blobs::new(&body);
        assert_eq!(b.text().unwrap(), "name");
        assert_eq!(b.u64().unwrap(), 7);
        assert_eq!(b.take().unwrap(), b"");
        assert!(b.is_empty());
        let mut cut = Blobs::new(&body[..body.len() - 9]);
        cut.take().unwrap();
        assert!(cut.take().is_err());
    }
}
