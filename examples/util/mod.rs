//! Shared QUIC plumbing for the transport prototypes.
#![allow(dead_code)]

use quinn::{ClientConfig, Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::{error::Error, net::SocketAddr, sync::Arc};

pub const DGRAM_BUFFER: usize = 4 << 20;

/// rustls needs exactly one process-wide crypto provider picked; quinn pulls in aws-lc-rs.
pub fn install_crypto() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

pub fn server(bind: SocketAddr) -> Result<(Endpoint, CertificateDer<'static>), Box<dyn Error>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let der = cert.cert.der().clone();
    let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    let mut cfg = ServerConfig::with_single_cert(vec![der.clone()], key.into())?;
    let t = Arc::get_mut(&mut cfg.transport).unwrap();
    t.datagram_send_buffer_size(DGRAM_BUFFER);
    t.datagram_receive_buffer_size(Some(DGRAM_BUFFER));

    Ok((Endpoint::server(cfg, bind)?, der))
}

/// Accept any server certificate. Prototype only: peers are already
/// authenticated by the tailnet, so the handshake adds nothing here.
pub fn insecure_client() -> Result<Endpoint, Box<dyn Error>> {
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let inner = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipVerify(Arc::new(provider))))
        .with_no_client_auth();

    let mut t = quinn::TransportConfig::default();
    t.datagram_send_buffer_size(DGRAM_BUFFER);
    t.datagram_receive_buffer_size(Some(DGRAM_BUFFER));

    let mut cfg = ClientConfig::new(Arc::new(quinn::crypto::rustls::QuicClientConfig::try_from(
        inner,
    )?));
    cfg.transport_config(Arc::new(t));

    let mut ep = Endpoint::client("0.0.0.0:0".parse()?)?;
    ep.set_default_client_config(cfg);
    Ok(ep)
}

#[derive(Debug)]
struct SkipVerify(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for SkipVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

pub fn client(cert: CertificateDer<'static>) -> Result<Endpoint, Box<dyn Error>> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert)?;
    let inner = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let mut t = quinn::TransportConfig::default();
    t.datagram_send_buffer_size(DGRAM_BUFFER);
    t.datagram_receive_buffer_size(Some(DGRAM_BUFFER));

    let mut cfg = ClientConfig::new(Arc::new(quinn::crypto::rustls::QuicClientConfig::try_from(
        inner,
    )?));
    cfg.transport_config(Arc::new(t));

    let mut ep = Endpoint::client("127.0.0.1:0".parse()?)?;
    ep.set_default_client_config(cfg);
    Ok(ep)
}
