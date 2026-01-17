//! TLS configuration helpers for QUIC transport.
//!
//! This module provides utilities for building TLS configurations:
//! - Loading certificates and keys from files
//! - Self-signed certificate generation for development
//! - Server and client configuration builders

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};

use super::config::TlsConfig;
use super::error::QuicError;

/// Default ALPN protocol for Plumtree.
#[allow(dead_code)]
pub const DEFAULT_ALPN: &[u8] = b"plumtree/1";

/// Build a QUIC server configuration from TLS settings.
///
/// If custom certificates are provided, they will be loaded from disk.
/// Otherwise, a self-signed certificate will be generated.
///
/// **Note**: If generating self-signed certificates, this performs CPU-intensive
/// cryptographic operations. For async contexts, prefer [`server_config_async`].
///
/// This synchronous version is provided for non-async initialization scenarios.
#[allow(dead_code)]
pub fn server_config(tls: &TlsConfig) -> Result<quinn::ServerConfig, QuicError> {
    let (cert_chain, private_key) = if tls.has_custom_certs() {
        load_certs_and_key(tls)?
    } else {
        generate_self_signed()?
    };

    build_server_config(tls, cert_chain, private_key)
}

/// Build a QUIC server configuration from TLS settings (async version).
///
/// This function offloads self-signed certificate generation to a blocking
/// thread pool, preventing it from blocking async worker threads.
pub async fn server_config_async(tls: &TlsConfig) -> Result<quinn::ServerConfig, QuicError> {
    let (cert_chain, private_key) = if tls.has_custom_certs() {
        load_certs_and_key(tls)?
    } else {
        generate_self_signed_async().await?
    };

    build_server_config(tls, cert_chain, private_key)
}

/// Internal helper to build server config from certs.
fn build_server_config(
    tls: &TlsConfig,
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
) -> Result<quinn::ServerConfig, QuicError> {
    let mut crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|e| QuicError::TlsConfig(e.to_string()))?;

    // Set ALPN protocols
    crypto.alpn_protocols = tls.alpn_protocols_or_default();

    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
            .map_err(|e| QuicError::TlsConfig(e.to_string()))?,
    ));

    // Configure transport parameters
    let transport = Arc::get_mut(&mut config.transport).unwrap();
    transport.max_idle_timeout(Some(
        quinn::IdleTimeout::try_from(std::time::Duration::from_secs(30))
            .map_err(|e| QuicError::TlsConfig(e.to_string()))?,
    ));

    Ok(config)
}

/// Build a QUIC client configuration from TLS settings.
pub fn client_config(tls: &TlsConfig) -> Result<quinn::ClientConfig, QuicError> {
    let crypto = if tls.skip_verification {
        insecure_client_crypto(tls)?
    } else {
        secure_client_crypto(tls)?
    };

    let config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(|e| QuicError::TlsConfig(e.to_string()))?,
    ));

    Ok(config)
}

/// Build a secure client crypto configuration.
fn secure_client_crypto(tls: &TlsConfig) -> Result<ClientConfig, QuicError> {
    let mut root_store = RootCertStore::empty();

    // Add system roots
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    // Add custom CA if provided
    if let Some(ca_path) = &tls.ca_path {
        let ca_certs = load_certificates(ca_path)?;
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| QuicError::Certificate(e.to_string()))?;
        }
    }

    let mut config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    // Set ALPN protocols
    config.alpn_protocols = tls.alpn_protocols_or_default();

    Ok(config)
}

/// Build an insecure client crypto configuration.
///
/// **WARNING**: This skips server certificate verification and is
/// vulnerable to man-in-the-middle attacks. Only use for development.
fn insecure_client_crypto(tls: &TlsConfig) -> Result<ClientConfig, QuicError> {
    let mut config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(InsecureServerVerifier))
        .with_no_client_auth();

    // Set ALPN protocols
    config.alpn_protocols = tls.alpn_protocols_or_default();

    Ok(config)
}

/// Load certificates and private key from TLS configuration.
fn load_certs_and_key(
    tls: &TlsConfig,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), QuicError> {
    let cert_path = tls
        .cert_path
        .as_ref()
        .ok_or_else(|| QuicError::TlsConfig("certificate path not set".to_string()))?;

    let key_path = tls
        .key_path
        .as_ref()
        .ok_or_else(|| QuicError::TlsConfig("key path not set".to_string()))?;

    let certs = load_certificates(cert_path)?;
    let key = load_private_key(key_path)?;

    Ok((certs, key))
}

/// Load certificates from a PEM file.
fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, QuicError> {
    let file = File::open(path).map_err(|e| {
        QuicError::Certificate(format!("failed to open cert file {:?}: {}", path, e))
    })?;
    let mut reader = BufReader::new(file);

    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .map_err(|e| QuicError::Certificate(format!("failed to parse certs: {}", e)))?;

    if certs.is_empty() {
        return Err(QuicError::Certificate(
            "no certificates found in file".to_string(),
        ));
    }

    Ok(certs)
}

/// Load a private key from a PEM file.
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, QuicError> {
    let file = File::open(path).map_err(|e| {
        QuicError::Certificate(format!("failed to open key file {:?}: {}", path, e))
    })?;
    let mut reader = BufReader::new(file);

    loop {
        match rustls_pemfile::read_one(&mut reader)
            .map_err(|e| QuicError::Certificate(format!("failed to parse key: {}", e)))?
        {
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => {
                return Ok(PrivateKeyDer::Pkcs1(key));
            }
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => {
                return Ok(PrivateKeyDer::Pkcs8(key));
            }
            Some(rustls_pemfile::Item::Sec1Key(key)) => {
                return Ok(PrivateKeyDer::Sec1(key));
            }
            None => break,
            _ => continue,
        }
    }

    Err(QuicError::Certificate(
        "no private key found in file".to_string(),
    ))
}

/// Generate a self-signed certificate for development.
///
/// **Note**: This function performs CPU-intensive cryptographic operations.
/// For use in async contexts, prefer [`generate_self_signed_async`] which
/// offloads the work to a blocking thread pool.
pub fn generate_self_signed(
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), QuicError> {
    let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .map_err(|e| QuicError::Certificate(format!("failed to generate cert: {}", e)))?;

    let cert_der = CertificateDer::from(certified_key.cert);
    let key_der = PrivateKeyDer::Pkcs8(certified_key.signing_key.serialize_der().into());

    Ok((vec![cert_der], key_der))
}

/// Generate a self-signed certificate for development (async version).
///
/// This function offloads the CPU-intensive certificate generation to a
/// blocking thread pool, preventing it from blocking async worker threads.
///
/// # Example
///
/// ```ignore
/// let (certs, key) = generate_self_signed_async().await?;
/// ```
pub async fn generate_self_signed_async(
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), QuicError> {
    // Offload CPU-intensive crypto work to the blocking thread pool
    tokio::task::spawn_blocking(generate_self_signed)
        .await
        .map_err(|e| QuicError::Internal(format!("spawn_blocking failed: {}", e)))?
}

/// Server verifier that accepts any certificate.
///
/// **WARNING**: This is insecure and should only be used for development.
#[derive(Debug)]
struct InsecureServerVerifier;

impl rustls::client::danger::ServerCertVerifier for InsecureServerVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_self_signed() {
        let result = generate_self_signed();
        assert!(result.is_ok());

        let (certs, _key) = result.unwrap();
        assert_eq!(certs.len(), 1);
    }

    #[test]
    fn test_server_config_self_signed() {
        let tls = TlsConfig::default();
        let result = server_config(&tls);
        assert!(result.is_ok());
    }

    #[test]
    fn test_client_config_insecure() {
        let tls = TlsConfig {
            skip_verification: true,
            ..Default::default()
        };
        let result = client_config(&tls);
        assert!(result.is_ok());
    }

    #[test]
    fn test_alpn_default() {
        assert_eq!(DEFAULT_ALPN, b"plumtree/1");
    }

    #[test]
    fn test_alpn_protocols_or_default() {
        // Empty protocols should return default
        let tls = TlsConfig::default();
        let alpn = tls.alpn_protocols_or_default();
        assert_eq!(alpn, vec![b"plumtree/1".to_vec()]);

        // Custom protocols should be used
        let tls = TlsConfig {
            alpn_protocols: vec![b"custom/1".to_vec()],
            ..Default::default()
        };
        let alpn = tls.alpn_protocols_or_default();
        assert_eq!(alpn, vec![b"custom/1".to_vec()]);
    }
}
