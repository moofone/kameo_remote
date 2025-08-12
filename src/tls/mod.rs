pub mod name;
pub mod resolver;
pub mod verifier;

use crate::{NodeId, Result, SecretKey, GossipError};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{ClientConfig, DigitallySignedStruct, DistinguishedName, Error, ServerConfig, SignatureScheme};
use std::sync::Arc;
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// Ensure the rustls CryptoProvider is installed (required for TLS)
/// This uses the ring provider which is enabled in kameo_remote's Cargo.toml
pub fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// TLS configuration for the gossip protocol
pub struct TlsConfig {
    /// Our secret key for this node
    pub secret_key: SecretKey,
    
    /// Our node ID (public key)
    pub node_id: NodeId,
    
    /// TLS client configuration
    pub client_config: Arc<ClientConfig>,
    
    /// TLS server configuration  
    pub server_config: Arc<ServerConfig>,
}

impl TlsConfig {
    /// Create a new TLS configuration with the given secret key
    pub fn new(secret_key: SecretKey) -> Result<Self> {
        let node_id = secret_key.public();
        
        // Create client config
        let client_config = make_client_config(&secret_key)?;
        
        // Create server config
        let server_config = make_server_config(&secret_key)?;
        
        Ok(Self {
            secret_key,
            node_id,
            client_config: Arc::new(client_config),
            server_config: Arc::new(server_config),
        })
    }
    
    /// Get a TLS connector for outgoing connections
    pub fn connector(&self) -> TlsConnector {
        TlsConnector::from(self.client_config.clone())
    }
    
    /// Get a TLS acceptor for incoming connections
    pub fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(self.server_config.clone())
    }
}

/// Create client configuration for TLS 1.3 with custom verification
fn make_client_config(secret_key: &SecretKey) -> Result<ClientConfig> {
    let mut config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NodeIdServerVerifier::new()))
        .with_client_cert_resolver(Arc::new(resolver::AlwaysResolvesCert::new(secret_key)?));
    
    // Set ALPN protocol
    config.alpn_protocols = vec![b"kameo-gossip/1".to_vec()];
    
    // Enable key logging for debugging if SSLKEYLOGFILE is set
    if std::env::var("SSLKEYLOGFILE").is_ok() {
        config.key_log = Arc::new(rustls::KeyLogFile::new());
    }
    
    Ok(config)
}

/// Create server configuration for TLS 1.3
fn make_server_config(secret_key: &SecretKey) -> Result<ServerConfig> {
    let mut config = ServerConfig::builder()
        .with_client_cert_verifier(Arc::new(NodeIdClientVerifier::new()))
        .with_cert_resolver(Arc::new(resolver::AlwaysResolvesCert::new(secret_key)?));
    
    // Set ALPN protocols
    config.alpn_protocols = vec![b"kameo-gossip/1".to_vec()];
    
    // Enable key logging for debugging if SSLKEYLOGFILE is set
    if std::env::var("SSLKEYLOGFILE").is_ok() {
        config.key_log = Arc::new(rustls::KeyLogFile::new());
    }
    
    Ok(config)
}

/// Custom server certificate verifier that validates NodeId
#[derive(Debug)]
struct NodeIdServerVerifier;

impl NodeIdServerVerifier {
    fn new() -> Self {
        Self
    }
}

impl ServerCertVerifier for NodeIdServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, Error> {
        // Extract NodeId from DNS name
        let expected_node_id = match server_name {
            ServerName::DnsName(dns_name) => {
                let name_str = dns_name.as_ref();
                name::decode(name_str)
                    .ok_or_else(|| Error::General("Invalid DNS name format for NodeId".into()))?
            }
            _ => return Err(Error::General("Expected DNS name for NodeId".into())),
        };
        
        // Extract public key from certificate and verify it matches
        let actual_node_id = extract_node_id_from_cert(end_entity)?;
        
        if actual_node_id != expected_node_id {
            return Err(Error::General(format!(
                "NodeId mismatch: expected {}, got {}",
                expected_node_id.fmt_short(),
                actual_node_id.fmt_short()
            ).into()));
        }
        
        Ok(ServerCertVerified::assertion())
    }
    
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, Error> {
        Err(Error::General("TLS 1.2 not supported".into()))
    }
    
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, Error> {
        // Verify the signature using the Ed25519 public key
        let node_id = extract_node_id_from_cert(cert)?;
        
        // Convert rustls signature to ed25519-dalek signature
        let signature = ed25519_dalek::Signature::from_slice(dss.signature())
            .map_err(|e| Error::General(format!("Invalid signature: {}", e).into()))?;
        
        // Verify using the public key
        node_id.verify(message, &signature)
            .map_err(|e| Error::General(format!("Signature verification failed: {}", e).into()))?;
        
        Ok(HandshakeSignatureValid::assertion())
    }
    
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}

/// Custom client certificate verifier that accepts any valid NodeId
#[derive(Debug)]
struct NodeIdClientVerifier;

impl NodeIdClientVerifier {
    fn new() -> Self {
        Self
    }
}

impl ClientCertVerifier for NodeIdClientVerifier {
    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> std::result::Result<ClientCertVerified, Error> {
        // Just validate it's a valid NodeId, we'll store it for later use
        let _node_id = extract_node_id_from_cert(end_entity)?;
        Ok(ClientCertVerified::assertion())
    }
    
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, Error> {
        Err(Error::General("TLS 1.2 not supported".into()))
    }
    
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, Error> {
        // Verify the signature using the Ed25519 public key
        let node_id = extract_node_id_from_cert(cert)?;
        
        // Convert rustls signature to ed25519-dalek signature
        let signature = ed25519_dalek::Signature::from_slice(dss.signature())
            .map_err(|e| Error::General(format!("Invalid signature: {}", e).into()))?;
        
        // Verify using the public key
        node_id.verify(message, &signature)
            .map_err(|e| Error::General(format!("Signature verification failed: {}", e).into()))?;
        
        Ok(HandshakeSignatureValid::assertion())
    }
    
    fn client_auth_mandatory(&self) -> bool {
        false // Start with optional client auth for migration
    }
    
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[] // No root hints needed for self-signed certs
    }
    
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}

/// Extract NodeId from a certificate by parsing the Ed25519 public key
/// This is a custom parser for our minimal self-signed certificates
fn extract_node_id_from_cert(cert: &CertificateDer<'_>) -> std::result::Result<NodeId, Error> {
    let cert_bytes = cert.as_ref();
    
    // The certificate structure we generate has the Ed25519 public key
    // in the SubjectPublicKeyInfo field. We need to find it.
    // 
    // Our certificates have this structure:
    // SEQUENCE (Certificate)
    //   SEQUENCE (TBSCertificate)
    //     ... various fields ...
    //     SEQUENCE (SubjectPublicKeyInfo)
    //       SEQUENCE (AlgorithmIdentifier)
    //         OID (Ed25519 = 2B 65 70)
    //       BIT STRING (public key)
    
    // Look for the SubjectPublicKeyInfo structure with Ed25519 OID
    // Structure: SEQUENCE { AlgorithmIdentifier { OID }, BIT STRING }
    // The Ed25519 public key appears in SubjectPublicKeyInfo, not in the signature
    let ed25519_oid_pattern = &[0x06, 0x03, 0x2B, 0x65, 0x70]; // OID for Ed25519
    
    // Find ALL occurrences of the Ed25519 OID in the certificate
    // We need the one in SubjectPublicKeyInfo, not the signature algorithm
    let mut oid_positions = Vec::new();
    for i in 0..cert_bytes.len().saturating_sub(ed25519_oid_pattern.len()) {
        if &cert_bytes[i..i + ed25519_oid_pattern.len()] == ed25519_oid_pattern {
            oid_positions.push(i);
        }
    }
    
    if oid_positions.is_empty() {
        return Err(Error::General("Certificate does not contain Ed25519 OID".into()));
    }
    
    // The SubjectPublicKeyInfo contains: SEQUENCE { AlgorithmIdentifier, BIT STRING }
    // We need to find the OID that's followed by a 33-byte BIT STRING (the public key)
    // The signature will have a 65-byte BIT STRING, so we can distinguish them
    
    let mut public_key_bytes = None;
    for oid_index in oid_positions {
        // After the OID, look for a BIT STRING
        let search_start = oid_index + ed25519_oid_pattern.len();
        
        // Find the next BIT STRING tag (0x03)
        for i in search_start..cert_bytes.len().saturating_sub(2).min(search_start + 10) {
            if cert_bytes[i] == 0x03 {
                // Check the length
                let length = cert_bytes[i + 1] as usize;
                if length == 33 {
                    // This is likely the public key (1 unused bit + 32 key bytes)
                    let key_start = i + 3; // Skip tag, length, and unused bits byte
                    let key_end = key_start + 32;
                    
                    if key_end <= cert_bytes.len() {
                        public_key_bytes = Some(&cert_bytes[key_start..key_end]);
                        break;
                    }
                }
            }
        }
        
        if public_key_bytes.is_some() {
            break;
        }
    }
    
    let key_bytes = public_key_bytes
        .ok_or_else(|| Error::General("Could not find Ed25519 public key in certificate".into()))?;
    
    // Create NodeId from the public key bytes
    NodeId::from_bytes(key_bytes)
        .map_err(|e| Error::General(format!("Invalid public key in certificate: {}", e).into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_tls_config_creation() {
        let secret_key = SecretKey::generate();
        let config = TlsConfig::new(secret_key).unwrap();
        
        // Verify we can create connector and acceptor
        let _connector = config.connector();
        let _acceptor = config.acceptor();
    }
}