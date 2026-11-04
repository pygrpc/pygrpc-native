//! TLS for a front that asks for it; plaintext connections never reach this.

// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;

/// `root_pem` is only used with `require_client_auth`: the server doesn't ask
/// for a client certificate otherwise.
pub fn acceptor(
    key_pem: &[u8],
    cert_chain_pem: &[u8],
    root_pem: Option<&[u8]>,
    require_client_auth: bool,
) -> Result<TlsAcceptor, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let chain = CertificateDer::pem_slice_iter(cert_chain_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("certificate chain: {err}"))?;
    if chain.is_empty() {
        return Err("certificate chain: no certificates".to_owned());
    }
    let key =
        PrivateKeyDer::from_pem_slice(key_pem).map_err(|err| format!("private key: {err}"))?;
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|err| err.to_string())?;
    let builder = if require_client_auth {
        let root_pem = root_pem.ok_or("require_client_auth needs root certificates")?;
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(root_pem) {
            let cert = cert.map_err(|err| format!("root certificates: {err}"))?;
            roots
                .add(cert)
                .map_err(|err| format!("root certificates: {err}"))?;
        }
        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .map_err(|err| format!("root certificates: {err}"))?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let mut config = builder
        .with_single_cert(chain, key)
        .map_err(|err| format!("private key and certificate chain: {err}"))?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}
