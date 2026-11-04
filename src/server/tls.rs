//! TLS for a front that asks for it; plaintext connections never reach this.

// SPDX-License-Identifier: Apache-2.0

use std::net::IpAddr;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;
use x509_parser::der_parser::asn1_rs::Tag;
use x509_parser::extensions::GeneralName;
use x509_parser::objects::{oid2abbrev, oid_registry};
use x509_parser::prelude::{FromDer, X509Certificate};
use x509_parser::x509::{AttributeTypeAndValue, X509Name};

const COMMON_NAME: &str = "x509_common_name";
const ALTERNATIVE_NAME: &str = "x509_subject_alternative_name";

/// A client's certificate as grpcio's `auth_context()` shows it.
pub struct PeerAuth {
    pub properties: Vec<(&'static str, Vec<u8>)>,
    pub identity_key: &'static str,
}

impl PeerAuth {
    /// `chain` starts with the client's own certificate.
    pub fn new(chain: &[CertificateDer<'_>]) -> Result<Self, String> {
        let (leaf, rest) = chain.split_first().ok_or("no certificates")?;
        let (_, cert) = X509Certificate::from_der(leaf).map_err(|err| err.to_string())?;
        let subject = cert.subject();
        let common_name = subject
            .iter_common_name()
            .next()
            .and_then(text)
            .unwrap_or_default();
        let mut properties = vec![
            ("transport_security_type", b"ssl".to_vec()),
            ("security_level", b"TSI_PRIVACY_AND_INTEGRITY".to_vec()),
            ("x509_subject", rfc2253(subject).into_bytes()),
            (COMMON_NAME, common_name.as_bytes().to_vec()),
            ("x509_pem_cert", pem(leaf).into_bytes()),
        ];
        if !rest.is_empty() {
            let chain: String = rest.iter().map(|cert| pem(cert)).collect();
            properties.push(("x509_pem_cert_chain", chain.into_bytes()));
        }
        let names = cert
            .subject_alternative_name()
            .map_err(|err| err.to_string())?;
        let names = names.map_or(&[][..], |names| &names.value.general_names[..]);
        let mut identity_key = COMMON_NAME;
        let mut uris = Vec::new();
        for name in names {
            let (kind, value) = match name {
                GeneralName::DNSName(name) => ("peer_dns", name.as_bytes().to_vec()),
                GeneralName::RFC822Name(name) => ("peer_email", name.as_bytes().to_vec()),
                GeneralName::URI(name) => {
                    uris.push(*name);
                    ("peer_uri", name.as_bytes().to_vec())
                }
                GeneralName::IPAddress(address) => match ip(address) {
                    Some(address) => ("peer_ip", address.into_bytes()),
                    None => continue,
                },
                _ => continue,
            };
            properties.push((ALTERNATIVE_NAME, value.clone()));
            properties.push((kind, value));
            identity_key = ALTERNATIVE_NAME;
        }
        // As in grpc-core, a SPIFFE certificate has exactly one URI.
        if let [uri] = uris[..] {
            if is_spiffe_id(uri) {
                properties.push(("peer_spiffe_id", uri.as_bytes().to_vec()));
            }
        }
        Ok(Self {
            properties,
            identity_key,
        })
    }
}

fn ip(address: &[u8]) -> Option<String> {
    let address = match address.len() {
        4 => IpAddr::from(<[u8; 4]>::try_from(address).ok()?),
        16 => IpAddr::from(<[u8; 16]>::try_from(address).ok()?),
        _ => return None,
    };
    Some(address.to_string())
}

fn is_spiffe_id(uri: &str) -> bool {
    let Some(id) = uri.strip_prefix("spiffe://") else {
        return false;
    };
    let Some((domain, path)) = id.split_once('/') else {
        return false;
    };
    uri.len() <= 2048 && domain.len() <= 255 && !path.is_empty()
}

fn pem(cert: &[u8]) -> String {
    let encoded = STANDARD.encode(cert);
    let mut text = String::from("-----BEGIN CERTIFICATE-----\n");
    for line in encoded.as_bytes().chunks(64) {
        text.push_str(&String::from_utf8_lossy(line));
        text.push('\n');
    }
    text.push_str("-----END CERTIFICATE-----\n");
    text
}

/// The subject as OpenSSL's RFC 2253 output has it: last attribute first.
fn rfc2253(name: &X509Name<'_>) -> String {
    let registry = oid_registry();
    let rdns: Vec<_> = name.iter().collect();
    let attributes = rdns.iter().rev().map(|rdn| {
        let attributes: Vec<_> = rdn.iter().collect();
        let parts = attributes.iter().rev().map(|attribute| {
            let oid = attribute.attr_type();
            let key = match oid2abbrev(oid, registry) {
                // OpenSSL's names, where the registry has others.
                Ok("Email") => "emailAddress".to_owned(),
                Ok("givenName") => "GN".to_owned(),
                Ok("streetAddress") => "street".to_owned(),
                Ok("surname") => "SN".to_owned(),
                Ok("uid") => "UID".to_owned(),
                Ok(abbreviation) => abbreviation.to_owned(),
                Err(_) => oid.to_id_string(),
            };
            let value = text(attribute).as_deref().map(escape).unwrap_or_default();
            format!("{key}={value}")
        });
        parts.collect::<Vec<_>>().join("+")
    });
    attributes.collect::<Vec<_>>().join(",")
}

/// x509-parser's `as_str()` leaves out BMPString, which older CAs still write.
fn text(attribute: &AttributeTypeAndValue<'_>) -> Option<String> {
    if let Ok(text) = attribute.as_str() {
        return Some(text.to_owned());
    }
    let value = attribute.attr_value();
    if value.tag() != Tag::BmpString {
        return None;
    }
    let (units, _) = value.data.as_chunks::<2>();
    let units = units.iter().map(|unit| u16::from_be_bytes(*unit));
    char::decode_utf16(units).collect::<Result<_, _>>().ok()
}

fn escape(value: &str) -> String {
    let last = value.chars().count().saturating_sub(1);
    let mut escaped = String::with_capacity(value.len());
    for (index, c) in value.chars().enumerate() {
        let edge = (index == 0 && matches!(c, ' ' | '#')) || (index == last && c == ' ');
        if edge || matches!(c, ',' | '+' | '"' | '\\' | '<' | '>' | ';') {
            escaped.push('\\');
        }
        if c.is_ascii() && !c.is_ascii_control() {
            escaped.push(c);
        } else {
            for byte in c.encode_utf8(&mut [0; 4]).bytes() {
                escaped.push_str(&format!("\\{byte:02X}"));
            }
        }
    }
    escaped
}

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
