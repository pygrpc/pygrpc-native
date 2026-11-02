// SPDX-License-Identifier: Apache-2.0

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Accepted bearer tokens, kept only as SHA-256 digests.
pub struct BearerAuth {
    digests: Vec<[u8; 32]>,
    pub(super) message: String,
}

impl BearerAuth {
    pub fn new(digests: Vec<[u8; 32]>, message: String) -> Self {
        BearerAuth { digests, message }
    }

    pub(super) fn accepts(&self, headers: &http::HeaderMap) -> bool {
        let token = headers
            .get(http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(bearer_token);
        let Some(token) = token else {
            return false;
        };
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        // Check every digest so the time taken doesn't depend on which matched.
        let mut matched = subtle::Choice::from(0);
        for accepted in &self.digests {
            matched |= accepted.ct_eq(&digest);
        }
        matched.into()
    }
}

/// `Bearer <token>`, with the scheme in any case.
fn bearer_token(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bearer_tokens() {
        assert_eq!(bearer_token("Bearer abc"), Some("abc"));
        assert_eq!(bearer_token("bearer abc"), Some("abc"));
        assert_eq!(bearer_token("BEARER a b"), Some("a b"));
        assert_eq!(bearer_token("Bearer  abc"), Some(" abc"));
        assert_eq!(bearer_token("Bearer "), None);
        assert_eq!(bearer_token("Bearer"), None);
        assert_eq!(bearer_token("Basic abc"), None);
        assert_eq!(bearer_token("abc"), None);
    }

    #[test]
    fn accepts_only_listed_tokens() {
        let digest = |token: &str| -> [u8; 32] { Sha256::digest(token.as_bytes()).into() };
        let auth = BearerAuth::new(vec![digest("old"), digest("new")], String::new());
        let headers = |value: &str| {
            let mut headers = http::HeaderMap::new();
            headers.insert(http::header::AUTHORIZATION, value.parse().unwrap());
            headers
        };
        assert!(auth.accepts(&headers("Bearer old")));
        assert!(auth.accepts(&headers("Bearer new")));
        assert!(!auth.accepts(&headers("Bearer other")));
        assert!(!auth.accepts(&headers("old")));
        assert!(!auth.accepts(&http::HeaderMap::new()));
    }
}
