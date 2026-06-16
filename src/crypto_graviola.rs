use std::sync::Arc;

use graviola::{
    aead::AesGcm,
    hashing::{Sha256, hmac::Hmac},
};
use noq_proto::crypto::{CryptoError, HandshakeTokenKey, HmacKey};
use rand::Rng;

// ---- HmacKey impl using graviola::Hmac<Sha256> ----

/// Wraps graviola's `Hmac<Sha256>` to implement `noq_proto::crypto::HmacKey`.
struct GraviolaHmacKey(Hmac<Sha256>);

impl HmacKey for GraviolaHmacKey {
    fn sign(&self, data: &[u8], signature_out: &mut [u8]) {
        let mut hmac = self.0.clone();
        hmac.update(data);
        let tag = hmac.finish();
        signature_out.copy_from_slice(tag.as_ref());
    }

    fn signature_len(&self) -> usize {
        32
    }

    fn verify(&self, data: &[u8], signature: &[u8]) -> Result<(), CryptoError> {
        let mut hmac = self.0.clone();
        hmac.update(data);
        hmac.verify(signature).map_err(|_| CryptoError)
    }
}

// ---- HandshakeTokenKey impl using graviola::AesGcm + HMAC-based HKDF ----

/// Implements retry token encryption using HMAC-SHA256-based HKDF followed by AES-256-GCM.
///
/// Replaces the ring-based `RetryTokenKey` from noq-proto when ring is unavailable.
struct GraviolaRetryTokenKey([u8; 32]);

impl GraviolaRetryTokenKey {
    fn new(rng: &mut impl Rng) -> Self {
        let mut master_key = [0u8; 32];
        rng.fill_bytes(&mut master_key);
        Self(master_key)
    }

    /// HMAC-based HKDF-expand to derive AES-256-GCM key from token_nonce.
    fn derive_aead(&self, token_nonce: u128) -> AesGcm {
        // HMAC-based HKDF:
        // prk = HMAC-SHA256(salt=[], ikm=self.0)
        // okm = HMAC-SHA256(prk, token_nonce || 0x01)
        let prk = {
            let mut h = Hmac::<Sha256>::new([]);
            h.update(self.0);
            h.finish()
        };
        let info = token_nonce.to_le_bytes();
        let mut h = Hmac::<Sha256>::new(prk.as_ref());
        h.update(info);
        h.update([0x01]);
        let okm = h.finish();

        AesGcm::new(&okm.as_ref()[..32])
    }
}

impl HandshakeTokenKey for GraviolaRetryTokenKey {
    fn seal(&self, token_nonce: u128, data: &mut Vec<u8>) -> Result<(), CryptoError> {
        let aead = self.derive_aead(token_nonce);
        let nonce = [0u8; 12];
        let data_len = data.len();
        data.resize(data_len + 16, 0);
        let mut tag = [0u8; 16];
        aead.encrypt(&nonce, &[], &mut data[..data_len], &mut tag);
        data[data_len..].copy_from_slice(&tag);
        Ok(())
    }

    fn open<'a>(&self, token_nonce: u128, data: &'a mut [u8]) -> Result<&'a [u8], CryptoError> {
        if data.len() < 16 {
            return Err(CryptoError);
        }
        let tag = data[data.len() - 16..].to_vec();
        let ciphertext_len = data.len() - 16;
        let aead = self.derive_aead(token_nonce);
        let nonce = [0u8; 12];
        aead.decrypt(&nonce, &[], &mut data[..ciphertext_len], &tag)
            .map_err(|_| CryptoError)?;
        Ok(&data[..ciphertext_len])
    }
}

// ---- Convenience functions (not extension traits — we call these directly) ----

use noq_proto::{EndpointConfig, ServerConfig};

/// Create a default `EndpointConfig` with a randomized reset key using graviola HMAC-SHA256.
pub(crate) fn graviola_endpoint_config() -> EndpointConfig {
    let mut reset_key = [0u8; 64];
    rand::rng().fill_bytes(&mut reset_key);
    let hmac_key = Hmac::<Sha256>::new(reset_key);
    EndpointConfig::new(Arc::new(GraviolaHmacKey(hmac_key)))
}

/// Create a `ServerConfig` with the given crypto config and a randomized
/// handshake token key using graviola.
pub(crate) fn graviola_server_with_crypto(
    crypto: Arc<dyn noq_proto::crypto::ServerConfig>,
) -> ServerConfig {
    let retry_token_key = GraviolaRetryTokenKey::new(&mut rand::rng());
    ServerConfig::new(crypto, Arc::new(retry_token_key))
}
