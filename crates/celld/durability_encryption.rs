// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Application-level encryption for cell database bytes in object storage.
//!
//! Coordination objects intentionally remain plaintext: ownership and epoch
//! CAS must be operable without decrypting customer data. LTX bodies and
//! checkpoint/fork SQLite images use this codec, authenticated to their exact
//! bucket key so copying ciphertext across cells, epochs, or checkpoints fails.

use std::collections::BTreeMap;
use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{bail, Context};
use base64::Engine;
use celld_ltx::{Error as LtxError, ReplicaObjectCodec};
use rand::RngCore;
use serde::Deserialize;

const MAGIC: &[u8; 8] = b"CRCELD01";
const NONCE_BYTES: usize = 12;
const DATA_KEY_BYTES: usize = 32;
const WRAPPED_DATA_KEY_BYTES: usize = DATA_KEY_BYTES + 16;
const FIXED_HEADER_BYTES: usize = MAGIC.len() + 2 + 8 + NONCE_BYTES + NONCE_BYTES + 2;
const MAXIMUM_KEYS: usize = 16;
const MAXIMUM_KEYRING_BYTES: usize = 16 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SerializedKeyring {
    active_key_id: String,
    keys: BTreeMap<String, String>,
}

pub struct Aes256GcmDurabilityCodec {
    active_key_id: String,
    keys: BTreeMap<String, Aes256Gcm>,
    allow_plaintext_reads: bool,
}

impl std::fmt::Debug for Aes256GcmDurabilityCodec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Aes256GcmDurabilityCodec")
            .field("active_key_id", &self.active_key_id)
            .field("key_count", &self.keys.len())
            .field("allow_plaintext_reads", &self.allow_plaintext_reads)
            .finish()
    }
}

fn valid_key_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

impl Aes256GcmDurabilityCodec {
    pub fn parse(serialized: &str, allow_plaintext_reads: bool) -> anyhow::Result<Self> {
        anyhow::ensure!(
            serialized.len() <= MAXIMUM_KEYRING_BYTES,
            "CELLD_DATA_ENCRYPTION_KEYRING exceeds {MAXIMUM_KEYRING_BYTES} bytes"
        );
        let parsed: SerializedKeyring =
            serde_json::from_str(serialized).context("decode CELLD_DATA_ENCRYPTION_KEYRING")?;
        anyhow::ensure!(
            valid_key_id(&parsed.active_key_id),
            "CELLD_DATA_ENCRYPTION_KEYRING has an invalid active key ID"
        );
        anyhow::ensure!(
            !parsed.keys.is_empty() && parsed.keys.len() <= MAXIMUM_KEYS,
            "CELLD_DATA_ENCRYPTION_KEYRING must contain 1 through {MAXIMUM_KEYS} keys"
        );
        anyhow::ensure!(
            parsed.keys.contains_key(&parsed.active_key_id),
            "CELLD_DATA_ENCRYPTION_KEYRING does not contain its active key"
        );
        let mut keys = BTreeMap::new();
        for (key_id, encoded) in parsed.keys {
            anyhow::ensure!(
                valid_key_id(&key_id),
                "CELLD_DATA_ENCRYPTION_KEYRING has an invalid key ID"
            );
            let encoded = encoded.trim();
            anyhow::ensure!(encoded.len() <= 64, "durability key {key_id} is too large");
            let key = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .with_context(|| format!("decode durability key {key_id}"))?;
            anyhow::ensure!(
                key.len() == 32,
                "durability key {key_id} must decode to exactly 32 bytes"
            );
            keys.insert(
                key_id,
                Aes256Gcm::new_from_slice(&key).expect("validated AES-256 key length"),
            );
        }
        Ok(Self {
            active_key_id: parsed.active_key_id,
            keys,
            allow_plaintext_reads,
        })
    }

    fn aad(object_key: &str, header: &[u8], domain: &[u8]) -> Vec<u8> {
        let mut aad = Vec::with_capacity(header.len() + object_key.len() + domain.len() + 2);
        aad.extend_from_slice(header);
        aad.push(0);
        aad.extend_from_slice(object_key.as_bytes());
        aad.push(0);
        aad.extend_from_slice(domain);
        aad
    }

    fn codec_error(message: impl Into<String>) -> LtxError {
        LtxError::Other(message.into().into())
    }
}

impl ReplicaObjectCodec for Aes256GcmDurabilityCodec {
    fn name(&self) -> &'static str {
        "aes-256-gcm-v1"
    }

    fn encode(&self, object_key: &str, plaintext: &[u8]) -> celld_ltx::Result<Vec<u8>> {
        let key_id = self.active_key_id.as_bytes();
        let key_id_length = u16::try_from(key_id.len())
            .map_err(|_| Self::codec_error("durability key ID is too long"))?;
        let plaintext_length = u64::try_from(plaintext.len())
            .map_err(|_| Self::codec_error("durability object is too large"))?;
        let mut wrapping_nonce = [0_u8; NONCE_BYTES];
        let mut data_nonce = [0_u8; NONCE_BYTES];
        let mut data_key = [0_u8; DATA_KEY_BYTES];
        rand::rngs::OsRng.fill_bytes(&mut wrapping_nonce);
        rand::rngs::OsRng.fill_bytes(&mut data_nonce);
        rand::rngs::OsRng.fill_bytes(&mut data_key);

        let mut prefix = Vec::with_capacity(FIXED_HEADER_BYTES + key_id.len());
        prefix.extend_from_slice(MAGIC);
        prefix.extend_from_slice(&key_id_length.to_be_bytes());
        prefix.extend_from_slice(&plaintext_length.to_be_bytes());
        prefix.extend_from_slice(&wrapping_nonce);
        prefix.extend_from_slice(&data_nonce);
        prefix.extend_from_slice(&(WRAPPED_DATA_KEY_BYTES as u16).to_be_bytes());
        prefix.extend_from_slice(key_id);
        let wrapping_aad = Self::aad(object_key, &prefix, b"data-key");
        let wrapping_key = self
            .keys
            .get(&self.active_key_id)
            .expect("active durability key was validated");
        let wrapped_data_key = wrapping_key
            .encrypt(
                Nonce::from_slice(&wrapping_nonce),
                Payload {
                    msg: &data_key,
                    aad: &wrapping_aad,
                },
            )
            .map_err(|_| Self::codec_error("durability data-key wrapping failed"))?;
        debug_assert_eq!(wrapped_data_key.len(), WRAPPED_DATA_KEY_BYTES);

        let data_cipher = Aes256Gcm::new_from_slice(&data_key)
            .expect("generated AES-256 data key has the required length");
        data_key.fill(0);
        let mut header = prefix;
        header.extend_from_slice(&wrapped_data_key);
        let data_aad = Self::aad(object_key, &header, b"database-bytes");
        let ciphertext = data_cipher
            .encrypt(
                Nonce::from_slice(&data_nonce),
                Payload {
                    msg: plaintext,
                    aad: &data_aad,
                },
            )
            .map_err(|_| Self::codec_error("durability object encryption failed"))?;
        header.extend_from_slice(&ciphertext);
        Ok(header)
    }

    fn decode(&self, object_key: &str, encoded: &[u8]) -> celld_ltx::Result<Vec<u8>> {
        if !encoded.starts_with(MAGIC) {
            return if self.allow_plaintext_reads {
                Ok(encoded.to_vec())
            } else {
                Err(Self::codec_error("durability object is not encrypted"))
            };
        }
        if encoded.len() < FIXED_HEADER_BYTES {
            return Err(Self::codec_error(
                "encrypted durability object is truncated",
            ));
        }
        let key_id_length = u16::from_be_bytes([encoded[8], encoded[9]]) as usize;
        let prefix_length = FIXED_HEADER_BYTES
            .checked_add(key_id_length)
            .ok_or_else(|| Self::codec_error("encrypted durability header overflow"))?;
        if encoded.len() < prefix_length + WRAPPED_DATA_KEY_BYTES + 16 {
            return Err(Self::codec_error(
                "encrypted durability object is truncated",
            ));
        }
        let plaintext_length = u64::from_be_bytes(
            encoded[10..18]
                .try_into()
                .expect("fixed plaintext length field"),
        );
        let wrapping_nonce = &encoded[18..30];
        let data_nonce = &encoded[30..42];
        let wrapped_data_key_length = u16::from_be_bytes([encoded[42], encoded[43]]) as usize;
        if wrapped_data_key_length != WRAPPED_DATA_KEY_BYTES {
            return Err(Self::codec_error(
                "encrypted durability wrapped data-key length is invalid",
            ));
        }
        let key_id = std::str::from_utf8(&encoded[FIXED_HEADER_BYTES..prefix_length])
            .map_err(|_| Self::codec_error("encrypted durability key ID is invalid UTF-8"))?;
        if !valid_key_id(key_id) {
            return Err(Self::codec_error("encrypted durability key ID is invalid"));
        }
        let wrapping_key = self.keys.get(key_id).ok_or_else(|| {
            Self::codec_error(format!(
                "encrypted durability object requires unknown key {key_id}"
            ))
        })?;
        let wrapped_end = prefix_length + wrapped_data_key_length;
        let wrapping_aad = Self::aad(object_key, &encoded[..prefix_length], b"data-key");
        let mut data_key = wrapping_key
            .decrypt(
                Nonce::from_slice(wrapping_nonce),
                Payload {
                    msg: &encoded[prefix_length..wrapped_end],
                    aad: &wrapping_aad,
                },
            )
            .map_err(|_| Self::codec_error("durability data-key authentication failed"))?;
        if data_key.len() != DATA_KEY_BYTES {
            data_key.fill(0);
            return Err(Self::codec_error(
                "encrypted durability data key has an invalid length",
            ));
        }
        let data_cipher = Aes256Gcm::new_from_slice(&data_key)
            .expect("unwrapped AES-256 data key has the required length");
        data_key.fill(0);
        let data_aad = Self::aad(object_key, &encoded[..wrapped_end], b"database-bytes");
        let plaintext = data_cipher
            .decrypt(
                Nonce::from_slice(data_nonce),
                Payload {
                    msg: &encoded[wrapped_end..],
                    aad: &data_aad,
                },
            )
            .map_err(|_| Self::codec_error("durability object authentication failed"))?;
        if plaintext.len() as u64 != plaintext_length {
            return Err(Self::codec_error(
                "durability object plaintext length mismatch",
            ));
        }
        Ok(plaintext)
    }
}

#[cfg(test)]
pub(crate) fn envelope_key_id_for_test(encoded: &[u8]) -> anyhow::Result<&str> {
    anyhow::ensure!(
        encoded.starts_with(MAGIC) && encoded.len() >= FIXED_HEADER_BYTES,
        "not a complete encrypted durability header"
    );
    let key_id_length = u16::from_be_bytes([encoded[8], encoded[9]]) as usize;
    let end = FIXED_HEADER_BYTES
        .checked_add(key_id_length)
        .context("encrypted durability key ID length overflow")?;
    let key_id = encoded
        .get(FIXED_HEADER_BYTES..end)
        .context("encrypted durability key ID is truncated")?;
    std::str::from_utf8(key_id).context("encrypted durability key ID is invalid UTF-8")
}

pub fn codec_from_env() -> anyhow::Result<Arc<dyn ReplicaObjectCodec>> {
    let required = crate::env_vars::flag("CELLD_DATA_ENCRYPTION_REQUIRED", false)?;
    let allow_plaintext_reads =
        crate::env_vars::flag("CELLD_DATA_ENCRYPTION_ALLOW_PLAINTEXT_READS", false)?;
    let Some(serialized) = crate::env_vars::value("CELLD_DATA_ENCRYPTION_KEYRING")? else {
        if required {
            bail!("CELLD_DATA_ENCRYPTION_REQUIRED=1 requires CELLD_DATA_ENCRYPTION_KEYRING");
        }
        anyhow::ensure!(
            !allow_plaintext_reads,
            "CELLD_DATA_ENCRYPTION_ALLOW_PLAINTEXT_READS requires an encryption keyring"
        );
        return Ok(Arc::new(celld_ltx::PlaintextReplicaObjectCodec));
    };
    anyhow::ensure!(
        !serialized.trim().is_empty(),
        "CELLD_DATA_ENCRYPTION_KEYRING cannot be empty"
    );
    Ok(Arc::new(Aes256GcmDurabilityCodec::parse(
        &serialized,
        allow_plaintext_reads,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> String {
        base64::engine::general_purpose::STANDARD.encode([byte; 32])
    }

    fn codec(
        active: &str,
        keys: &[(&str, u8)],
        allow_plaintext_reads: bool,
    ) -> Aes256GcmDurabilityCodec {
        let keys = keys
            .iter()
            .map(|(id, byte)| (id.to_string(), key(*byte)))
            .collect::<BTreeMap<_, _>>();
        Aes256GcmDurabilityCodec::parse(
            &serde_json::json!({ "active_key_id": active, "keys": keys }).to_string(),
            allow_plaintext_reads,
        )
        .expect("valid keyring")
    }

    #[test]
    fn encrypts_and_authenticates_the_exact_object_key() {
        let codec = codec("v2", &[("v1", 1), ("v2", 2)], false);
        let plaintext = b"SQLite format 3\0private";
        let encoded = codec
            .encode("cells/a/ltx/e1/0000/x.ltx", plaintext)
            .unwrap();
        assert_eq!(envelope_key_id_for_test(&encoded).unwrap(), "v2");
        assert!(envelope_key_id_for_test(b"CRCELD01").is_err());
        assert!(!encoded
            .windows(plaintext.len())
            .any(|bytes| bytes == plaintext));
        assert_eq!(
            codec.decode("cells/a/ltx/e1/0000/x.ltx", &encoded).unwrap(),
            plaintext
        );
        assert!(codec
            .decode("cells/b/ltx/e1/0000/x.ltx", &encoded)
            .unwrap_err()
            .to_string()
            .contains("authentication failed"));
        let mut tampered = encoded;
        *tampered.last_mut().unwrap() ^= 1;
        assert!(codec
            .decode("cells/a/ltx/e1/0000/x.ltx", &tampered)
            .is_err());
    }

    #[test]
    fn reads_retained_keys_during_rotation_and_rejects_unknown_keys() {
        let old = codec("v1", &[("v1", 1)], false);
        let encoded = old.encode("object", b"state").unwrap();
        let rotated = codec("v2", &[("v1", 1), ("v2", 2)], false);
        assert_eq!(rotated.decode("object", &encoded).unwrap(), b"state");
        let missing = codec("v2", &[("v2", 2)], false);
        assert!(missing
            .decode("object", &encoded)
            .unwrap_err()
            .to_string()
            .contains("unknown key v1"));
    }

    #[test]
    fn plaintext_migration_is_explicit() {
        assert!(codec("v1", &[("v1", 1)], false)
            .decode("object", b"plaintext")
            .is_err());
        assert_eq!(
            codec("v1", &[("v1", 1)], true)
                .decode("object", b"plaintext")
                .unwrap(),
            b"plaintext"
        );
    }

    #[test]
    fn parser_rejects_missing_active_short_and_unknown_fields() {
        assert!(Aes256GcmDurabilityCodec::parse(
            &serde_json::json!({ "active_key_id": "v2", "keys": { "v1": key(1) } }).to_string(),
            false,
        )
        .is_err());
        assert!(Aes256GcmDurabilityCodec::parse(
            &serde_json::json!({ "active_key_id": "v1", "keys": { "v1": "AA==" } }).to_string(),
            false,
        )
        .is_err());
        assert!(Aes256GcmDurabilityCodec::parse(
            &serde_json::json!({ "active_key_id": "v1", "keys": { "v1": key(1) }, "extra": true })
                .to_string(),
            false,
        )
        .is_err());
    }

    #[test]
    fn adversarial_envelopes_never_panic_or_return_plaintext() {
        let codec = codec("v1", &[("v1", 1)], false);
        for length in 0..512 {
            let mut encoded = vec![0_u8; length];
            rand::rngs::OsRng.fill_bytes(&mut encoded);
            if length >= MAGIC.len() {
                encoded[..MAGIC.len()].copy_from_slice(MAGIC);
            }
            assert!(codec.decode("object", &encoded).is_err());
        }

        let valid = codec.encode("object", b"private database bytes").unwrap();
        for index in 0..valid.len() {
            let mut tampered = valid.clone();
            tampered[index] ^= 1;
            assert!(codec.decode("object", &tampered).is_err());
        }
    }
}
