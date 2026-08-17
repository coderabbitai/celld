// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Optional, fail-closed deployment admission for bucket-backed fleets.

use crate::protocol::{DeployPointer, DeploymentSignature};
use anyhow::{bail, Context};
use base64::Engine as _;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

const SIGNATURE_SCHEMA_VERSION: u32 = 1;
const DOMAIN: &[u8] = b"celld-deployment-attestation-v1\0";

#[derive(Serialize)]
struct SignedPayload<'a> {
    schema_version: u32,
    script_name: &'a Option<String>,
    version: &'a str,
    prefix: &'a str,
    rollout_percent: u8,
    manifest_sha256: &'a str,
    metadata: &'a BTreeMap<String, String>,
}

fn payload(pointer: &DeployPointer, signature: &DeploymentSignature) -> anyhow::Result<Vec<u8>> {
    let mut encoded = DOMAIN.to_vec();
    encoded.extend(serde_json::to_vec(&SignedPayload {
        schema_version: signature.schema_version,
        script_name: &pointer.script_name,
        version: &pointer.version,
        prefix: &pointer.prefix,
        rollout_percent: pointer.rollout.percent,
        manifest_sha256: &signature.manifest_sha256,
        metadata: &signature.metadata,
    })?);
    Ok(encoded)
}

pub fn manifest_sha256(manifest: &[u8]) -> String {
    format!("{:x}", Sha256::digest(manifest))
}

pub fn sign(
    pointer: &DeployPointer,
    manifest: &[u8],
    key_id: String,
    metadata: BTreeMap<String, String>,
    key: &SigningKey,
) -> anyhow::Result<DeploymentSignature> {
    if key_id.trim().is_empty() {
        bail!("deployment signing key ID must not be empty");
    }
    let mut attestation = DeploymentSignature {
        schema_version: SIGNATURE_SCHEMA_VERSION,
        key_id,
        manifest_sha256: manifest_sha256(manifest),
        metadata,
        ed25519: String::new(),
    };
    let signature = key.sign(&payload(pointer, &attestation)?);
    attestation.ed25519 = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
    Ok(attestation)
}

pub fn verify(
    pointer: &DeployPointer,
    manifest: &[u8],
    keys: &BTreeMap<String, VerifyingKey>,
) -> anyhow::Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    let attestation = pointer.signature.as_ref().context(
        "deployment pointer is unsigned but CELLD_DEPLOYMENT_VERIFY_KEYS_FILE is configured",
    )?;
    if attestation.schema_version != SIGNATURE_SCHEMA_VERSION {
        bail!(
            "unsupported deployment signature schema {}",
            attestation.schema_version
        );
    }
    let expected_manifest = manifest_sha256(manifest);
    if attestation.manifest_sha256 != expected_manifest {
        bail!("deployment manifest digest does not match its signed pointer");
    }
    let key = keys.get(&attestation.key_id).with_context(|| {
        format!(
            "deployment signature uses unknown key ID {:?}",
            attestation.key_id
        )
    })?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&attestation.ed25519)
        .context("decode deployment Ed25519 signature")?;
    let signature =
        Signature::from_slice(&bytes).context("invalid deployment Ed25519 signature")?;
    key.verify(&payload(pointer, attestation)?, &signature)
        .context("deployment signature verification failed")
}

pub fn read_signing_key(path: &Path) -> anyhow::Result<SigningKey> {
    let encoded = std::fs::read_to_string(path)
        .with_context(|| format!("read deployment signing key {}", path.display()))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .context("decode deployment signing key")?;
    let seed: [u8; 32] = bytes.try_into().map_err(|_| {
        anyhow::anyhow!("deployment signing key must be a base64-encoded 32-byte seed")
    })?;
    Ok(SigningKey::from_bytes(&seed))
}

pub fn read_verifying_keys(path: &Path) -> anyhow::Result<BTreeMap<String, VerifyingKey>> {
    let encoded = std::fs::read_to_string(path)
        .with_context(|| format!("read deployment verification keys {}", path.display()))?;
    let values: BTreeMap<String, String> =
        serde_json::from_str(&encoded).context("decode deployment verification key map")?;
    if values.is_empty() {
        bail!("deployment verification key map must not be empty");
    }
    values
        .into_iter()
        .map(|(id, value)| {
            if id.trim().is_empty() {
                bail!("deployment verification key ID must not be empty");
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(value)
                .with_context(|| format!("decode deployment verification key {id:?}"))?;
            let bytes: [u8; 32] = bytes.try_into().map_err(|_| {
                anyhow::anyhow!("deployment verification key {id:?} must contain 32 bytes")
            })?;
            Ok((
                id,
                VerifyingKey::from_bytes(&bytes)
                    .context("invalid Ed25519 deployment verification key")?,
            ))
        })
        .collect()
}

pub fn configured_verifying_keys() -> anyhow::Result<BTreeMap<String, VerifyingKey>> {
    match std::env::var_os("CELLD_DEPLOYMENT_VERIFY_KEYS_FILE") {
        Some(path) if !path.is_empty() => read_verifying_keys(Path::new(&path)),
        _ => Ok(BTreeMap::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{DeployPointer, Rollout};

    fn pointer() -> DeployPointer {
        DeployPointer {
            script_name: Some("knowledge".into()),
            version: "abc123".into(),
            prefix: "deploy/knowledge/abc123".into(),
            rollout: Rollout { percent: 100 },
            signature: None,
        }
    }

    #[test]
    fn signed_pointer_binds_manifest_and_pointer() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let mut pointer = pointer();
        pointer.signature = Some(
            sign(
                &pointer,
                b"manifest",
                "release-2026".into(),
                BTreeMap::from([("source_commit".into(), "deadbeef".into())]),
                &key,
            )
            .unwrap(),
        );
        let keys = BTreeMap::from([("release-2026".into(), key.verifying_key())]);

        verify(&pointer, b"manifest", &keys).unwrap();
        assert!(verify(&pointer, b"changed", &keys).is_err());
        pointer.prefix.push_str("-tampered");
        assert!(verify(&pointer, b"manifest", &keys).is_err());
    }

    #[test]
    fn configured_verification_rejects_unsigned_and_unknown_keys() {
        let key = SigningKey::from_bytes(&[9; 32]);
        let keys = BTreeMap::from([("trusted".into(), key.verifying_key())]);
        let mut pointer = pointer();
        assert!(verify(&pointer, b"manifest", &keys).is_err());

        pointer.signature =
            Some(sign(&pointer, b"manifest", "other".into(), BTreeMap::new(), &key).unwrap());
        assert!(verify(&pointer, b"manifest", &keys).is_err());
    }

    #[test]
    fn unconfigured_generic_node_preserves_unsigned_compatibility() {
        verify(&pointer(), b"manifest", &BTreeMap::new()).unwrap();
    }
}
