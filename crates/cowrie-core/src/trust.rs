//! Plane D: Out-of-band Authority lanes and Causal-DAG Sticky Taint Membrane (`BIT_TAINTED = 0x80`).

use std::collections::HashMap;
use crate::CowrieError;

pub const AUTH_SYS_POLICY: u8    = 0x01;
pub const AUTH_VERIFIED_TOOL: u8 = 0x02;
pub const AUTH_UNTRUSTED_EXT: u8 = 0x04;
pub const AUTH_MODEL_SYNTH: u8   = 0x08;
pub const BIT_TAINTED: u8        = 0x80;

pub const EPI_OBSERVED: u8  = 0x01;
pub const EPI_DERIVED: u8   = 0x02;
pub const EPI_ESTIMATED: u8 = 0x03;

/// Returns true if an authority bitmask carries untrusted origin or transitive causal taint.
#[inline]
pub fn is_tainted(authority: u8) -> bool {
    (authority & (AUTH_UNTRUSTED_EXT | BIT_TAINTED)) != 0
}

/// Human-readable label for authority + taint flags.
pub fn authority_label(authority: u8) -> String {
    let base = match authority & 0x0F {
        AUTH_SYS_POLICY => "SYS_POLICY",
        AUTH_VERIFIED_TOOL => "VERIFIED_TOOL",
        AUTH_UNTRUSTED_EXT => "UNTRUSTED_EXT",
        AUTH_MODEL_SYNTH => "MODEL_SYNTH",
        _ => "MIXED",
    };
    if (authority & BIT_TAINTED) != 0 {
        format!("{}|TAINTED", base)
    } else {
        base.to_string()
    }
}

/// Compute the wire authority byte for a frame derived from `parents`.
/// If ANY parent is `UNTRUSTED_EXT (0x04)` or already carries `BIT_TAINTED (0x80)`,
/// the derived frame irrevocably inherits `BIT_TAINTED (0x80)`.
#[inline]
pub fn propagate_authority(base_authority: u8, parent_authorities: &[u8]) -> u8 {
    if is_tainted(base_authority) || parent_authorities.iter().any(|&a| is_tainted(a)) {
        base_authority | BIT_TAINTED
    } else {
        base_authority
    }
}

/// Privileged tool actions guarded by the Trust Membrane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolAction {
    ReadOnlySafe,
    ShellExec,
    CredRead,
    FsWrite,
    NetExfil,
}

impl ToolAction {
    pub fn is_privileged(self) -> bool {
        !matches!(self, ToolAction::ReadOnlySafe)
    }
}

/// Causal lineage DAG tracking frames strictly by full 34-byte CID and enforcing transitive taint checks.
/// Sticky rule: re-registering an existing CID can NEVER clear `BIT_TAINTED` once set.
#[derive(Default, Debug)]
pub struct TrustLedger {
    nodes: HashMap<[u8; 34], (u8, Vec<[u8; 34]>)>,
}

impl TrustLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a frame with its base authority and causal parent CIDs.
    /// Returns the effective authority byte (with `BIT_TAINTED` set if any ancestor was tainted).
    pub fn record(&mut self, cid: [u8; 34], base_authority: u8, parents: &[[u8; 34]]) -> u8 {
        let mut eff = base_authority;
        if is_tainted(eff) {
            eff |= BIT_TAINTED;
        }
        if let Some(&(existing_auth, _)) = self.nodes.get(&cid) {
            if is_tainted(existing_auth) {
                eff |= BIT_TAINTED;
            }
        }
        for p in parents {
            if let Some(&(p_auth, _)) = self.nodes.get(p) {
                if is_tainted(p_auth) {
                    eff |= BIT_TAINTED;
                }
            } else {
                // Unknown parent fails closed as tainted.
                eff |= BIT_TAINTED;
            }
        }
        self.nodes.insert(cid, (eff, parents.to_vec()));
        eff
    }

    pub fn authority_of(&self, cid: &[u8; 34]) -> Option<u8> {
        self.nodes.get(cid).map(|(a, _)| *a)
    }

    /// Policy gate enforced before invoking a tool with arguments/context derived from `arg_cids`.
    /// Empty `arg_cids` on a privileged call without `owner_override` fails closed (`TaintedExecutionBlocked`).
    pub fn check_tool_call(
        &self,
        action: ToolAction,
        arg_cids: &[[u8; 34]],
        owner_override: bool,
    ) -> Result<(), CowrieError> {
        if !action.is_privileged() || owner_override {
            return Ok(());
        }
        if arg_cids.is_empty() {
            return Err(CowrieError::TaintedExecutionBlocked);
        }
        for cid in arg_cids {
            match self.nodes.get(cid) {
                Some(&(auth, _)) if !is_tainted(auth) => {}
                _ => return Err(CowrieError::TaintedExecutionBlocked),
            }
        }
        Ok(())
    }

    /// Policy gate supporting scoped cryptographic `Ed25519` owner-declassification tokens (`COWRIE_DECLASSIFY_V1`).
    /// Each tainted argument CID must be individually covered by a valid, non-expired `Ed25519` signature
    /// for `action` under `owner_pubkey`; any uncovered tainted or unknown CID still fails closed!
    pub fn check_tool_call_signed(
        &self,
        action: ToolAction,
        arg_cids: &[[u8; 34]],
        tokens: &[crate::ed25519::DeclassificationToken],
        owner_pubkey: Option<&[u8; 32]>,
        now_unix_secs: u64,
    ) -> Result<(), CowrieError> {
        if !action.is_privileged() {
            return Ok(());
        }
        if arg_cids.is_empty() {
            return Err(CowrieError::TaintedExecutionBlocked);
        }
        for cid in arg_cids {
            let clean = matches!(self.nodes.get(cid), Some(&(auth, _)) if !is_tainted(auth));
            if clean {
                continue;
            }
            let Some(pk) = owner_pubkey else {
                return Err(CowrieError::TaintedExecutionBlocked);
            };
            let approved = tokens
                .iter()
                .any(|t| &t.target_cid == cid && t.verify(pk, action, now_unix_secs).is_ok());
            if !approved {
                return Err(CowrieError::TaintedExecutionBlocked);
            }
        }
        Ok(())
    }
}
