//! Crash-Safe Hash-Chained Append-Only Framed WAL (`.cwwal`) + Live `CWIV` Vector Delta Ingest & Compaction.
//!
//! Turns Cowrie from a static snapshot container into a live mutating store:
//! - Every appended frame or INT8 vector entry is chained via `SHA-256` (`prev_hash -> record_hash`).
//! - Torn tail writes from power loss / `SIGKILL` recover cleanly on `open(..., recover_torn_tail = true)`,
//!   while any mid-log tampering fails closed (`ERR_CORRUPT_TRAILER` / `ERR_ROOT_MISMATCH`).
//! - `LiveIvfStore` pairs an immutable 16 KB `ColocatedIvfReader` (`CWIV`) with a WAL-backed SIMD delta
//!   tail and one-call `compact_to_cwiv()` merge.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::canonical::verify_frame_zero_alloc;
use crate::ivf::{
    dot_i8, write_colocated_ivf_shard_streaming, ColocatedIvfReader, IvfSearchHit, VecMeta,
};
use crate::sha256::{content_address, sha256, Sha256};
use crate::shard::build_shard_16kb;
use crate::trust::is_tainted;
use crate::CowrieError;

pub const WAL_MAGIC: &[u8; 4] = b"CWAL";
pub const WAL_REC_MAGIC: &[u8; 4] = b"CWLR";
const WAL_HDR_BYTES: usize = 32;
const GEN_MANIFEST_MAGIC: &[u8; 4] = b"CWG1";
const GEN_MANIFEST_BYTES: usize = 88;

pub const WAL_KIND_FRAME: u8 = 0;
pub const WAL_KIND_TOMBSTONE: u8 = 1;
pub const WAL_KIND_IVF_VEC: u8 = 2;

static WAL_TMP_NONCE: AtomicU64 = AtomicU64::new(1);

mod wal_sys {
    extern "C" {
        pub fn flock(fd: i32, operation: i32) -> i32;
    }
    pub const LOCK_EX: i32 = 2;
    #[cfg(all(target_os = "linux", any(target_arch = "aarch64", target_arch = "arm")))]
    pub const O_NOFOLLOW: i32 = 0o100000;
    #[cfg(all(target_os = "linux", not(any(target_arch = "aarch64", target_arch = "arm"))))]
    pub const O_NOFOLLOW: i32 = 0o400000;
    #[cfg(target_os = "macos")]
    pub const O_NOFOLLOW: i32 = 0x0100;
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub const O_NOFOLLOW: i32 = 0;
}

fn unique_wal_tmp_path(path: &Path, suffix: &str) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let fname = path.file_name().and_then(|s| s.to_str()).unwrap_or("wal");
    let seq = WAL_TMP_NONCE.fetch_add(1, Ordering::Relaxed);
    parent.join(format!(".{}.{}.{}.{}.tmp", fname, suffix, std::process::id(), seq))
}

fn fsync_parent_dir(path: &Path) {
    if let Some(parent) = path.parent() {
        let p = if parent.as_os_str().is_empty() { Path::new(".") } else { parent };
        if let Ok(df) = File::open(p) {
            let _ = df.sync_all();
        }
    }
}

#[derive(Clone, Debug)]
pub struct WalEntry {
    pub seq: u64,
    pub kind: u8,
    pub payload: Vec<u8>,
    pub record_hash: [u8; 32],
    pub cid: Option<[u8; 34]>,
}

fn wal_genesis_v2(base_seq: u64) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"COWRIE_WAL_GENESIS_V2\0");
    h.update(&base_seq.to_le_bytes());
    h.finalize()
}

fn manifest_path_for(base_path: &Path) -> PathBuf {
    let mut s = base_path.as_os_str().to_os_string();
    s.push(".cwgen");
    PathBuf::from(s)
}

fn gen_shard_path_for(base_path: &Path, gen_id: u64) -> PathBuf {
    let mut s = base_path.as_os_str().to_os_string();
    s.push(format!(".gen.{}.cwiv", gen_id));
    PathBuf::from(s)
}

fn sha256_file_strict(path: &Path) -> Result<[u8; 32], CowrieError> {
    let mut f = OpenOptions::new()
        .read(true)
        .custom_flags(wal_sys::O_NOFOLLOW)
        .open(path)
        .map_err(|_| CowrieError::IoError)?;
    let meta = f.metadata().map_err(|_| CowrieError::IoError)?;
    if !meta.file_type().is_file() {
        return Err(CowrieError::InvalidContract);
    }
    let mut h = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = f.read(&mut buf).map_err(|_| CowrieError::IoError)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(h.finalize())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LiveIvfGenManifest {
    gen_id: u64,
    applied_wal_seq_exclusive: u64,
    base_gen_sha256: [u8; 32],
}

impl LiveIvfGenManifest {
    fn encode(&self) -> [u8; GEN_MANIFEST_BYTES] {
        let mut out = [0u8; GEN_MANIFEST_BYTES];
        out[0..4].copy_from_slice(GEN_MANIFEST_MAGIC);
        out[4..8].copy_from_slice(&1u32.to_le_bytes());
        out[8..16].copy_from_slice(&self.gen_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.applied_wal_seq_exclusive.to_le_bytes());
        out[24..56].copy_from_slice(&self.base_gen_sha256);
        let digest = sha256(&out[0..56]);
        out[56..88].copy_from_slice(&digest);
        out
    }

    fn decode(raw: &[u8]) -> Result<Self, CowrieError> {
        if raw.len() != GEN_MANIFEST_BYTES {
            return Err(CowrieError::Truncated);
        }
        if &raw[0..4] != GEN_MANIFEST_MAGIC {
            return Err(CowrieError::BadMagic);
        }
        let ver = u32::from_le_bytes(raw[4..8].try_into().unwrap());
        if ver != 1 {
            return Err(CowrieError::UnsupportedVer);
        }
        let expected = sha256(&raw[0..56]);
        if raw[56..88] != expected {
            return Err(CowrieError::RootMismatch);
        }
        let gen_id = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        let applied_wal_seq_exclusive = u64::from_le_bytes(raw[16..24].try_into().unwrap());
        let mut base_gen_sha256 = [0u8; 32];
        base_gen_sha256.copy_from_slice(&raw[24..56]);
        Ok(Self {
            gen_id,
            applied_wal_seq_exclusive,
            base_gen_sha256,
        })
    }

    fn read_if_exists(path: &Path) -> Result<Option<Self>, CowrieError> {
        let mut f = match OpenOptions::new()
            .read(true)
            .custom_flags(wal_sys::O_NOFOLLOW)
            .open(path)
        {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(CowrieError::IoError),
        };
        let mut buf = Vec::with_capacity(GEN_MANIFEST_BYTES);
        f.read_to_end(&mut buf).map_err(|_| CowrieError::IoError)?;
        Ok(Some(Self::decode(&buf)?))
    }

    fn write_atomic(&self, path: &Path) -> Result<(), CowrieError> {
        let bytes = self.encode();
        let tmp = unique_wal_tmp_path(path, "cwgen");
        let write_res = (|| -> Result<(), CowrieError> {
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&tmp)
                .map_err(|_| CowrieError::IoError)?;
            f.write_all(&bytes).map_err(|_| CowrieError::IoError)?;
            f.sync_all().map_err(|_| CowrieError::IoError)?;
            Ok(())
        })();
        if let Err(e) = write_res {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        if fs::rename(&tmp, path).is_err() {
            let _ = fs::remove_file(&tmp);
            return Err(CowrieError::IoError);
        }
        fsync_parent_dir(path);
        Ok(())
    }
}

/// Tamper-evident hash-chained append-only Write-Ahead Log (`.cwwal`) protected by `flock(LOCK_EX)`.
pub struct WalStore {
    path: PathBuf,
    file: File,
    next_seq: u64,
    head_hash: [u8; 32],
    entries: Vec<WalEntry>,
}

impl WalStore {
    /// Open or create a `0600` `.cwwal` file under exclusive `flock(LOCK_EX)`, verifying the entire SHA-256 hash chain.
    /// If `recover_torn_tail` is true, an incomplete or zero-filled trailing record at EOF is truncated cleanly.
    pub fn open(path: &Path, recover_torn_tail: bool) -> Result<Self, CowrieError> {
        Self::open_with_base_seq(path, 0, recover_torn_tail)
    }

    /// Open or create a `.cwwal` file whose empty genesis starts at `initial_base_seq` (monotonic across compactions).
    pub fn open_with_base_seq(
        path: &Path,
        initial_base_seq: u64,
        recover_torn_tail: bool,
    ) -> Result<Self, CowrieError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(wal_sys::O_NOFOLLOW)
            .open(path)
            .map_err(|_| CowrieError::IoError)?;

        let meta = file.metadata().map_err(|_| CowrieError::IoError)?;
        if !meta.file_type().is_file() {
            return Err(CowrieError::InvalidContract);
        }
        if unsafe { wal_sys::flock(file.as_raw_fd(), wal_sys::LOCK_EX) } != 0 {
            return Err(CowrieError::IoError);
        }

        let flen = file.metadata().map_err(|_| CowrieError::IoError)?.len();
        let genesis_v1 = sha256(b"COWRIE_WAL_GENESIS_V1");

        if flen == 0 || (flen < WAL_HDR_BYTES as u64 && recover_torn_tail) {
            file.set_len(0).map_err(|_| CowrieError::IoError)?;
            file.seek(SeekFrom::Start(0)).map_err(|_| CowrieError::IoError)?;
            let mut hdr = [0u8; WAL_HDR_BYTES];
            hdr[0..4].copy_from_slice(WAL_MAGIC);
            let head_hash = if initial_base_seq == 0 {
                hdr[4..8].copy_from_slice(&1u32.to_le_bytes());
                hdr[8..32].copy_from_slice(&genesis_v1[0..24]);
                genesis_v1
            } else {
                let g2 = wal_genesis_v2(initial_base_seq);
                hdr[4..8].copy_from_slice(&2u32.to_le_bytes());
                hdr[8..16].copy_from_slice(&initial_base_seq.to_le_bytes());
                hdr[16..32].copy_from_slice(&g2[0..16]);
                g2
            };
            file.write_all(&hdr).map_err(|_| CowrieError::IoError)?;
            file.sync_all().map_err(|_| CowrieError::IoError)?;
            return Ok(Self {
                path: path.to_path_buf(),
                file,
                next_seq: initial_base_seq,
                head_hash,
                entries: Vec::new(),
            });
        }

        if flen < WAL_HDR_BYTES as u64 {
            return Err(CowrieError::Truncated);
        }
        let flen_usize = usize::try_from(flen).map_err(|_| CowrieError::AllocLimit)?;
        let mut raw = Vec::with_capacity(flen_usize);
        file.seek(SeekFrom::Start(0)).map_err(|_| CowrieError::IoError)?;
        file.read_to_end(&mut raw).map_err(|_| CowrieError::IoError)?;

        if &raw[0..4] != WAL_MAGIC {
            return Err(CowrieError::BadMagic);
        }
        let ver = u32::from_le_bytes(raw[4..8].try_into().unwrap());
        let (mut prev_hash, mut seq_exp) = match ver {
            1 => {
                if raw[8..32] != genesis_v1[0..24] {
                    return Err(CowrieError::UnsupportedVer);
                }
                (genesis_v1, 0u64)
            }
            2 => {
                let bseq = u64::from_le_bytes(raw[8..16].try_into().unwrap());
                let g2 = wal_genesis_v2(bseq);
                if raw[16..32] != g2[0..16] {
                    return Err(CowrieError::UnsupportedVer);
                }
                (g2, bseq)
            }
            _ => return Err(CowrieError::UnsupportedVer),
        };

        let mut pos = WAL_HDR_BYTES;
        let mut entries = Vec::new();
        let mut truncate_to: Option<usize> = None;

        // Record wire format:
        // [0..4)   "CWLR"
        // [4]      kind
        // [5..8)   reserved (0)
        // [8..16)  seq: u64 LE
        // [16..20) payload_len: u32 LE
        // [20..52) prev_hash: [u8; 32]
        // [52..52+len) payload
        // [52+len..84+len) record_hash: [u8; 32]
        while pos < raw.len() {
            if pos + 52 > raw.len() {
                if recover_torn_tail {
                    truncate_to = Some(pos);
                    break;
                }
                return Err(CowrieError::Truncated);
            }
            // Power-loss filesystem preallocation often leaves an all-zero tail >= 52 bytes at EOF.
            if recover_torn_tail && raw[pos..].iter().all(|&b| b == 0) {
                truncate_to = Some(pos);
                break;
            }
            if &raw[pos..pos + 4] != WAL_REC_MAGIC || raw[pos + 5..pos + 8] != [0, 0, 0] {
                return Err(CowrieError::CorruptTrailer);
            }
            let kind = raw[pos + 4];
            let seq = u64::from_le_bytes(raw[pos + 8..pos + 16].try_into().unwrap());
            let plen = u32::from_le_bytes(raw[pos + 16..pos + 20].try_into().unwrap()) as usize;
            let rec_end = pos
                .checked_add(52)
                .and_then(|x| x.checked_add(plen))
                .and_then(|x| x.checked_add(32))
                .ok_or(CowrieError::Truncated)?;
            if rec_end > raw.len() {
                if recover_torn_tail {
                    truncate_to = Some(pos);
                    break;
                }
                return Err(CowrieError::Truncated);
            }

            if seq != seq_exp || raw[pos + 20..pos + 52] != prev_hash {
                return Err(CowrieError::RootMismatch);
            }
            let mut h = Sha256::new();
            h.update(&raw[pos..pos + 52 + plen]);
            let got_hash = h.finalize();
            let stored_hash: [u8; 32] = raw[pos + 52 + plen..rec_end].try_into().unwrap();
            if got_hash != stored_hash {
                // Only treat as torn tail if this is the final partial/unflushed record AND caller opted in;
                // mid-log hash mismatch ALWAYS fails closed!
                if recover_torn_tail && rec_end == raw.len() {
                    truncate_to = Some(pos);
                    break;
                }
                return Err(CowrieError::RootMismatch);
            }

            let payload = raw[pos + 52..pos + 52 + plen].to_vec();
            let cid = match kind {
                WAL_KIND_FRAME => {
                    let _ = verify_frame_zero_alloc(&payload)?;
                    Some(content_address(&payload))
                }
                WAL_KIND_TOMBSTONE => {
                    if payload.len() != 34 {
                        return Err(CowrieError::InvalidContract);
                    }
                    let mut c = [0u8; 34];
                    c.copy_from_slice(&payload);
                    Some(c)
                }
                WAL_KIND_IVF_VEC => {
                    if payload.len() <= 16 {
                        return Err(CowrieError::InvalidContract);
                    }
                    None
                }
                _ => return Err(CowrieError::InvalidContract),
            };
            prev_hash = got_hash;
            entries.push(WalEntry {
                seq,
                kind,
                payload,
                record_hash: got_hash,
                cid,
            });
            seq_exp = seq_exp.checked_add(1).ok_or(CowrieError::AllocLimit)?;
            pos = rec_end;
        }

        if let Some(cut) = truncate_to {
            file.set_len(cut as u64).map_err(|_| CowrieError::IoError)?;
            file.seek(SeekFrom::End(0)).map_err(|_| CowrieError::IoError)?;
            file.sync_all().map_err(|_| CowrieError::IoError)?;
        } else {
            file.seek(SeekFrom::End(0)).map_err(|_| CowrieError::IoError)?;
        }

        Ok(Self {
            path: path.to_path_buf(),
            file,
            next_seq: seq_exp,
            head_hash: prev_hash,
            entries,
        })
    }

    fn append_raw(
        &mut self,
        kind: u8,
        payload: &[u8],
        cid: Option<[u8; 34]>,
        fsync_now: bool,
    ) -> Result<u64, CowrieError> {
        let plen = u32::try_from(payload.len()).map_err(|_| CowrieError::AllocLimit)?;
        let seq = self.next_seq;
        // [FORMAL-F16] Check sequence overflow BEFORE writing to disk so u64::MAX never bricks the WAL.
        let next_seq = self.next_seq.checked_add(1).ok_or(CowrieError::AllocLimit)?;
        let mut rec = Vec::with_capacity(84 + payload.len());
        rec.extend_from_slice(WAL_REC_MAGIC);
        rec.push(kind);
        rec.extend_from_slice(&[0u8; 3]);
        rec.extend_from_slice(&seq.to_le_bytes());
        rec.extend_from_slice(&plen.to_le_bytes());
        rec.extend_from_slice(&self.head_hash);
        rec.extend_from_slice(payload);

        let rec_hash = sha256(&rec);
        rec.extend_from_slice(&rec_hash);

        let pre_offset = self.file.stream_position().map_err(|_| CowrieError::IoError)?;
        if self.file.write_all(&rec).is_err() {
            let _ = self.file.set_len(pre_offset);
            let _ = self.file.seek(SeekFrom::Start(pre_offset));
            return Err(CowrieError::IoError);
        }
        if fsync_now {
            self.file.sync_all().map_err(|_| CowrieError::IoError)?;
        }

        self.head_hash = rec_hash;
        self.next_seq = next_seq;
        self.entries.push(WalEntry {
            seq,
            kind,
            payload: payload.to_vec(),
            record_hash: rec_hash,
            cid,
        });
        Ok(seq)
    }

    /// Zero-alloc verify and append a canonical `COWR` frame to the WAL.
    pub fn append_frame(&mut self, frame: &[u8], fsync_now: bool) -> Result<(u64, [u8; 34]), CowrieError> {
        let _ = verify_frame_zero_alloc(frame)?;
        let cid = content_address(frame);
        let seq = self.append_raw(WAL_KIND_FRAME, frame, Some(cid), fsync_now)?;
        Ok((seq, cid))
    }

    /// Append a tombstone revoking a 34-byte `CID` on next compaction.
    pub fn append_tombstone(&mut self, cid: &[u8; 34], fsync_now: bool) -> Result<u64, CowrieError> {
        self.append_raw(WAL_KIND_TOMBSTONE, cid, Some(*cid), fsync_now)
    }

    pub fn sync(&mut self) -> Result<(), CowrieError> {
        self.file.sync_all().map_err(|_| CowrieError::IoError)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn head_hash(&self) -> [u8; 32] {
        self.head_hash
    }

    /// Return all live (non-tombstoned) canonical frames in `O(N)` via sequential log replay:
    /// `WAL_KIND_TOMBSTONE(cid)` revokes prior frames with `cid`, while a LATER re-insertion
    /// of `cid` (`append_frame` at a higher `seq`) makes `cid` live again (preventing zombie tombstones, `L14b`).
    pub fn live_frames(&self) -> Vec<&[u8]> {
        let mut slots: Vec<Option<&[u8]>> = Vec::with_capacity(self.entries.len());
        let mut by_cid: HashMap<[u8; 34], usize> = HashMap::with_capacity(self.entries.len());
        for e in &self.entries {
            match e.kind {
                WAL_KIND_FRAME => {
                    let cid = e.cid.unwrap_or_else(|| content_address(&e.payload));
                    if let Some(old_idx) = by_cid.insert(cid, slots.len()) {
                        slots[old_idx] = None;
                    }
                    slots.push(Some(e.payload.as_slice()));
                }
                WAL_KIND_TOMBSTONE if e.payload.len() == 34 => {
                    let t_cid = e.cid.unwrap_or_else(|| {
                        let mut c = [0u8; 34];
                        c.copy_from_slice(&e.payload);
                        c
                    });
                    if let Some(old_idx) = by_cid.remove(&t_cid) {
                        slots[old_idx] = None;
                    }
                }
                _ => {}
            }
        }
        slots.into_iter().flatten().collect()
    }

    /// Compact all live WAL frames into a sealed 16 KB-aligned `CWRF` shard on disk (`O_EXCL` `0600` atomic rename).
    pub fn compact_to_shard(&self, out_shard_path: &Path) -> Result<[u8; 34], CowrieError> {
        let live = self.live_frames();
        let shard = build_shard_16kb(&live, &[], 0);
        let rep = crate::shard::verify_shard_and_localize(&shard)?;
        let tmp = unique_wal_tmp_path(out_shard_path, "compact");
        let write_res = (|| -> Result<(), CowrieError> {
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&tmp)
                .map_err(|_| CowrieError::IoError)?;
            f.write_all(&shard).map_err(|_| CowrieError::IoError)?;
            f.sync_all().map_err(|_| CowrieError::IoError)?;
            Ok(())
        })();
        if let Err(e) = write_res {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        if fs::rename(&tmp, out_shard_path).is_err() {
            let _ = fs::remove_file(&tmp);
            return Err(CowrieError::IoError);
        }
        fsync_parent_dir(out_shard_path);
        Ok(rep.computed_root)
    }
}

// =========================================================================
// LIVE MUTABLE `CWIV` STORE (BASE 16 KB SHARD + WAL DELTA TAIL + COMPACT)
// =========================================================================

pub struct LiveIvfStore {
    base_path: PathBuf,
    gen_id: u64,
    applied_wal_seq_exclusive: u64,
    base: ColocatedIvfReader,
    wal: WalStore,
    delta_embs: Vec<i8>, // contiguous [delta_cnt * dim]
    delta_meta: Vec<VecMeta>,
}

impl LiveIvfStore {
    pub fn open(base_cwiv_path: &Path, wal_path: &Path) -> Result<Self, CowrieError> {
        let mpath = manifest_path_for(base_cwiv_path);
        let (base, gen_id, applied_wal_seq_exclusive) =
            if let Some(m) = LiveIvfGenManifest::read_if_exists(&mpath)? {
                let gpath = gen_shard_path_for(base_cwiv_path, m.gen_id);
                let actual_sha = sha256_file_strict(&gpath)?;
                if actual_sha != m.base_gen_sha256 {
                    return Err(CowrieError::RootMismatch);
                }
                let rdr = ColocatedIvfReader::open(&gpath)?;
                (rdr, m.gen_id, m.applied_wal_seq_exclusive)
            } else {
                let rdr = ColocatedIvfReader::open(base_cwiv_path)?;
                (rdr, 0, 0)
            };

        // [FORMAL-F10/F16] Initialize any new/empty WAL at the manifest's `applied_wal_seq_exclusive`
        // watermark and reject BOTH stale/rewound WALs (`next_seq < applied`) AND forward-gap/swapped
        // WALs (`wal_base_seq > applied`) that would silently drop committed delta sequences.
        let wal = WalStore::open_with_base_seq(wal_path, applied_wal_seq_exclusive, true)?;
        let wal_base_seq = wal.next_seq.saturating_sub(wal.entries.len() as u64);
        if wal.next_seq < applied_wal_seq_exclusive || wal_base_seq > applied_wal_seq_exclusive {
            return Err(CowrieError::InvalidContract);
        }
        let dim = base.dim;
        let mut delta_embs = Vec::new();
        let mut delta_meta = Vec::new();

        for e in &wal.entries {
            // Skip any WAL entry already committed into the active immutable base generation (crash-safe!)
            if e.seq < applied_wal_seq_exclusive {
                continue;
            }
            if e.kind == WAL_KIND_IVF_VEC {
                if e.payload.len() != 16 + dim {
                    return Err(CowrieError::InvalidContract);
                }
                let mb: &[u8; 16] = e.payload[0..16].try_into().unwrap();
                let m = VecMeta::from_le_bytes(mb);
                if !m.scale.is_finite() || !m.conf.is_finite() {
                    return Err(CowrieError::InvalidContract);
                }
                delta_meta.push(m);
                for &b in &e.payload[16..] {
                    delta_embs.push(b as i8);
                }
            }
        }
        Ok(Self {
            base_path: base_cwiv_path.to_path_buf(),
            gen_id,
            applied_wal_seq_exclusive,
            base,
            wal,
            delta_embs,
            delta_meta,
        })
    }

    pub fn delta_len(&self) -> usize {
        self.delta_meta.len()
    }

    pub fn base_vec_count(&self) -> usize {
        self.base.toc.iter().map(|e| e.vec_count as usize).sum()
    }

    pub fn total_vec_count(&self) -> usize {
        self.base_vec_count() + self.delta_meta.len()
    }

    /// Append a new INT8 vector + trust metadata into the durable WAL delta tail (`< 10 µs` buffered).
    pub fn append_vector(
        &mut self,
        emb: &[i8],
        meta: VecMeta,
        fsync_now: bool,
    ) -> Result<u64, CowrieError> {
        if emb.len() != self.base.dim || !meta.scale.is_finite() || !meta.conf.is_finite() {
            return Err(CowrieError::InvalidContract);
        }
        let mut payload = Vec::with_capacity(16 + emb.len());
        payload.extend_from_slice(&meta.to_le_bytes());
        for &x in emb {
            payload.push(x as u8);
        }
        let seq = self.wal.append_raw(WAL_KIND_IVF_VEC, &payload, None, fsync_now)?;
        self.delta_embs.extend_from_slice(emb);
        self.delta_meta.push(meta);
        Ok(seq)
    }

    /// Hybrid query over both the 16 KB `CWIV` base shard (`nprobe` clusters) AND the live SIMD delta tail.
    pub fn search_trusted(
        &self,
        query: &[i8],
        nprobe: usize,
        top_k: usize,
        conf_thresh: f32,
    ) -> Result<(Vec<IvfSearchHit>, usize), CowrieError> {
        let (mut hits, bread) = self.base.search_trusted(query, nprobe, top_k, conf_thresh)?;
        let dim = self.base.dim;
        for (j, m) in self.delta_meta.iter().enumerate() {
            if is_tainted(m.auth) || !(m.conf >= conf_thresh) || !m.scale.is_finite() {
                continue;
            }
            let row = &self.delta_embs[j * dim..(j + 1) * dim];
            let acc = dot_i8(row, query);
            let score = (acc as f32) * m.scale;
            if !score.is_finite() {
                continue;
            }
            hits.push(IvfSearchHit {
                orig_id: m.orig_id,
                score,
                conf: m.conf,
                auth: m.auth,
            });
        }
        hits.sort_by(|a, b| b.score.total_cmp(&a.score));
        hits.truncate(top_k);
        Ok((hits, bread))
    }

    /// Bounded-memory, crash-safe LSM compaction with single-point atomic generation publication:
    /// 1. Stream-write immutable `<out>.gen.<N>.cwiv` ONE cluster at a time (`O(largest_cluster)` RAM).
    /// 2. Atomically publish `<out>.cwgen` committing `(gen_id, applied_wal_seq_exclusive, base_gen_sha256)`.
    ///    *(Crash before here -> old gen + full WAL; crash right after -> new gen skips WAL `seq < applied`!)*
    /// 3. Atomically replace `.cwwal` with a fresh `ver=2` genesis starting at `base_seq = applied_wal_seq_exclusive`.
    /// 4. Best-effort publish `<out>` alias (hardlink/copy) for standalone readers & reclaim old gen file.
    pub fn compact_to_cwiv(&mut self, out_cwiv_path: &Path) -> Result<u64, CowrieError> {
        self.compact_to_cwiv_internal(out_cwiv_path, false)
    }

    #[cfg(test)]
    pub(crate) fn compact_to_cwiv_simulate_crash_before_wal_reset(
        &mut self,
        out_cwiv_path: &Path,
    ) -> Result<u64, CowrieError> {
        self.compact_to_cwiv_internal(out_cwiv_path, true)
    }

    fn compact_to_cwiv_internal(
        &mut self,
        out_cwiv_path: &Path,
        stop_before_wal_reset: bool,
    ) -> Result<u64, CowrieError> {
        let k = self.base.k;
        if k == 0 {
            return Err(CowrieError::InvalidContract);
        }
        let dim = self.base.dim;

        // Route ONLY live WAL deltas (`O(delta)` RAM) to nearest centroid before streaming base clusters.
        let mut delta_by_cluster: Vec<Vec<usize>> = vec![Vec::new(); k];
        for (j, _m) in self.delta_meta.iter().enumerate() {
            let v = &self.delta_embs[j * dim..(j + 1) * dim];
            let mut best_c = 0usize;
            let mut best_dot = i32::MIN;
            for c in 0..k {
                let cent = &self.base.centroids[c * dim..(c + 1) * dim];
                let d = dot_i8(cent, v);
                if d > best_dot {
                    best_dot = d;
                    best_c = c;
                }
            }
            delta_by_cluster[best_c].push(j);
        }

        let mut cluster_counts = Vec::with_capacity(k);
        for c in 0..k {
            let base_cnt = self.base.toc[c].vec_count as usize;
            let tot = base_cnt
                .checked_add(delta_by_cluster[c].len())
                .ok_or(CowrieError::AllocLimit)?;
            cluster_counts.push(tot);
        }

        let prev_gen_id = self.gen_id;
        let prev_base_path = self.base_path.clone();
        let new_gen_id = prev_gen_id.checked_add(1).ok_or(CowrieError::AllocLimit)?;
        let new_gen_path = gen_shard_path_for(out_cwiv_path, new_gen_id);
        let applied_wal_seq_exclusive = self.wal.next_seq;

        let base_ref = &self.base;
        let delta_embs_ref = &self.delta_embs;
        let delta_meta_ref = &self.delta_meta;
        let (total, base_gen_sha256) = write_colocated_ivf_shard_streaming(
            &new_gen_path,
            dim as u32,
            &base_ref.centroids,
            &cluster_counts,
            |c, embs_out, metas_out| {
                let entry = base_ref.toc[c];
                let cnt = entry.vec_count as usize;
                let eb = cnt * dim;
                let mut buf = vec![0u8; entry.disk_len as usize];
                base_ref
                    .file_read_at(&mut buf, entry.disk_off)
                    .map_err(|_| CowrieError::IoError)?;
                embs_out.reserve(eb + delta_by_cluster[c].len() * dim);
                metas_out.reserve(cnt + delta_by_cluster[c].len());
                embs_out.extend(buf[..eb].iter().map(|&b| b as i8));
                for j in 0..cnt {
                    let mb: &[u8; 16] = buf[eb + j * 16..eb + (j + 1) * 16].try_into().unwrap();
                    metas_out.push(VecMeta::from_le_bytes(mb));
                }
                for &dj in &delta_by_cluster[c] {
                    embs_out.extend_from_slice(&delta_embs_ref[dj * dim..(dj + 1) * dim]);
                    metas_out.push(delta_meta_ref[dj]);
                }
                Ok(())
            },
        )?;

        // Single atomic commit point: publish generation manifest naming `(new_gen_id, applied_wal_seq_exclusive, sha256)`.
        let manifest = LiveIvfGenManifest {
            gen_id: new_gen_id,
            applied_wal_seq_exclusive,
            base_gen_sha256,
        };
        manifest.write_atomic(&manifest_path_for(out_cwiv_path))?;

        if stop_before_wal_reset {
            return Ok(total);
        }

        // [FORMAL-F16] Atomically replace WAL with fresh empty `ver=2` genesis starting at
        // `applied_wal_seq_exclusive` while holding the exclusive `flock` across `rename()`.
        let tmp_wal_path = unique_wal_tmp_path(&self.wal.path, "reset");
        let mut new_wal = WalStore::open_with_base_seq(&tmp_wal_path, applied_wal_seq_exclusive, false)?;
        if fs::rename(&tmp_wal_path, &self.wal.path).is_err() {
            drop(new_wal);
            let _ = fs::remove_file(&tmp_wal_path);
            return Err(CowrieError::IoError);
        }
        fsync_parent_dir(&self.wal.path);
        new_wal.path = self.wal.path.clone();

        // Best-effort publish active `out_cwiv_path` alias (hardlink/copy + atomic rename) for standalone readers.
        let alias_tmp = unique_wal_tmp_path(out_cwiv_path, "alias");
        let linked = fs::hard_link(&new_gen_path, &alias_tmp).is_ok()
            || fs::copy(&new_gen_path, &alias_tmp).is_ok();
        if linked {
            if fs::rename(&alias_tmp, out_cwiv_path).is_ok() {
                fsync_parent_dir(out_cwiv_path);
            } else {
                let _ = fs::remove_file(&alias_tmp);
            }
        }

        let reopened_base = ColocatedIvfReader::open(&new_gen_path)?;
        self.base = reopened_base;
        self.base_path = out_cwiv_path.to_path_buf();
        self.gen_id = new_gen_id;
        self.applied_wal_seq_exclusive = applied_wal_seq_exclusive;
        self.wal = new_wal;
        self.delta_embs.clear();
        self.delta_meta.clear();

        // Reclaim superseded immutable generation file if present.
        if prev_gen_id > 0 {
            let old_gen_path = gen_shard_path_for(&prev_base_path, prev_gen_id);
            if old_gen_path != new_gen_path {
                let _ = fs::remove_file(&old_gen_path);
            }
        }
        Ok(total)
    }
}
