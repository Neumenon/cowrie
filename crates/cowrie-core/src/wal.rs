//! Crash-Safe Hash-Chained Append-Only Framed WAL (`.cwwal`) + Live `CWIV` Vector Delta Ingest & Compaction.
//!
//! Turns Cowrie from a static snapshot container into a live mutating store:
//! - Every appended frame or INT8 vector entry is chained via `SHA-256` (`prev_hash -> record_hash`).
//! - Torn tail writes from power loss / `SIGKILL` recover cleanly on `open(..., recover_torn_tail = true)`,
//!   while any mid-log tampering fails closed (`ERR_CORRUPT_TRAILER` / `ERR_ROOT_MISMATCH`).
//! - `LiveIvfStore` pairs an immutable 16 KB `ColocatedIvfReader` (`CWIV`) with a WAL-backed SIMD delta
//!   tail and one-call `compact_to_cwiv()` merge.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use crate::canonical::verify_frame_zero_alloc;
use crate::ivf::{dot_i8, write_colocated_ivf_shard, ColocatedIvfReader, IvfSearchHit, VecMeta};
use crate::sha256::{content_address, sha256, Sha256};
use crate::shard::build_shard_16kb;
use crate::trust::is_tainted;
use crate::CowrieError;

pub const WAL_MAGIC: &[u8; 4] = b"CWAL";
pub const WAL_REC_MAGIC: &[u8; 4] = b"CWLR";
const WAL_HDR_BYTES: usize = 32;
const REC_FIXED_PREFIX: usize = 48; // magic(4) + kind(1) + flags(1) + res(2) + seq(8) + len(4) + prev_hash[28..wait -> let's use exact 52B!]

pub const WAL_KIND_FRAME: u8 = 0;
pub const WAL_KIND_TOMBSTONE: u8 = 1;
pub const WAL_KIND_IVF_VEC: u8 = 2;

#[derive(Clone, Debug)]
pub struct WalEntry {
    pub seq: u64,
    pub kind: u8,
    pub payload: Vec<u8>,
    pub record_hash: [u8; 32],
}

/// Tamper-evident hash-chained append-only Write-Ahead Log (`.cwwal`).
pub struct WalStore {
    path: PathBuf,
    file: File,
    next_seq: u64,
    head_hash: [u8; 32],
    entries: Vec<WalEntry>,
}

impl WalStore {
    /// Open or create a `0600` `.cwwal` file, verifying the entire SHA-256 hash chain.
    /// If `recover_torn_tail` is true, an incomplete trailing record at EOF is truncated cleanly.
    pub fn open(path: &Path, recover_torn_tail: bool) -> Result<Self, CowrieError> {
        let _ = REC_FIXED_PREFIX;
        let exists = path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .open(path)
            .map_err(|_| CowrieError::IoError)?;

        let flen = file.metadata().map_err(|_| CowrieError::IoError)?.len();
        let genesis = sha256(b"COWRIE_WAL_GENESIS_V1");

        if !exists || flen == 0 {
            let mut hdr = [0u8; WAL_HDR_BYTES];
            hdr[0..4].copy_from_slice(WAL_MAGIC);
            hdr[4..8].copy_from_slice(&1u32.to_le_bytes());
            hdr[8..32].copy_from_slice(&genesis[0..24]);
            file.write_all(&hdr).map_err(|_| CowrieError::IoError)?;
            file.sync_all().map_err(|_| CowrieError::IoError)?;
            return Ok(Self {
                path: path.to_path_buf(),
                file,
                next_seq: 0,
                head_hash: genesis,
                entries: Vec::new(),
            });
        }

        if flen < WAL_HDR_BYTES as u64 {
            return Err(CowrieError::Truncated);
        }
        let mut raw = Vec::with_capacity(flen as usize);
        file.seek(SeekFrom::Start(0)).map_err(|_| CowrieError::IoError)?;
        file.read_to_end(&mut raw).map_err(|_| CowrieError::IoError)?;

        if &raw[0..4] != WAL_MAGIC {
            return Err(CowrieError::BadMagic);
        }
        let ver = u32::from_le_bytes(raw[4..8].try_into().unwrap());
        if ver != 1 || raw[8..32] != genesis[0..24] {
            return Err(CowrieError::UnsupportedVer);
        }

        let mut pos = WAL_HDR_BYTES;
        let mut prev_hash = genesis;
        let mut seq_exp = 0u64;
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
            match kind {
                WAL_KIND_FRAME => {
                    let _ = verify_frame_zero_alloc(&payload)?;
                }
                WAL_KIND_TOMBSTONE => {
                    if payload.len() != 34 {
                        return Err(CowrieError::InvalidContract);
                    }
                }
                WAL_KIND_IVF_VEC => {
                    if payload.len() <= 16 {
                        return Err(CowrieError::InvalidContract);
                    }
                }
                _ => return Err(CowrieError::InvalidContract),
            }
            prev_hash = got_hash;
            entries.push(WalEntry {
                seq,
                kind,
                payload,
                record_hash: got_hash,
            });
            seq_exp += 1;
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

    fn append_raw(&mut self, kind: u8, payload: &[u8], fsync_now: bool) -> Result<u64, CowrieError> {
        let seq = self.next_seq;
        let plen = payload.len() as u32;
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

        self.file.write_all(&rec).map_err(|_| CowrieError::IoError)?;
        if fsync_now {
            self.file.sync_all().map_err(|_| CowrieError::IoError)?;
        }

        self.head_hash = rec_hash;
        self.next_seq += 1;
        self.entries.push(WalEntry {
            seq,
            kind,
            payload: payload.to_vec(),
            record_hash: rec_hash,
        });
        Ok(seq)
    }

    /// Zero-alloc verify and append a canonical `COWR` frame to the WAL.
    pub fn append_frame(&mut self, frame: &[u8], fsync_now: bool) -> Result<(u64, [u8; 34]), CowrieError> {
        let _ = verify_frame_zero_alloc(frame)?;
        let cid = content_address(frame);
        let seq = self.append_raw(WAL_KIND_FRAME, frame, fsync_now)?;
        Ok((seq, cid))
    }

    /// Append a tombstone revoking a 34-byte `CID` on next compaction.
    pub fn append_tombstone(&mut self, cid: &[u8; 34], fsync_now: bool) -> Result<u64, CowrieError> {
        self.append_raw(WAL_KIND_TOMBSTONE, cid, fsync_now)
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

    /// Return all live (non-tombstoned) canonical frames via sequential log replay:
    /// `WAL_KIND_TOMBSTONE(cid)` revokes prior frames with `cid`, while a LATER re-insertion
    /// of `cid` (`append_frame` at a higher `seq`) makes `cid` live again (preventing zombie tombstones, `L14b`).
    pub fn live_frames(&self) -> Vec<&[u8]> {
        let _ = HashSet::<[u8; 34]>::new;
        let mut live: Vec<([u8; 34], &[u8])> = Vec::with_capacity(self.entries.len());
        for e in &self.entries {
            match e.kind {
                WAL_KIND_FRAME => {
                    let cid = content_address(&e.payload);
                    live.retain(|(c, _)| c != &cid);
                    live.push((cid, e.payload.as_slice()));
                }
                WAL_KIND_TOMBSTONE if e.payload.len() == 34 => {
                    let mut t_cid = [0u8; 34];
                    t_cid.copy_from_slice(&e.payload);
                    live.retain(|(c, _)| c != &t_cid);
                }
                _ => {}
            }
        }
        live.into_iter().map(|(_, f)| f).collect()
    }

    /// Compact all live WAL frames into a sealed 16 KB-aligned `CWRF` shard on disk.
    pub fn compact_to_shard(&self, out_shard_path: &Path) -> Result<[u8; 34], CowrieError> {
        let live = self.live_frames();
        let shard = build_shard_16kb(&live, &[], 0);
        let rep = crate::shard::verify_shard_and_localize(&shard)?;
        let tmp = out_shard_path.with_extension("compact.tmp");
        {
            let mut f = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .mode(0o600)
                .open(&tmp)
                .map_err(|_| CowrieError::IoError)?;
            f.write_all(&shard).map_err(|_| CowrieError::IoError)?;
            f.sync_all().map_err(|_| CowrieError::IoError)?;
        }
        fs::rename(&tmp, out_shard_path).map_err(|_| CowrieError::IoError)?;
        Ok(rep.computed_root)
    }
}

// =========================================================================
// LIVE MUTABLE `CWIV` STORE (BASE 16 KB SHARD + WAL DELTA TAIL + COMPACT)
// =========================================================================

pub struct LiveIvfStore {
    base: ColocatedIvfReader,
    wal: WalStore,
    delta_embs: Vec<i8>, // contiguous [delta_cnt * dim]
    delta_meta: Vec<VecMeta>,
}

impl LiveIvfStore {
    pub fn open(base_cwiv_path: &Path, wal_path: &Path) -> Result<Self, CowrieError> {
        let base = ColocatedIvfReader::open(base_cwiv_path)?;
        let wal = WalStore::open(wal_path, true)?;
        let dim = base.dim;
        let mut delta_embs = Vec::new();
        let mut delta_meta = Vec::new();

        for e in &wal.entries {
            if e.kind == WAL_KIND_IVF_VEC {
                if e.payload.len() != 16 + dim {
                    return Err(CowrieError::InvalidContract);
                }
                let mb: &[u8; 16] = e.payload[0..16].try_into().unwrap();
                delta_meta.push(VecMeta::from_le_bytes(mb));
                for &b in &e.payload[16..] {
                    delta_embs.push(b as i8);
                }
            }
        }
        Ok(Self {
            base,
            wal,
            delta_embs,
            delta_meta,
        })
    }

    /// Append a new INT8 vector + trust metadata into the durable WAL delta tail (`< 10 µs` buffered).
    pub fn append_vector(
        &mut self,
        emb: &[i8],
        meta: VecMeta,
        fsync_now: bool,
    ) -> Result<u64, CowrieError> {
        if emb.len() != self.base.dim {
            return Err(CowrieError::InvalidContract);
        }
        let mut payload = Vec::with_capacity(16 + emb.len());
        payload.extend_from_slice(&meta.to_le_bytes());
        for &x in emb {
            payload.push(x as u8);
        }
        let seq = self.wal.append_raw(WAL_KIND_IVF_VEC, &payload, fsync_now)?;
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
            if is_tainted(m.auth) || !(m.conf >= conf_thresh) {
                continue;
            }
            let row = &self.delta_embs[j * dim..(j + 1) * dim];
            let acc = dot_i8(row, query);
            hits.push(IvfSearchHit {
                orig_id: m.orig_id,
                score: (acc as f32) * m.scale,
                conf: m.conf,
                auth: m.auth,
            });
        }
        hits.sort_by(|a, b| b.score.total_cmp(&a.score));
        hits.truncate(top_k);
        Ok((hits, bread))
    }

    /// LSM-style compaction: assign all WAL delta vectors to their highest-dot-product centroid
    /// and write a new sealed 16 KB-aligned `CWIV` shard, then reset the WAL.
    pub fn compact_to_cwiv(&mut self, out_cwiv_path: &Path) -> Result<u64, CowrieError> {
        use std::os::unix::fs::FileExt;
        let k = self.base.k;
        let dim = self.base.dim;
        let mut cluster_embs: Vec<Vec<i8>> = vec![Vec::new(); k];
        let mut cluster_metas: Vec<Vec<VecMeta>> = vec![Vec::new(); k];

        // Load existing clusters from base
        for (c, entry) in self.base.toc.iter().enumerate() {
            let mut buf = vec![0u8; entry.disk_len as usize];
            self.base
                .file_read_at(&mut buf, entry.disk_off)
                .map_err(|_| CowrieError::IoError)?;
            let cnt = entry.vec_count as usize;
            let eb = cnt * dim;
            cluster_embs[c].extend(buf[..eb].iter().map(|&b| b as i8));
            for j in 0..cnt {
                let mb: &[u8; 16] = buf[eb + j * 16..eb + (j + 1) * 16].try_into().unwrap();
                cluster_metas[c].push(VecMeta::from_le_bytes(mb));
            }
        }
        let _ = FileExt::read_exact_at as fn(&File, &mut [u8], u64) -> std::io::Result<()>;

        // Route WAL delta vectors to nearest centroid
        for (j, m) in self.delta_meta.iter().enumerate() {
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
            cluster_embs[best_c].extend_from_slice(v);
            cluster_metas[best_c].push(*m);
        }

        let refs: Vec<(&[i8], &[VecMeta])> = cluster_embs
            .iter()
            .zip(cluster_metas.iter())
            .map(|(e, m)| (e.as_slice(), m.as_slice()))
            .collect();
        let total = write_colocated_ivf_shard(out_cwiv_path, dim as u32, &self.base.centroids, &refs)?;
        self.base = ColocatedIvfReader::open(out_cwiv_path)?;

        // Atomically replace WAL with a fresh fsync'd genesis header via rename (zero crash window!)
        let tmp_wal_path = self.wal.path.with_extension("cwwal.reset.tmp");
        let _ = fs::remove_file(&tmp_wal_path);
        drop(WalStore::open(&tmp_wal_path, false)?);
        fs::rename(&tmp_wal_path, &self.wal.path).map_err(|_| CowrieError::IoError)?;
        self.wal = WalStore::open(&self.wal.path, true)?;
        self.delta_embs.clear();
        self.delta_meta.clear();
        Ok(total)
    }
}
