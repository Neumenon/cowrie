//! Plane D Scalar Zone-Maps & 16 KB Page-Targeted Sparse Index (`CWZM`).
//!
//! Eliminates the `< 9%` cold-disk selectivity gap vs SQLite B-trees by packing canonical
//! `COWR` frames into self-verifying 16,384-byte leaf pages and storing a compact 64-byte
//! cache-line-aligned `(min_key, max_key, min_sec, max_sec, page_merkle_leaf)` Zone-Map
//! directory in Plane D. Point and narrow-range queries `pread` ONLY matching 16 KB pages,
//! verifying each page's hardware SHA-NI Merkle leaf and filtering `0x04`/`0x80` taint in O(1).

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::canonical::verify_frame_zero_alloc;
use crate::sha256::{merkle_leaf, parallel_for, shard_merkle_root_from_leaves};
use crate::shard::{checked_align_16kb, PAGE_ALIGN};
use crate::trust::is_tainted;
use crate::CowrieError;

pub const ZONEMAP_MAGIC: &[u8; 4] = b"CWZM";
pub const ZONE_ENTRY_BYTES: usize = 64; // 4 * i64 (32B) + leaf_sha256 (32B) = 1 CPU cache line!
const REC_SLOT_BYTES: usize = 20;       // key:i64 + sec:i64 + rel_off:u16 + len:u16
const MAX_ZONED_PAGES: usize = 4_194_304;

static ZM_TMP_NONCE: AtomicU64 = AtomicU64::new(1);

#[cfg(all(target_os = "linux", any(target_arch = "aarch64", target_arch = "arm")))]
const ZM_O_NOFOLLOW: i32 = 0o100000;
#[cfg(all(target_os = "linux", not(any(target_arch = "aarch64", target_arch = "arm"))))]
const ZM_O_NOFOLLOW: i32 = 0o400000;
#[cfg(target_os = "macos")]
const ZM_O_NOFOLLOW: i32 = 0x0100;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
const ZM_O_NOFOLLOW: i32 = 0;

fn unique_zm_tmp_path(path: &Path) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let fname = path.file_name().and_then(|s| s.to_str()).unwrap_or("shard.cwzm");
    let seq = ZM_TMP_NONCE.fetch_add(1, Ordering::Relaxed);
    parent.join(format!(".{}.{}.{}.tmp", fname, std::process::id(), seq))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageZoneEntry {
    pub min_key: i64,
    pub max_key: i64,
    pub min_sec: i64,
    pub max_sec: i64,
    pub leaf_sha256: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct ZonedRecord<'a> {
    pub primary_key: i64,
    pub secondary_val: i64,
    pub frame: &'a [u8],
}

/// Build a 16 KB page-indexed, Merkle-sealed `CWZM` shard sorted by `primary_key`.
/// Returns `(total_file_bytes, page_count, merkle_root_34)`.
pub fn write_zoned_shard(
    path: &Path,
    records: &mut [ZonedRecord<'_>],
) -> Result<(u64, u32, [u8; 34]), CowrieError> {
    records.sort_by_key(|r| r.primary_key);
    for r in records.iter() {
        if r.frame.len() > PAGE_ALIGN - 32 {
            return Err(CowrieError::InvalidContract);
        }
        let _ = verify_frame_zero_alloc(r.frame)?;
    }

    let mut pages: Vec<[u8; PAGE_ALIGN]> = Vec::new();
    let mut zones: Vec<PageZoneEntry> = Vec::new();
    let mut leaves: Vec<[u8; 32]> = Vec::new();

    let mut idx = 0usize;
    while idx < records.len() {
        // Greedily pack records into a 16,384-byte page:
        // Header = 4 bytes (`count: u16, reserved: u16`), directory grows forward (`count * 20`),
        // canonical `COWR` frames pack contiguously immediately after the directory!
        let start = idx;
        let mut payload_sum = 0usize;
        while idx < records.len() {
            let next_cnt = (idx - start) + 1;
            let needed = 4 + next_cnt * REC_SLOT_BYTES + payload_sum + records[idx].frame.len();
            if needed > PAGE_ALIGN {
                break;
            }
            payload_sum += records[idx].frame.len();
            idx += 1;
        }
        let slice = &records[start..idx];
        let cnt = slice.len();
        let mut page = [0u8; PAGE_ALIGN];
        page[0..2].copy_from_slice(&(cnt as u16).to_le_bytes());

        let mut data_pos = 4 + cnt * REC_SLOT_BYTES;
        let mut min_k = i64::MAX;
        let mut max_k = i64::MIN;
        let mut min_s = i64::MAX;
        let mut max_s = i64::MIN;

        for (j, rec) in slice.iter().enumerate() {
            min_k = min_k.min(rec.primary_key);
            max_k = max_k.max(rec.primary_key);
            min_s = min_s.min(rec.secondary_val);
            max_s = max_s.max(rec.secondary_val);

            let flen = rec.frame.len();
            let slot_off = 4 + j * REC_SLOT_BYTES;
            page[slot_off..slot_off + 8].copy_from_slice(&rec.primary_key.to_le_bytes());
            page[slot_off + 8..slot_off + 16].copy_from_slice(&rec.secondary_val.to_le_bytes());
            page[slot_off + 16..slot_off + 18].copy_from_slice(&(data_pos as u16).to_le_bytes());
            page[slot_off + 18..slot_off + 20].copy_from_slice(&(flen as u16).to_le_bytes());

            page[data_pos..data_pos + flen].copy_from_slice(rec.frame);
            data_pos += flen;
        }

        let leaf = merkle_leaf(&page);
        leaves.push(leaf);
        zones.push(PageZoneEntry {
            min_key: min_k,
            max_key: max_k,
            min_sec: min_s,
            max_sec: max_s,
            leaf_sha256: leaf,
        });
        pages.push(page);
    }

    let page_count = u32::try_from(pages.len()).map_err(|_| CowrieError::AllocLimit)?;
    let pages_off = PAGE_ALIGN as u64;
    let pages_bytes = (pages.len() as u64)
        .checked_mul(PAGE_ALIGN as u64)
        .ok_or(CowrieError::AllocLimit)?;
    let zonemap_off = pages_off.checked_add(pages_bytes).ok_or(CowrieError::AllocLimit)?;
    let zm_raw_bytes = zones
        .len()
        .checked_mul(ZONE_ENTRY_BYTES)
        .ok_or(CowrieError::AllocLimit)?;
    let zm_padded = checked_align_16kb(zm_raw_bytes.max(1))?;
    let total_bytes = zonemap_off
        .checked_add(zm_padded as u64)
        .ok_or(CowrieError::AllocLimit)?;

    let mut zm_buf = vec![0u8; zm_padded];
    let mut zone_leaves: Vec<[u8; 32]> = Vec::with_capacity(zones.len());
    for (p, z) in zones.iter().enumerate() {
        let base = p * ZONE_ENTRY_BYTES;
        zm_buf[base..base + 8].copy_from_slice(&z.min_key.to_le_bytes());
        zm_buf[base + 8..base + 16].copy_from_slice(&z.max_key.to_le_bytes());
        zm_buf[base + 16..base + 24].copy_from_slice(&z.min_sec.to_le_bytes());
        zm_buf[base + 24..base + 32].copy_from_slice(&z.max_sec.to_le_bytes());
        zm_buf[base + 32..base + 64].copy_from_slice(&z.leaf_sha256);
        // Bind BOTH the 32-byte min/max fence bounds AND the 32-byte page_leaf_sha256 into the Merkle tree
        // so tampering with Zone-Map fence pointers cannot silently censor pages (`formal/LAWS.bend` L12).
        zone_leaves.push(merkle_leaf(&zm_buf[base..base + ZONE_ENTRY_BYTES]));
    }
    let _ = leaves;
    let mroot = shard_merkle_root_from_leaves(&zone_leaves, &[]);

    let mut hdr = [0u8; PAGE_ALIGN];
    hdr[0..4].copy_from_slice(ZONEMAP_MAGIC);
    hdr[4..8].copy_from_slice(&1u32.to_le_bytes());
    hdr[8..12].copy_from_slice(&page_count.to_le_bytes());
    hdr[16..24].copy_from_slice(&(records.len() as u64).to_le_bytes());
    hdr[24..32].copy_from_slice(&pages_off.to_le_bytes());
    hdr[32..40].copy_from_slice(&zonemap_off.to_le_bytes());
    hdr[40..48].copy_from_slice(&(zm_raw_bytes as u64).to_le_bytes());
    hdr[48..82].copy_from_slice(&mroot);

    let tmp_path = unique_zm_tmp_path(path);
    let write_res = (|| -> Result<(), CowrieError> {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp_path)
            .map_err(|_| CowrieError::IoError)?;
        f.write_all(&hdr).map_err(|_| CowrieError::IoError)?;
        for pg in &pages {
            f.write_all(pg).map_err(|_| CowrieError::IoError)?;
        }
        f.write_all(&zm_buf).map_err(|_| CowrieError::IoError)?;
        f.sync_all().map_err(|_| CowrieError::IoError)?;
        Ok(())
    })();
    if let Err(e) = write_res {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }
    if fs::rename(&tmp_path, path).is_err() {
        let _ = fs::remove_file(&tmp_path);
        return Err(CowrieError::IoError);
    }
    if let Some(parent) = path.parent() {
        let p = if parent.as_os_str().is_empty() { Path::new(".") } else { parent };
        if let Ok(df) = File::open(p) {
            let _ = df.sync_all();
        }
    }

    Ok((total_bytes, page_count, mroot))
}

#[derive(Clone, Debug)]
pub struct ZonedQueryHit {
    pub primary_key: i64,
    pub secondary_val: i64,
    pub authority: u8,
    pub frame: Vec<u8>,
}

/// Out-of-core 16 KB page-targeted reader with Zone-Map pruning, binary-search fence pointers,
/// hardware SHA-NI per-page Merkle verification, and byte-`0x05` taint filtering.
pub struct ZonedShardReader {
    file: File,
    pub record_count: u64,
    pub pages_off: u64,
    pub merkle_root: [u8; 34],
    pub zones: Vec<PageZoneEntry>,
}

impl ZonedShardReader {
    pub fn open(path: &Path) -> Result<Self, CowrieError> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(ZM_O_NOFOLLOW)
            .open(path)
            .map_err(|_| CowrieError::IoError)?;
        let meta = file.metadata().map_err(|_| CowrieError::IoError)?;
        if !meta.file_type().is_file() {
            return Err(CowrieError::InvalidContract);
        }
        let flen = meta.len();
        if flen < PAGE_ALIGN as u64 {
            return Err(CowrieError::Truncated);
        }
        let mut hdr = [0u8; PAGE_ALIGN];
        file.read_exact_at(&mut hdr, 0).map_err(|_| CowrieError::Truncated)?;
        if &hdr[0..4] != ZONEMAP_MAGIC {
            return Err(CowrieError::BadMagic);
        }
        let ver = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
        if ver != 1 {
            return Err(CowrieError::UnsupportedVer);
        }
        if hdr[12..16] != [0, 0, 0, 0] || hdr[82..PAGE_ALIGN].iter().any(|&b| b != 0) {
            return Err(CowrieError::NonCanonicalFlags);
        }
        let page_count = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
        if page_count > MAX_ZONED_PAGES {
            return Err(CowrieError::AllocLimit);
        }
        let record_count = u64::from_le_bytes(hdr[16..24].try_into().unwrap());
        // [FORMAL-F17] Reject impossible record_count vs page_count header combinations.
        if (page_count == 0 && record_count != 0) || record_count < page_count as u64 {
            return Err(CowrieError::InvalidContract);
        }
        let pages_off = u64::from_le_bytes(hdr[24..32].try_into().unwrap());
        let zonemap_off = u64::from_le_bytes(hdr[32..40].try_into().unwrap());
        let zm_raw_bytes_u64 = u64::from_le_bytes(hdr[40..48].try_into().unwrap());
        let zm_raw_bytes = usize::try_from(zm_raw_bytes_u64).map_err(|_| CowrieError::AllocLimit)?;
        let mut merkle_root = [0u8; 34];
        merkle_root.copy_from_slice(&hdr[48..82]);

        if pages_off != PAGE_ALIGN as u64
            || zonemap_off % (PAGE_ALIGN as u64) != 0
        {
            return Err(CowrieError::UnalignedArena);
        }
        let pages_bytes = (page_count as u64)
            .checked_mul(PAGE_ALIGN as u64)
            .ok_or(CowrieError::AllocLimit)?;
        let pages_end = pages_off.checked_add(pages_bytes).ok_or(CowrieError::Truncated)?;
        if pages_end != zonemap_off || pages_end > flen {
            return Err(CowrieError::InvalidContract);
        }
        let exp_zm = page_count.checked_mul(ZONE_ENTRY_BYTES).ok_or(CowrieError::AllocLimit)?;
        let exp_zm_padded = checked_align_16kb(exp_zm.max(1))?;
        let exp_total = zonemap_off
            .checked_add(exp_zm_padded as u64)
            .ok_or(CowrieError::Truncated)?;
        if zm_raw_bytes != exp_zm || exp_total != flen {
            return Err(CowrieError::Truncated);
        }

        let mut zm_buf = vec![0u8; exp_zm_padded];
        file.read_exact_at(&mut zm_buf, zonemap_off).map_err(|_| CowrieError::Truncated)?;
        if zm_buf[zm_raw_bytes..].iter().any(|&b| b != 0) {
            return Err(CowrieError::NonZeroPad);
        }
        let zm_raw = &zm_buf[..zm_raw_bytes];
        let mut zones: Vec<PageZoneEntry> = Vec::with_capacity(page_count);
        let mut zone_leaves = Vec::with_capacity(page_count);
        for p in 0..page_count {
            let b = &zm_raw[p * ZONE_ENTRY_BYTES..(p + 1) * ZONE_ENTRY_BYTES];
            zone_leaves.push(merkle_leaf(b));
            let mut leaf = [0u8; 32];
            leaf.copy_from_slice(&b[32..64]);
            let z = PageZoneEntry {
                min_key: i64::from_le_bytes(b[0..8].try_into().unwrap()),
                max_key: i64::from_le_bytes(b[8..16].try_into().unwrap()),
                min_sec: i64::from_le_bytes(b[16..24].try_into().unwrap()),
                max_sec: i64::from_le_bytes(b[24..32].try_into().unwrap()),
                leaf_sha256: leaf,
            };
            if z.min_key > z.max_key || z.min_sec > z.max_sec {
                return Err(CowrieError::InvalidContract);
            }
            if p > 0 && zones[p - 1].max_key > z.min_key {
                return Err(CowrieError::InvalidContract);
            }
            zones.push(z);
        }

        let computed = shard_merkle_root_from_leaves(&zone_leaves, &[]);
        if computed != merkle_root {
            return Err(CowrieError::RootMismatch);
        }

        Ok(Self {
            file,
            record_count,
            pages_off,
            merkle_root,
            zones,
        })
    }

    fn scan_selected_pages<P>(
        &self,
        page_indices: &[usize],
        trusted_only: bool,
        verify_merkle: bool,
        pred: P,
    ) -> Result<(Vec<ZonedQueryHit>, usize), CowrieError>
    where
        P: Fn(i64, i64) -> bool + Sync,
    {
        if page_indices.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let mut per_page: Vec<Vec<ZonedQueryHit>> = vec![Vec::new(); page_indices.len()];
        let out_ptr = per_page.as_mut_ptr() as usize;
        let failed = AtomicBool::new(false);
        let bad_hash = AtomicBool::new(false);

        parallel_for(page_indices.len(), |slot_idx| {
            let p_idx = page_indices[slot_idx];
            let Some(disk_off) = (p_idx as u64)
                .checked_mul(PAGE_ALIGN as u64)
                .and_then(|o| self.pages_off.checked_add(o))
            else {
                bad_hash.store(true, Ordering::Release);
                return;
            };
            let mut buf = [0u8; PAGE_ALIGN];
            if self.file.read_exact_at(&mut buf, disk_off).is_err() {
                failed.store(true, Ordering::Release);
                return;
            }
            if verify_merkle && merkle_leaf(&buf) != self.zones[p_idx].leaf_sha256 {
                bad_hash.store(true, Ordering::Release);
                return;
            }
            let cnt = u16::from_le_bytes([buf[0], buf[1]]) as usize;
            let dir_end = 4 + cnt * REC_SLOT_BYTES;
            if buf[2] != 0 || buf[3] != 0 || dir_end > PAGE_ALIGN {
                bad_hash.store(true, Ordering::Release);
                return;
            }
            let z_meta = &self.zones[p_idx];
            let mut hits = Vec::new();
            for j in 0..cnt {
                let s = 4 + j * REC_SLOT_BYTES;
                let k = i64::from_le_bytes(buf[s..s + 8].try_into().unwrap());
                let sec = i64::from_le_bytes(buf[s + 8..s + 16].try_into().unwrap());
                if k < z_meta.min_key || k > z_meta.max_key || sec < z_meta.min_sec || sec > z_meta.max_sec {
                    bad_hash.store(true, Ordering::Release);
                    return;
                }
                if !pred(k, sec) {
                    continue;
                }
                let roff = u16::from_le_bytes([buf[s + 16], buf[s + 17]]) as usize;
                let flen = u16::from_le_bytes([buf[s + 18], buf[s + 19]]) as usize;
                if roff < dir_end || roff + flen > PAGE_ALIGN || flen < 8 {
                    bad_hash.store(true, Ordering::Release);
                    return;
                }
                let fb = &buf[roff..roff + flen];
                let Ok((auth, _)) = verify_frame_zero_alloc(fb) else {
                    bad_hash.store(true, Ordering::Release);
                    return;
                };
                if trusted_only && is_tainted(auth) {
                    continue;
                }
                hits.push(ZonedQueryHit {
                    primary_key: k,
                    secondary_val: sec,
                    authority: auth,
                    frame: fb.to_vec(),
                });
            }
            unsafe {
                let dst = (out_ptr as *mut Vec<ZonedQueryHit>).add(slot_idx);
                *dst = hits;
            }
        });

        if bad_hash.load(Ordering::Acquire) {
            return Err(CowrieError::RootMismatch);
        }
        if failed.load(Ordering::Acquire) {
            return Err(CowrieError::IoError);
        }
        let bytes_read = page_indices.len() * PAGE_ALIGN;
        Ok((per_page.into_iter().flatten().collect(), bytes_read))
    }

    /// Primary-key point or range lookup (`primary_key in [lo..=hi]`): O(log P) binary search + targeted 16 KB `pread`.
    pub fn query_key_range(
        &self,
        lo: i64,
        hi: i64,
        trusted_only: bool,
        verify_merkle: bool,
    ) -> Result<(Vec<ZonedQueryHit>, usize), CowrieError> {
        if lo > hi || self.zones.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let first_p = self.zones.partition_point(|z| z.max_key < lo);
        let mut matching_pages = Vec::new();
        for p in first_p..self.zones.len() {
            if self.zones[p].min_key > hi {
                break;
            }
            matching_pages.push(p);
        }
        self.scan_selected_pages(&matching_pages, trusted_only, verify_merkle, |k, _| k >= lo && k <= hi)
    }

    /// Secondary-column min/max Zone-Map pruned scan (`secondary_val in [lo..=hi]`).
    pub fn query_secondary_range(
        &self,
        lo: i64,
        hi: i64,
        trusted_only: bool,
        verify_merkle: bool,
    ) -> Result<(Vec<ZonedQueryHit>, usize), CowrieError> {
        if lo > hi || self.zones.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let matching_pages: Vec<usize> = self
            .zones
            .iter()
            .enumerate()
            .filter_map(|(p, z)| if z.max_sec >= lo && z.min_sec <= hi { Some(p) } else { None })
            .collect();
        self.scan_selected_pages(&matching_pages, trusted_only, verify_merkle, |_, s| s >= lo && s <= hi)
    }
}
