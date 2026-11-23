//! Plane C Profile B (v1.0): Co-Located 16 KB-Aligned IVF-INT8 Vector + Trust Index.
//!
//! Each cluster block `c` packs `[embs_c (cnt * D bytes) || meta_c (cnt * 16 bytes) || 0x00 pad -> 16 KB]`
//! contiguously at a strict 16,384-byte offset so out-of-core queries issue ONE concurrent `pread`/`O_DIRECT`
//! read per probed cluster (`QD = nprobe`) with zero split seeks and hardware-level `0x04` trust filtering.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::shard::{align_16kb, PAGE_ALIGN};
use crate::trust::is_tainted;
use crate::CowrieError;

pub const IVF_MAGIC: &[u8; 4] = b"CWIV";

/// 16-byte aligned metadata struct matching CUDA `VecMeta` byte-for-byte.
#[repr(C, align(16))]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VecMeta {
    pub scale: f32,
    pub conf: f32,
    pub orig_id: u32,
    pub auth: u8,
    pub pad: [u8; 3],
}

impl VecMeta {
    pub fn to_le_bytes(self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[0..4].copy_from_slice(&self.scale.to_le_bytes());
        out[4..8].copy_from_slice(&self.conf.to_le_bytes());
        out[8..12].copy_from_slice(&self.orig_id.to_le_bytes());
        out[12] = self.auth;
        out
    }

    pub fn from_le_bytes(b: &[u8; 16]) -> Self {
        Self {
            scale: f32::from_le_bytes(b[0..4].try_into().unwrap()),
            conf: f32::from_le_bytes(b[4..8].try_into().unwrap()),
            orig_id: u32::from_le_bytes(b[8..12].try_into().unwrap()),
            auth: b[12],
            pad: [b[13], b[14], b[15]],
        }
    }
}

/// 24-byte Table-of-Contents entry pointing to one co-located 16 KB-aligned cluster block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClusterTocEntry {
    pub disk_off: u64,
    pub disk_len: u64,
    pub vec_count: u32,
}

#[derive(Clone, Debug)]
pub struct IvfSearchHit {
    pub orig_id: u32,
    pub score: f32,
    pub conf: f32,
    pub auth: u8,
}

/// Write a co-located 16 KB-aligned IVF-INT8 shard to disk.
/// `clusters[c]` contains `(embeddings_i8: Vec<i8> [len = cnt*dim], metas: Vec<VecMeta> [len = cnt])`.
pub fn write_colocated_ivf_shard(
    path: &Path,
    dim: u32,
    centroids: &[i8],
    clusters: &[(&[i8], &[VecMeta])],
) -> Result<u64, CowrieError> {
    let k = clusters.len() as u32;
    if centroids.len() != (k as usize) * (dim as usize) {
        return Err(CowrieError::InvalidContract);
    }

    let off_cent = PAGE_ALIGN as u64;
    let cent_len = centroids.len();
    let off_toc = align_16kb((off_cent as usize) + cent_len) as u64;
    let toc_bytes = (k as usize) * 24;
    let mut cur_off = align_16kb((off_toc as usize) + toc_bytes) as u64;

    let mut toc = Vec::with_capacity(clusters.len());
    for (embs, metas) in clusters {
        if embs.len() != metas.len() * (dim as usize) {
            return Err(CowrieError::InvalidContract);
        }
        let raw_b = embs.len() + metas.len() * 16;
        let blk_b = align_16kb(raw_b.max(1));
        toc.push(ClusterTocEntry {
            disk_off: cur_off,
            disk_len: blk_b as u64,
            vec_count: metas.len() as u32,
        });
        cur_off += blk_b as u64;
    }

    let mut hdr = vec![0u8; PAGE_ALIGN];
    hdr[0..4].copy_from_slice(IVF_MAGIC);
    hdr[4..8].copy_from_slice(&1u32.to_le_bytes());
    hdr[8..12].copy_from_slice(&k.to_le_bytes());
    hdr[12..16].copy_from_slice(&dim.to_le_bytes());
    hdr[16..24].copy_from_slice(&off_cent.to_le_bytes());
    hdr[24..32].copy_from_slice(&off_toc.to_le_bytes());

    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(|_| CowrieError::IoError)?;
    f.write_all(&hdr).map_err(|_| CowrieError::IoError)?;

    // Centroids (padded to 16 KB)
    let cent_u8: &[u8] = unsafe { std::slice::from_raw_parts(centroids.as_ptr() as *const u8, cent_len) };
    f.write_all(cent_u8).map_err(|_| CowrieError::IoError)?;
    let cent_pad = (off_toc as usize) - ((off_cent as usize) + cent_len);
    if cent_pad > 0 {
        f.write_all(&vec![0u8; cent_pad]).map_err(|_| CowrieError::IoError)?;
    }

    // TOC (padded to 16 KB)
    let mut toc_buf = Vec::with_capacity(toc_bytes);
    for e in &toc {
        toc_buf.extend_from_slice(&e.disk_off.to_le_bytes());
        toc_buf.extend_from_slice(&e.disk_len.to_le_bytes());
        toc_buf.extend_from_slice(&e.vec_count.to_le_bytes());
        toc_buf.extend_from_slice(&0u32.to_le_bytes());
    }
    f.write_all(&toc_buf).map_err(|_| CowrieError::IoError)?;
    let first_blk = toc.first().map(|e| e.disk_off as usize).unwrap_or(cur_off as usize);
    let toc_pad = first_blk - ((off_toc as usize) + toc_bytes);
    if toc_pad > 0 {
        f.write_all(&vec![0u8; toc_pad]).map_err(|_| CowrieError::IoError)?;
    }

    // Co-located 16 KB-aligned cluster blocks: [embs_c || meta_c || 0x00 pad]
    for ((embs, metas), entry) in clusters.iter().zip(toc.iter()) {
        let mut blk = vec![0u8; entry.disk_len as usize];
        let eb = embs.len();
        let e_u8: &[u8] = unsafe { std::slice::from_raw_parts(embs.as_ptr() as *const u8, eb) };
        blk[..eb].copy_from_slice(e_u8);
        for (j, m) in metas.iter().enumerate() {
            blk[eb + j * 16..eb + (j + 1) * 16].copy_from_slice(&m.to_le_bytes());
        }
        f.write_all(&blk).map_err(|_| CowrieError::IoError)?;
    }
    f.sync_all().map_err(|_| CowrieError::IoError)?;
    Ok(cur_off)
}

/// Out-of-core co-located IVF-INT8 shard reader with parallel QD=nprobe `pread` + trust filtering.
pub struct ColocatedIvfReader {
    file: File,
    pub k: usize,
    pub dim: usize,
    pub centroids: Vec<i8>,
    pub toc: Vec<ClusterTocEntry>,
}

impl ColocatedIvfReader {
    pub fn open(path: &Path) -> Result<Self, CowrieError> {
        let file = File::open(path).map_err(|_| CowrieError::IoError)?;
        let mut hdr = [0u8; 32];
        file.read_exact_at(&mut hdr, 0).map_err(|_| CowrieError::Truncated)?;
        if &hdr[0..4] != IVF_MAGIC {
            return Err(CowrieError::BadMagic);
        }
        let k = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
        let dim = u32::from_le_bytes(hdr[12..16].try_into().unwrap()) as usize;
        let off_cent = u64::from_le_bytes(hdr[16..24].try_into().unwrap());
        let off_toc = u64::from_le_bytes(hdr[24..32].try_into().unwrap());
        if (off_cent as usize) % PAGE_ALIGN != 0 || (off_toc as usize) % PAGE_ALIGN != 0 {
            return Err(CowrieError::UnalignedArena);
        }

        let mut cent_raw = vec![0u8; k * dim];
        file.read_exact_at(&mut cent_raw, off_cent).map_err(|_| CowrieError::Truncated)?;
        let centroids: Vec<i8> = cent_raw.into_iter().map(|b| b as i8).collect();

        let mut toc_raw = vec![0u8; k * 24];
        file.read_exact_at(&mut toc_raw, off_toc).map_err(|_| CowrieError::Truncated)?;
        let mut toc = Vec::with_capacity(k);
        for c in 0..k {
            let s = &toc_raw[c * 24..(c + 1) * 24];
            let disk_off = u64::from_le_bytes(s[0..8].try_into().unwrap());
            let disk_len = u64::from_le_bytes(s[8..16].try_into().unwrap());
            let vec_count = u32::from_le_bytes(s[16..20].try_into().unwrap());
            if (disk_off as usize) % PAGE_ALIGN != 0 || (disk_len as usize) % PAGE_ALIGN != 0 {
                return Err(CowrieError::UnalignedArena);
            }
            toc.push(ClusterTocEntry { disk_off, disk_len, vec_count });
        }

        Ok(Self { file, k, dim, centroids, toc })
    }

    /// Out-of-core search: score resident centroids -> read ONLY `nprobe` co-located 16 KB blocks
    /// concurrently via `std::thread::scope` `read_exact_at` -> filter out tainted/low-conf records -> Top-K.
    pub fn search_trusted(
        &self,
        query: &[i8],
        nprobe: usize,
        top_k: usize,
        conf_thresh: f32,
    ) -> Result<(Vec<IvfSearchHit>, usize), CowrieError> {
        if query.len() != self.dim {
            return Err(CowrieError::InvalidContract);
        }
        let mut cscores: Vec<(i32, usize)> = (0..self.k)
            .map(|c| {
                let row = &self.centroids[c * self.dim..(c + 1) * self.dim];
                let dot: i32 = row.iter().zip(query.iter()).map(|(&a, &b)| (a as i32) * (b as i32)).sum();
                (dot, c)
            })
            .collect();
        let np = nprobe.min(self.k);
        cscores.select_nth_unstable_by(np.saturating_sub(1), |a, b| b.0.cmp(&a.0));
        let chosen: Vec<ClusterTocEntry> = cscores[..np].iter().map(|&(_, c)| self.toc[c]).collect();

        let bytes_read: usize = chosen.iter().map(|e| e.disk_len as usize).sum();
        let mut per_cluster_hits: Vec<Vec<IvfSearchHit>> = vec![Vec::new(); chosen.len()];
        let dim = self.dim;
        let file = &self.file;

        std::thread::scope(|s| {
            for (entry, out_slot) in chosen.iter().zip(per_cluster_hits.iter_mut()) {
                s.spawn(move || {
                    let mut buf = vec![0u8; entry.disk_len as usize];
                    if file.read_exact_at(&mut buf, entry.disk_off).is_err() {
                        return;
                    }
                    let cnt = entry.vec_count as usize;
                    let eb = cnt * dim;
                    for j in 0..cnt {
                        let m_bytes: &[u8; 16] = buf[eb + j * 16..eb + (j + 1) * 16].try_into().unwrap();
                        let m = VecMeta::from_le_bytes(m_bytes);
                        if is_tainted(m.auth) || !(m.conf >= conf_thresh) {
                            continue;
                        }
                        let row = &buf[j * dim..(j + 1) * dim];
                        let mut acc: i32 = 0;
                        for d in 0..dim {
                            acc += (row[d] as i8 as i32) * (query[d] as i32);
                        }
                        out_slot.push(IvfSearchHit {
                            orig_id: m.orig_id,
                            score: (acc as f32) * m.scale,
                            conf: m.conf,
                            auth: m.auth,
                        });
                    }
                });
            }
        });

        let mut all: Vec<IvfSearchHit> = per_cluster_hits.into_iter().flatten().collect();
        all.sort_by(|a, b| b.score.total_cmp(&a.score));
        all.truncate(top_k);
        Ok((all, bytes_read))
    }
}
