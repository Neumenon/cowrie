//! Plane C Profile B (v1.1): Co-Located 16 KB-Aligned IVF-INT8 Vector + Trust Index.
//!
//! Each cluster block `c` packs `[embs_c (cnt * D bytes) || meta_c (cnt * 16 bytes) || 0x00 pad -> 16 KB]`
//! contiguously at a strict 16,384-byte offset so out-of-core queries issue ONE concurrent `pread`/`O_DIRECT`
//! read per probed cluster (`QD = nprobe`) over the bounded `WorkerPool` with AVX2 / NEON INT8 dot products,
//! strict bounds validation, and hardware-level `0x04` / `0x80` trust filtering.

#[cfg(unix)]
use std::fs::{File, OpenOptions};
#[cfg(unix)]
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(unix)]
use std::path::Path;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(unix)]
use crate::sha256::parallel_for;
#[cfg(unix)]
use crate::shard::{align_16kb, PAGE_ALIGN};
#[cfg(unix)]
use crate::trust::is_tainted;
#[cfg(unix)]
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

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct IvfSearchHit {
    pub orig_id: u32,
    pub score: f32,
    pub conf: f32,
    pub auth: u8,
}

#[inline]
pub fn dot_i8_scalar(a: &[i8], b: &[i8]) -> i32 {
    debug_assert_eq!(a.len(), b.len());
    let mut acc: i32 = 0;
    for i in 0..a.len() {
        acc += (a[i] as i32) * (b[i] as i32);
    }
    acc
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn dot_i8_avx2(a: &[i8], b: &[i8]) -> i32 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;

    let n = a.len();
    let pa = a.as_ptr();
    let pb = b.as_ptr();
    let mut sum256 = _mm256_setzero_si256();
    let mut i = 0usize;

    while i + 32 <= n {
        let va = _mm256_loadu_si256(pa.add(i) as *const __m256i);
        let vb = _mm256_loadu_si256(pb.add(i) as *const __m256i);

        let va_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(va));
        let vb_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(vb));
        let prod_lo = _mm256_madd_epi16(va_lo, vb_lo);

        let va_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(va, 1));
        let vb_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(vb, 1));
        let prod_hi = _mm256_madd_epi16(va_hi, vb_hi);

        sum256 = _mm256_add_epi32(sum256, _mm256_add_epi32(prod_lo, prod_hi));
        i += 32;
    }

    let sum128 = _mm_add_epi32(
        _mm256_castsi256_si128(sum256),
        _mm256_extracti128_si256(sum256, 1),
    );
    let mut lanes = [0i32; 4];
    _mm_storeu_si128(lanes.as_mut_ptr() as *mut __m128i, sum128);
    let mut total = lanes[0] + lanes[1] + lanes[2] + lanes[3];

    while i < n {
        total += (*pa.add(i) as i32) * (*pb.add(i) as i32);
        i += 1;
    }
    total
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn dot_i8_neon(a: &[i8], b: &[i8]) -> i32 {
    use std::arch::aarch64::*;
    let n = a.len();
    let pa = a.as_ptr();
    let pb = b.as_ptr();
    let mut acc = vdupq_n_s32(0);
    let mut i = 0usize;

    while i + 16 <= n {
        let va = vld1q_s8(pa.add(i));
        let vb = vld1q_s8(pb.add(i));
        let prod_lo = vmull_s8(vget_low_s8(va), vget_low_s8(vb));
        let prod_hi = vmull_s8(vget_high_s8(va), vget_high_s8(vb));
        acc = vpadalq_s16(acc, prod_lo);
        acc = vpadalq_s16(acc, prod_hi);
        i += 16;
    }
    let mut total = vaddvq_s32(acc);
    while i < n {
        total += (*pa.add(i) as i32) * (*pb.add(i) as i32);
        i += 1;
    }
    total
}

/// Hardware-accelerated signed INT8 dot product (`AVX2` on x86_64, `NEON` on AArch64, exact scalar tail).
#[inline]
pub fn dot_i8(a: &[i8], b: &[i8]) -> i32 {
    debug_assert_eq!(a.len(), b.len());
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if a.len() >= 32 && std::is_x86_feature_detected!("avx2") {
            return unsafe { dot_i8_avx2(a, b) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if a.len() >= 16 {
            return unsafe { dot_i8_neon(a, b) };
        }
    }
    dot_i8_scalar(a, b)
}

/// Write a co-located 16 KB-aligned IVF-INT8 shard to disk.
#[cfg(unix)]
pub fn write_colocated_ivf_shard(
    path: &Path,
    dim: u32,
    centroids: &[i8],
    clusters: &[(&[i8], &[VecMeta])],
) -> Result<u64, CowrieError> {
    if dim == 0 || dim > 65536 {
        return Err(CowrieError::InvalidContract);
    }
    let k = clusters.len() as u32;
    let exp_cent = (k as usize)
        .checked_mul(dim as usize)
        .ok_or(CowrieError::AllocLimit)?;
    if centroids.len() != exp_cent {
        return Err(CowrieError::InvalidContract);
    }

    let off_cent = PAGE_ALIGN as u64;
    let cent_len = centroids.len();
    let off_toc = align_16kb((off_cent as usize) + cent_len) as u64;
    let toc_bytes = (k as usize).checked_mul(24).ok_or(CowrieError::AllocLimit)?;
    let mut cur_off = align_16kb((off_toc as usize) + toc_bytes) as u64;

    let mut toc = Vec::with_capacity(clusters.len());
    for (embs, metas) in clusters {
        let exp_eb = metas
            .len()
            .checked_mul(dim as usize)
            .ok_or(CowrieError::AllocLimit)?;
        if embs.len() != exp_eb {
            return Err(CowrieError::InvalidContract);
        }
        let raw_b = embs
            .len()
            .checked_add(metas.len().checked_mul(16).ok_or(CowrieError::AllocLimit)?)
            .ok_or(CowrieError::AllocLimit)?;
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

    let cent_u8: &[u8] = unsafe { std::slice::from_raw_parts(centroids.as_ptr() as *const u8, cent_len) };
    f.write_all(cent_u8).map_err(|_| CowrieError::IoError)?;
    let cent_pad = (off_toc as usize) - ((off_cent as usize) + cent_len);
    if cent_pad > 0 {
        f.write_all(&vec![0u8; cent_pad]).map_err(|_| CowrieError::IoError)?;
    }

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

/// Out-of-core co-located IVF-INT8 shard reader with bounded `WorkerPool` `pread`,
/// strict header/TOC bounds enforcement, SIMD INT8 kernels, and hardware trust filtering.
#[cfg(unix)]
pub struct ColocatedIvfReader {
    file: File,
    pub k: usize,
    pub dim: usize,
    pub centroids: Vec<i8>,
    pub toc: Vec<ClusterTocEntry>,
}

#[cfg(unix)]
impl ColocatedIvfReader {
    #[inline]
    pub fn file_read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        self.file.read_exact_at(buf, offset)
    }

    pub fn open(path: &Path) -> Result<Self, CowrieError> {
        let file = File::open(path).map_err(|_| CowrieError::IoError)?;
        let file_len = file.metadata().map_err(|_| CowrieError::IoError)?.len();
        if file_len < PAGE_ALIGN as u64 {
            return Err(CowrieError::Truncated);
        }

        let mut hdr = [0u8; 32];
        file.read_exact_at(&mut hdr, 0).map_err(|_| CowrieError::Truncated)?;
        if &hdr[0..4] != IVF_MAGIC {
            return Err(CowrieError::BadMagic);
        }
        let ver = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
        if ver != 1 {
            return Err(CowrieError::UnsupportedVer);
        }
        let k = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
        let dim = u32::from_le_bytes(hdr[12..16].try_into().unwrap()) as usize;
        if dim == 0 || dim > 65536 || k > 1_048_576 {
            return Err(CowrieError::InvalidContract);
        }
        let off_cent = u64::from_le_bytes(hdr[16..24].try_into().unwrap());
        let off_toc = u64::from_le_bytes(hdr[24..32].try_into().unwrap());
        if (off_cent as usize) % PAGE_ALIGN != 0 || (off_toc as usize) % PAGE_ALIGN != 0 {
            return Err(CowrieError::UnalignedArena);
        }

        let cent_bytes = k.checked_mul(dim).ok_or(CowrieError::AllocLimit)?;
        let toc_bytes = k.checked_mul(24).ok_or(CowrieError::AllocLimit)?;
        if off_cent
            .checked_add(cent_bytes as u64)
            .map_or(true, |end| end > file_len)
            || off_toc
                .checked_add(toc_bytes as u64)
                .map_or(true, |end| end > file_len)
        {
            return Err(CowrieError::Truncated);
        }

        let mut cent_raw = vec![0u8; cent_bytes];
        file.read_exact_at(&mut cent_raw, off_cent).map_err(|_| CowrieError::Truncated)?;
        let centroids: Vec<i8> = cent_raw.into_iter().map(|b| b as i8).collect();

        let mut toc_raw = vec![0u8; toc_bytes];
        file.read_exact_at(&mut toc_raw, off_toc).map_err(|_| CowrieError::Truncated)?;
        let per_vec_bytes = dim.checked_add(16).ok_or(CowrieError::AllocLimit)?;
        let mut toc = Vec::with_capacity(k);

        for c in 0..k {
            let s = &toc_raw[c * 24..(c + 1) * 24];
            let disk_off = u64::from_le_bytes(s[0..8].try_into().unwrap());
            let disk_len = u64::from_le_bytes(s[8..16].try_into().unwrap());
            let vec_count = u32::from_le_bytes(s[16..20].try_into().unwrap());
            let reserved = u32::from_le_bytes(s[20..24].try_into().unwrap());
            if reserved != 0 {
                return Err(CowrieError::NonCanonicalFlags);
            }
            if (disk_off as usize) % PAGE_ALIGN != 0
                || (disk_len as usize) % PAGE_ALIGN != 0
                || disk_len == 0
            {
                return Err(CowrieError::UnalignedArena);
            }
            if disk_off.checked_add(disk_len).map_or(true, |end| end > file_len) {
                return Err(CowrieError::Truncated);
            }
            let min_needed = (vec_count as usize)
                .checked_mul(per_vec_bytes)
                .ok_or(CowrieError::AllocLimit)?;
            if min_needed > (disk_len as usize) {
                return Err(CowrieError::InvalidContract);
            }
            toc.push(ClusterTocEntry { disk_off, disk_len, vec_count });
        }

        Ok(Self { file, k, dim, centroids, toc })
    }

    /// Out-of-core search: score resident centroids via SIMD INT8 -> read ONLY `nprobe` co-located
    /// 16 KB blocks concurrently on the bounded `WorkerPool` -> filter out tainted/low-conf -> Top-K.
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
        if self.k == 0 || nprobe == 0 || top_k == 0 {
            return Ok((Vec::new(), 0));
        }

        let mut cscores: Vec<(i32, usize)> = (0..self.k)
            .map(|c| {
                let row = &self.centroids[c * self.dim..(c + 1) * self.dim];
                (dot_i8(row, query), c)
            })
            .collect();
        let np = nprobe.min(self.k);
        cscores.select_nth_unstable_by(np.saturating_sub(1), |a, b| b.0.cmp(&a.0));
        let chosen: Vec<ClusterTocEntry> = cscores[..np].iter().map(|&(_, c)| self.toc[c]).collect();

        let bytes_read: usize = chosen.iter().map(|e| e.disk_len as usize).sum();
        let mut per_cluster_hits: Vec<Vec<IvfSearchHit>> = vec![Vec::new(); chosen.len()];
        let hits_ptr = per_cluster_hits.as_mut_ptr() as usize;
        let io_failed = AtomicBool::new(false);
        let dim = self.dim;
        let file = &self.file;

        parallel_for(chosen.len(), |idx| {
            let entry = chosen[idx];
            let mut buf = vec![0u8; entry.disk_len as usize];
            if file.read_exact_at(&mut buf, entry.disk_off).is_err() {
                io_failed.store(true, Ordering::Release);
                return;
            }
            let cnt = entry.vec_count as usize;
            let eb = cnt * dim;
            let mut local_hits = Vec::new();
            for j in 0..cnt {
                let m_bytes: &[u8; 16] = buf[eb + j * 16..eb + (j + 1) * 16].try_into().unwrap();
                let m = VecMeta::from_le_bytes(m_bytes);
                if is_tainted(m.auth) || !(m.conf >= conf_thresh) {
                    continue;
                }
                let row_u8 = &buf[j * dim..(j + 1) * dim];
                let row_i8: &[i8] = unsafe { std::slice::from_raw_parts(row_u8.as_ptr() as *const i8, dim) };
                let acc = dot_i8(row_i8, query);
                local_hits.push(IvfSearchHit {
                    orig_id: m.orig_id,
                    score: (acc as f32) * m.scale,
                    conf: m.conf,
                    auth: m.auth,
                });
            }
            unsafe {
                let slot = (hits_ptr as *mut Vec<IvfSearchHit>).add(idx);
                *slot = local_hits;
            }
        });

        if io_failed.load(Ordering::Acquire) {
            return Err(CowrieError::IoError);
        }

        let mut all: Vec<IvfSearchHit> = per_cluster_hits.into_iter().flatten().collect();
        all.sort_by(|a, b| b.score.total_cmp(&a.score));
        all.truncate(top_k);
        Ok((all, bytes_read))
    }
}
