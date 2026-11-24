//! Audited SHA-256 with runtime x86_64 SHA-NI and AArch64 SHA2 hardware acceleration,
//! RFC 2104 HMAC-SHA256, 34-byte multihash CIDs, RFC 6962 count-bound Merkle roots
//! (covering both Plane A frames and Plane C tensor arenas), and a bounded reusable
//! worker pool for deterministic multi-core 4 MiB chunked Merkle hashing and IVF reads.

#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::{Condvar, Mutex, OnceLock};

use crate::canonical::encode_uvarint;

pub const STATEPAGE_CHUNK_BYTES: usize = 4 * 1024 * 1024; // 4 MiB chunks for parallel HW SHA-256
pub const MAX_POOL_WORKERS: usize = 16;

const K256: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

#[inline]
fn compress_block_scalar(h: &mut [u32; 8], block: &[u8; 64]) {
    let mut w = [0u32; 64];
    for i in 0..16 {
        w[i] = u32::from_be_bytes([
            block[4 * i],
            block[4 * i + 1],
            block[4 * i + 2],
            block[4 * i + 3],
        ]);
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
    }
    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = *h;
    for i in 0..64 {
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ ((!e) & g);
        let t1 = hh.wrapping_add(s1).wrapping_add(ch).wrapping_add(K256[i]).wrapping_add(w[i]);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let t2 = s0.wrapping_add(maj);
        hh = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);
    }
    h[0] = h[0].wrapping_add(a);
    h[1] = h[1].wrapping_add(b);
    h[2] = h[2].wrapping_add(c);
    h[3] = h[3].wrapping_add(d);
    h[4] = h[4].wrapping_add(e);
    h[5] = h[5].wrapping_add(f);
    h[6] = h[6].wrapping_add(g);
    h[7] = h[7].wrapping_add(hh);
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sha,ssse3,sse4.1")]
unsafe fn compress_blocks_shani(h: &mut [u32; 8], blocks: &[u8]) {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;

    let mut state0 = _mm_set_epi32(h[0] as i32, h[1] as i32, h[4] as i32, h[5] as i32);
    let mut state1 = _mm_set_epi32(h[2] as i32, h[3] as i32, h[6] as i32, h[7] as i32);
    let shuf_mask = _mm_set_epi8(
        12, 13, 14, 15, 8, 9, 10, 11, 4, 5, 6, 7, 0, 1, 2, 3,
    );

    let mut off = 0usize;
    while off + 64 <= blocks.len() {
        let p = blocks.as_ptr().add(off) as *const __m128i;
        let abef_save = state0;
        let cdgh_save = state1;

        let mut msg0 = _mm_shuffle_epi8(_mm_loadu_si128(p), shuf_mask);
        let mut msg1 = _mm_shuffle_epi8(_mm_loadu_si128(p.add(1)), shuf_mask);
        let mut msg2 = _mm_shuffle_epi8(_mm_loadu_si128(p.add(2)), shuf_mask);
        let mut msg3 = _mm_shuffle_epi8(_mm_loadu_si128(p.add(3)), shuf_mask);

        macro_rules! kvec {
            ($i:expr) => {
                _mm_set_epi32(
                    K256[$i + 3] as i32,
                    K256[$i + 2] as i32,
                    K256[$i + 1] as i32,
                    K256[$i] as i32,
                )
            };
        }

        let mut msg = _mm_add_epi32(msg0, kvec!(0));
        state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
        msg = _mm_shuffle_epi32(msg, 0x0E);
        state0 = _mm_sha256rnds2_epu32(state0, state1, msg);

        msg = _mm_add_epi32(msg1, kvec!(4));
        state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
        msg = _mm_shuffle_epi32(msg, 0x0E);
        state0 = _mm_sha256rnds2_epu32(state0, state1, msg);
        msg0 = _mm_sha256msg1_epu32(msg0, msg1);

        msg = _mm_add_epi32(msg2, kvec!(8));
        state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
        msg = _mm_shuffle_epi32(msg, 0x0E);
        state0 = _mm_sha256rnds2_epu32(state0, state1, msg);
        msg1 = _mm_sha256msg1_epu32(msg1, msg2);

        msg = _mm_add_epi32(msg3, kvec!(12));
        state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
        let mut tmp = _mm_alignr_epi8(msg3, msg2, 4);
        msg0 = _mm_add_epi32(msg0, tmp);
        msg0 = _mm_sha256msg2_epu32(msg0, msg3);
        msg = _mm_shuffle_epi32(msg, 0x0E);
        state0 = _mm_sha256rnds2_epu32(state0, state1, msg);
        msg2 = _mm_sha256msg1_epu32(msg2, msg3);

        macro_rules! round4_sched {
            ($k:expr, $m_cur:ident, $m_prev1:ident, $m_prev2:ident, $m_next:ident) => {
                msg = _mm_add_epi32($m_cur, kvec!($k));
                state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
                tmp = _mm_alignr_epi8($m_cur, $m_prev1, 4);
                $m_next = _mm_add_epi32($m_next, tmp);
                $m_next = _mm_sha256msg2_epu32($m_next, $m_cur);
                msg = _mm_shuffle_epi32(msg, 0x0E);
                state0 = _mm_sha256rnds2_epu32(state0, state1, msg);
                $m_prev1 = _mm_sha256msg1_epu32($m_prev1, $m_cur);
            };
        }

        round4_sched!(16, msg0, msg3, msg2, msg1);
        round4_sched!(20, msg1, msg0, msg3, msg2);
        round4_sched!(24, msg2, msg1, msg0, msg3);
        round4_sched!(28, msg3, msg2, msg1, msg0);
        round4_sched!(32, msg0, msg3, msg2, msg1);
        round4_sched!(36, msg1, msg0, msg3, msg2);
        round4_sched!(40, msg2, msg1, msg0, msg3);
        round4_sched!(44, msg3, msg2, msg1, msg0);
        round4_sched!(48, msg0, msg3, msg2, msg1);

        msg = _mm_add_epi32(msg1, kvec!(52));
        state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
        tmp = _mm_alignr_epi8(msg1, msg0, 4);
        msg2 = _mm_add_epi32(msg2, tmp);
        msg2 = _mm_sha256msg2_epu32(msg2, msg1);
        msg = _mm_shuffle_epi32(msg, 0x0E);
        state0 = _mm_sha256rnds2_epu32(state0, state1, msg);

        msg = _mm_add_epi32(msg2, kvec!(56));
        state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
        tmp = _mm_alignr_epi8(msg2, msg1, 4);
        msg3 = _mm_add_epi32(msg3, tmp);
        msg3 = _mm_sha256msg2_epu32(msg3, msg2);
        msg = _mm_shuffle_epi32(msg, 0x0E);
        state0 = _mm_sha256rnds2_epu32(state0, state1, msg);

        msg = _mm_add_epi32(msg3, kvec!(60));
        state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
        msg = _mm_shuffle_epi32(msg, 0x0E);
        state0 = _mm_sha256rnds2_epu32(state0, state1, msg);

        state0 = _mm_add_epi32(state0, abef_save);
        state1 = _mm_add_epi32(state1, cdgh_save);
        off += 64;
    }

    let mut s0 = [0u32; 4];
    let mut s1 = [0u32; 4];
    _mm_storeu_si128(s0.as_mut_ptr() as *mut __m128i, state0);
    _mm_storeu_si128(s1.as_mut_ptr() as *mut __m128i, state1);
    h[0] = s0[3];
    h[1] = s0[2];
    h[2] = s1[3];
    h[3] = s1[2];
    h[4] = s0[1];
    h[5] = s0[0];
    h[6] = s1[1];
    h[7] = s1[0];
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "sha2")]
unsafe fn compress_blocks_arm_sha2(h: &mut [u32; 8], blocks: &[u8]) {
    use std::arch::aarch64::*;

    let mut abcd = vld1q_u32(h.as_ptr());
    let mut efgh = vld1q_u32(h.as_ptr().add(4));

    let mut off = 0usize;
    while off + 64 <= blocks.len() {
        let p = blocks.as_ptr().add(off);
        let abcd_save = abcd;
        let efgh_save = efgh;

        let mut msg0 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(p)));
        let mut msg1 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(p.add(16))));
        let mut msg2 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(p.add(32))));
        let mut msg3 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(p.add(48))));

        macro_rules! arm_round4 {
            ($k:expr, $m0:ident, $m1:ident, $m2:ident, $m3:ident, sched) => {{
                let wk = vaddq_u32($m0, vld1q_u32(K256.as_ptr().add($k)));
                let t = abcd;
                abcd = vsha256hq_u32(abcd, efgh, wk);
                efgh = vsha256h2q_u32(efgh, t, wk);
                $m0 = vsha256su1q_u32(vsha256su0q_u32($m0, $m1), $m2, $m3);
            }};
            ($k:expr, $m0:ident) => {{
                let wk = vaddq_u32($m0, vld1q_u32(K256.as_ptr().add($k)));
                let t = abcd;
                abcd = vsha256hq_u32(abcd, efgh, wk);
                efgh = vsha256h2q_u32(efgh, t, wk);
            }};
        }

        arm_round4!(0, msg0, msg1, msg2, msg3, sched);
        arm_round4!(4, msg1, msg2, msg3, msg0, sched);
        arm_round4!(8, msg2, msg3, msg0, msg1, sched);
        arm_round4!(12, msg3, msg0, msg1, msg2, sched);
        arm_round4!(16, msg0, msg1, msg2, msg3, sched);
        arm_round4!(20, msg1, msg2, msg3, msg0, sched);
        arm_round4!(24, msg2, msg3, msg0, msg1, sched);
        arm_round4!(28, msg3, msg0, msg1, msg2, sched);
        arm_round4!(32, msg0, msg1, msg2, msg3, sched);
        arm_round4!(36, msg1, msg2, msg3, msg0, sched);
        arm_round4!(40, msg2, msg3, msg0, msg1, sched);
        arm_round4!(44, msg3, msg0, msg1, msg2, sched);
        arm_round4!(48, msg0);
        arm_round4!(52, msg1);
        arm_round4!(56, msg2);
        arm_round4!(60, msg3);

        abcd = vaddq_u32(abcd, abcd_save);
        efgh = vaddq_u32(efgh, efgh_save);
        off += 64;
    }

    vst1q_u32(h.as_mut_ptr(), abcd);
    vst1q_u32(h.as_mut_ptr().add(4), efgh);
}

/// Returns true if hardware SHA-256 instructions are active (`x86_64` SHA-NI or `aarch64` SHA2).
#[inline]
pub fn has_hw_sha256() -> bool {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        return std::is_x86_feature_detected!("sha")
            && std::is_x86_feature_detected!("ssse3")
            && std::is_x86_feature_detected!("sse4.1");
    }
    #[cfg(target_arch = "aarch64")]
    {
        return std::arch::is_aarch64_feature_detected!("sha2");
    }
    #[allow(unreachable_code)]
    false
}

/// Preserved alias for C-ABI / Python / CLI callers; returns true on x86 SHA-NI or ARMv8 SHA2.
#[inline]
pub fn has_shani() -> bool {
    has_hw_sha256()
}

/// Streaming SHA-256 hasher with automatic x86_64 SHA-NI / AArch64 SHA2 acceleration.
#[derive(Clone)]
pub struct Sha256 {
    h: [u32; 8],
    buf: [u8; 64],
    buf_len: usize,
    total_len: u64,
    use_hw: bool,
}

impl Default for Sha256 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256 {
    pub fn new() -> Self {
        Self {
            h: [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
            ],
            buf: [0u8; 64],
            buf_len: 0,
            total_len: 0,
            use_hw: has_hw_sha256(),
        }
    }

    /// Force pure scalar mode (used for cross-verification unit tests).
    pub fn new_scalar() -> Self {
        let mut s = Self::new();
        s.use_hw = false;
        s
    }

    #[inline]
    fn compress_slice(&mut self, full_blocks: &[u8]) {
        if full_blocks.is_empty() {
            return;
        }
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if self.use_hw {
            unsafe { compress_blocks_shani(&mut self.h, full_blocks) };
            return;
        }
        #[cfg(target_arch = "aarch64")]
        if self.use_hw {
            unsafe { compress_blocks_arm_sha2(&mut self.h, full_blocks) };
            return;
        }
        for ch in full_blocks.chunks_exact(64) {
            compress_block_scalar(&mut self.h, ch.try_into().unwrap());
        }
    }

    pub fn update(&mut self, mut data: &[u8]) {
        self.total_len = self.total_len.wrapping_add(data.len() as u64);
        if self.buf_len > 0 {
            let need = 64 - self.buf_len;
            if data.len() < need {
                self.buf[self.buf_len..self.buf_len + data.len()].copy_from_slice(data);
                self.buf_len += data.len();
                return;
            }
            self.buf[self.buf_len..64].copy_from_slice(&data[..need]);
            let blk = self.buf;
            self.compress_slice(&blk);
            self.buf_len = 0;
            data = &data[need..];
        }
        let full_len = data.len() & !63usize;
        if full_len > 0 {
            self.compress_slice(&data[..full_len]);
        }
        let rem = &data[full_len..];
        if !rem.is_empty() {
            self.buf[..rem.len()].copy_from_slice(rem);
            self.buf_len = rem.len();
        }
    }

    pub fn finalize(mut self) -> [u8; 32] {
        let bit_len = self.total_len.wrapping_mul(8);
        self.buf[self.buf_len] = 0x80;
        self.buf_len += 1;
        if self.buf_len > 56 {
            for b in &mut self.buf[self.buf_len..64] {
                *b = 0;
            }
            let blk = self.buf;
            self.compress_slice(&blk);
            self.buf_len = 0;
        }
        for b in &mut self.buf[self.buf_len..56] {
            *b = 0;
        }
        self.buf[56..64].copy_from_slice(&bit_len.to_be_bytes());
        let blk = self.buf;
        self.compress_slice(&blk);

        let mut out = [0u8; 32];
        for (i, w) in self.h.iter().enumerate() {
            out[4 * i..4 * i + 4].copy_from_slice(&w.to_be_bytes());
        }
        out
    }
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize()
}

pub fn sha256_hex(data: &[u8]) -> String {
    to_hex(&sha256(data))
}

/// Pure-Rust RFC 2104 HMAC-SHA256 using hardware SHA-NI / ARMv8 SHA2 when available.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut k_pad = [0u8; 64];
    if key.len() > 64 {
        let kh = sha256(key);
        k_pad[..32].copy_from_slice(&kh);
    } else {
        k_pad[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= k_pad[i];
        opad[i] ^= k_pad[i];
    }
    let mut inner = Sha256::new();
    inner.update(&ipad);
    inner.update(data);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(&opad);
    outer.update(&inner_digest);
    outer.finalize()
}

/// Constant-time 32-byte digest comparison (immune to timing side channels).
#[inline]
pub fn ct_eq_32(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

pub fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0F) as usize] as char);
    }
    out
}

/// 34-byte multihash content address: `0x12 0x20 || SHA-256(frame)`.
pub fn content_address(frame: &[u8]) -> [u8; 34] {
    let mut out = [0u8; 34];
    out[0] = 0x12;
    out[1] = 0x20;
    out[2..].copy_from_slice(&sha256(frame));
    out
}

/// RFC 6962 domain-separated leaf hash: `SHA-256(0x00 || chunk)`.
pub fn merkle_leaf(chunk: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(&[0x00]);
    h.update(chunk);
    h.finalize()
}

/// Domain-separated Plane C tensor Merkle leaf: `SHA-256(0x03 || uvarint(byte_len) || merkle_4m_digest)`.
pub fn tensor_merkle_leaf(byte_len: u64, merkle_4m_digest: &[u8; 32]) -> [u8; 32] {
    let mut buf = Vec::with_capacity(44);
    buf.push(0x03);
    encode_uvarint(byte_len, &mut buf);
    buf.extend_from_slice(merkle_4m_digest);
    sha256(&buf)
}

fn reduce_tree_digest(mut level: Vec<[u8; 32]>) -> [u8; 32] {
    if level.is_empty() {
        return sha256(&[]);
    }
    while level.len() > 1 {
        let mut nxt = Vec::with_capacity((level.len() + 1) / 2);
        let mut i = 0;
        while i < level.len() {
            if i + 1 < level.len() {
                let mut h = Sha256::new();
                h.update(&[0x01]);
                h.update(&level[i]);
                h.update(&level[i + 1]);
                nxt.push(h.finalize());
            } else {
                nxt.push(level[i]); // promote-odd (CVE-2012-2459 safe)
            }
            i += 2;
        }
        level = nxt;
    }
    level[0]
}

fn reduce_merkle_leaves(leaves: Vec<[u8; 32]>) -> [u8; 32] {
    let n = leaves.len();
    let tree_digest = reduce_tree_digest(leaves);
    let mut root_in = Vec::with_capacity(48);
    root_in.push(0x02);
    encode_uvarint(n as u64, &mut root_in);
    root_in.extend_from_slice(&tree_digest);
    sha256(&root_in)
}

/// RFC 6962 domain-separated, promote-odd (CVE-2012-2459 safe), count-bound Merkle root over frames.
pub fn merkle_root_rfc6962(frames: &[&[u8]]) -> [u8; 34] {
    let leaves: Vec<[u8; 32]> = frames.iter().map(|f| merkle_leaf(f)).collect();
    let root = reduce_merkle_leaves(leaves);
    let mut out = [0u8; 34];
    out[0] = 0x12;
    out[1] = 0x20;
    out[2..].copy_from_slice(&root);
    out
}

/// Unified `CWRF` shard Merkle root committing to BOTH Plane A frames and Plane C tensor arenas.
/// - When `tensor_leaves.is_empty()`, matches `merkle_root_rfc6962` (`0x02` domain) byte-for-byte.
/// - When `!tensor_leaves.is_empty()`, binds both trees under domain `0x04`:
///   `0x12 0x20 || SHA-256(0x04 || uvarint(F) || frame_tree || uvarint(T) || tensor_tree)`.
pub fn shard_merkle_root_from_leaves(
    frame_leaves: &[[u8; 32]],
    tensor_leaves: &[[u8; 32]],
) -> [u8; 34] {
    let root = if tensor_leaves.is_empty() {
        reduce_merkle_leaves(frame_leaves.to_vec())
    } else {
        let f_tree = reduce_tree_digest(frame_leaves.to_vec());
        let t_tree = reduce_tree_digest(tensor_leaves.to_vec());
        let mut root_in = Vec::with_capacity(96);
        root_in.push(0x04);
        encode_uvarint(frame_leaves.len() as u64, &mut root_in);
        root_in.extend_from_slice(&f_tree);
        encode_uvarint(tensor_leaves.len() as u64, &mut root_in);
        root_in.extend_from_slice(&t_tree);
        sha256(&root_in)
    };
    let mut out = [0u8; 34];
    out[0] = 0x12;
    out[1] = 0x20;
    out[2..].copy_from_slice(&root);
    out
}

// =========================================================================
// BOUNDED REUSABLE WORKER POOL (ZERO PER-CALL OS THREAD SPAWN STORM)
// =========================================================================

#[cfg(not(target_arch = "wasm32"))]
struct ScopedJob {
    ctx: *const (),
    call: unsafe fn(*const (), usize),
    count: usize,
    next: AtomicUsize,
    active_workers: AtomicUsize,
    panicked: AtomicBool,
    done_mu: Mutex<bool>,
    done_cv: Condvar,
}

#[cfg(not(target_arch = "wasm32"))]
unsafe impl Send for ScopedJob {}
#[cfg(not(target_arch = "wasm32"))]
unsafe impl Sync for ScopedJob {}

#[cfg(not(target_arch = "wasm32"))]
impl ScopedJob {
    fn drain(&self) {
        loop {
            let idx = self.next.fetch_add(1, Ordering::Relaxed);
            if idx >= self.count {
                break;
            }
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
                (self.call)(self.ctx, idx);
            }));
            if res.is_err() {
                self.panicked.store(true, Ordering::Release);
            }
        }
    }

    fn worker_finish(&self) {
        if self.active_workers.fetch_sub(1, Ordering::AcqRel) == 1 {
            let mut done = self.done_mu.lock().unwrap();
            *done = true;
            self.done_cv.notify_one();
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct WorkerPool {
    queue: Mutex<Vec<usize>>, // raw *const ScopedJob addresses
    cv: Condvar,
    n_workers: usize,
}

#[cfg(not(target_arch = "wasm32"))]
static POOL_INNER: OnceLock<WorkerPool> = OnceLock::new();

#[cfg(not(target_arch = "wasm32"))]
fn global_pool() -> &'static WorkerPool {
    static POOL: OnceLock<&'static WorkerPool> = OnceLock::new();
    *POOL.get_or_init(|| {
        let n_workers = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4)
            .clamp(1, MAX_POOL_WORKERS);
        let pool_ref = POOL_INNER.get_or_init(|| WorkerPool {
            queue: Mutex::new(Vec::with_capacity(64)),
            cv: Condvar::new(),
            n_workers,
        });
        for tid in 0..n_workers {
            let _ = std::thread::Builder::new()
                .name(format!("cowrie-w{}", tid))
                .spawn(move || loop {
                    let job_addr = {
                        let mut q = pool_ref.queue.lock().unwrap();
                        while q.is_empty() {
                            q = pool_ref.cv.wait(q).unwrap();
                        }
                        q.pop().unwrap()
                    };
                    let job = unsafe { &*(job_addr as *const ScopedJob) };
                    job.drain();
                    job.worker_finish();
                });
        }
        pool_ref
    })
}

/// Execute `0..count` across the bounded persistent worker pool + calling thread (or sequentially on `wasm32`).
pub fn parallel_for<F>(count: usize, f: F)
where
    F: Fn(usize) + Sync,
{
    #[cfg(target_arch = "wasm32")]
    {
        for i in 0..count {
            f(i);
        }
        return;
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        if count == 0 {
            return;
        }
        if count == 1 {
            f(0);
            return;
        }
        unsafe fn trampoline<F: Fn(usize) + Sync>(ctx: *const (), idx: usize) {
            let func = &*(ctx as *const F);
            func(idx);
        }

        let pool = global_pool();
        let tickets = (count - 1).min(pool.n_workers);
        if tickets == 0 {
            for i in 0..count {
                f(i);
            }
            return;
        }

        let job = ScopedJob {
            ctx: (&f as *const F) as *const (),
            call: trampoline::<F>,
            count,
            next: AtomicUsize::new(0),
            active_workers: AtomicUsize::new(tickets),
            panicked: AtomicBool::new(false),
            done_mu: Mutex::new(false),
            done_cv: Condvar::new(),
        };
        let job_addr = (&job as *const ScopedJob) as usize;

        {
            let mut q = pool.queue.lock().unwrap();
            for _ in 0..tickets {
                q.push(job_addr);
            }
        }
        if tickets == 1 {
            pool.cv.notify_one();
        } else {
            pool.cv.notify_all();
        }

        job.drain();

        let mut done = job.done_mu.lock().unwrap();
        while !*done {
            done = job.done_cv.wait(done).unwrap();
        }
        drop(done);

        if job.panicked.load(Ordering::Acquire) {
            panic!("cowrie worker panicked inside parallel_for");
        }
    }
}

/// Parallel 4 MiB chunked RFC 6962 Merkle SHA-256 digest for large `StatePage` & tensor payloads.
/// 100% deterministic regardless of core count; uses bounded reusable `WorkerPool`.
pub fn sha256_parallel_merkle(data: &[u8]) -> [u8; 32] {
    if data.is_empty() {
        return reduce_merkle_leaves(Vec::new());
    }
    let n = (data.len() + STATEPAGE_CHUNK_BYTES - 1) / STATEPAGE_CHUNK_BYTES;
    if n <= 1 {
        return reduce_merkle_leaves(vec![merkle_leaf(data)]);
    }

    let mut leaves = vec![[0u8; 32]; n];
    let leaves_ptr = leaves.as_mut_ptr() as usize;
    parallel_for(n, |idx| {
        let start = idx * STATEPAGE_CHUNK_BYTES;
        let end = (start + STATEPAGE_CHUNK_BYTES).min(data.len());
        let digest = merkle_leaf(&data[start..end]);
        unsafe {
            let dst = (leaves_ptr as *mut [u8; 32]).add(idx);
            *dst = digest;
        }
    });
    reduce_merkle_leaves(leaves)
}

pub fn sha256_parallel_merkle_hex(data: &[u8]) -> String {
    to_hex(&sha256_parallel_merkle(data))
}
