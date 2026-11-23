//! Audited SHA-256 with runtime x86_64 SHA-NI hardware acceleration,
//! 34-byte multihash CIDs, RFC 6962 count-bound Merkle roots, and
//! deterministic multi-threaded 4 MB chunked Merkle hashing for large StatePages.

use crate::canonical::encode_uvarint;

pub const STATEPAGE_CHUNK_BYTES: usize = 4 * 1024 * 1024; // 4 MiB chunks for parallel SHA-NI

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

    // Pack h[0..8] = [A, B, C, D, E, F, G, H] into:
    //   state0 = [A, B, E, F] (MSB -> LSB)
    //   state1 = [C, D, G, H] (MSB -> LSB)
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

        // Rounds 0..4
        let mut msg = _mm_add_epi32(msg0, kvec!(0));
        state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
        msg = _mm_shuffle_epi32(msg, 0x0E);
        state0 = _mm_sha256rnds2_epu32(state0, state1, msg);

        // Rounds 4..8 + schedule 16..20
        msg = _mm_add_epi32(msg1, kvec!(4));
        state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
        msg = _mm_shuffle_epi32(msg, 0x0E);
        state0 = _mm_sha256rnds2_epu32(state0, state1, msg);
        msg0 = _mm_sha256msg1_epu32(msg0, msg1);

        // Rounds 8..12
        msg = _mm_add_epi32(msg2, kvec!(8));
        state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
        msg = _mm_shuffle_epi32(msg, 0x0E);
        state0 = _mm_sha256rnds2_epu32(state0, state1, msg);
        msg1 = _mm_sha256msg1_epu32(msg1, msg2);

        // Rounds 12..16
        msg = _mm_add_epi32(msg3, kvec!(12));
        state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
        let mut tmp = _mm_alignr_epi8(msg3, msg2, 4);
        msg0 = _mm_add_epi32(msg0, tmp);
        msg0 = _mm_sha256msg2_epu32(msg0, msg3);
        msg = _mm_shuffle_epi32(msg, 0x0E);
        state0 = _mm_sha256rnds2_epu32(state0, state1, msg);
        msg2 = _mm_sha256msg1_epu32(msg2, msg3);

        // Macro for pipelined rounds 16..52
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

        // Rounds 16..52
        round4_sched!(16, msg0, msg3, msg2, msg1);
        round4_sched!(20, msg1, msg0, msg3, msg2);
        round4_sched!(24, msg2, msg1, msg0, msg3);
        round4_sched!(28, msg3, msg2, msg1, msg0);
        round4_sched!(32, msg0, msg3, msg2, msg1);
        round4_sched!(36, msg1, msg0, msg3, msg2);
        round4_sched!(40, msg2, msg1, msg0, msg3);
        round4_sched!(44, msg3, msg2, msg1, msg0);
        round4_sched!(48, msg0, msg3, msg2, msg1);

        // Rounds 52..56 (last msg2 update)
        msg = _mm_add_epi32(msg1, kvec!(52));
        state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
        tmp = _mm_alignr_epi8(msg1, msg0, 4);
        msg2 = _mm_add_epi32(msg2, tmp);
        msg2 = _mm_sha256msg2_epu32(msg2, msg1);
        msg = _mm_shuffle_epi32(msg, 0x0E);
        state0 = _mm_sha256rnds2_epu32(state0, state1, msg);

        // Rounds 56..60 (last msg3 update)
        msg = _mm_add_epi32(msg2, kvec!(56));
        state1 = _mm_sha256rnds2_epu32(state1, state0, msg);
        tmp = _mm_alignr_epi8(msg2, msg1, 4);
        msg3 = _mm_add_epi32(msg3, tmp);
        msg3 = _mm_sha256msg2_epu32(msg3, msg2);
        msg = _mm_shuffle_epi32(msg, 0x0E);
        state0 = _mm_sha256rnds2_epu32(state0, state1, msg);

        // Rounds 60..64
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
    // s0 = [F, E, B, A], s1 = [H, G, D, C]
    h[0] = s0[3];
    h[1] = s0[2];
    h[2] = s1[3];
    h[3] = s1[2];
    h[4] = s0[1];
    h[5] = s0[0];
    h[6] = s1[1];
    h[7] = s1[0];
}

#[inline]
pub fn has_shani() -> bool {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        std::is_x86_feature_detected!("sha")
            && std::is_x86_feature_detected!("ssse3")
            && std::is_x86_feature_detected!("sse4.1")
    }
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    {
        false
    }
}

/// Streaming SHA-256 hasher with automatic x86_64 SHA-NI hardware acceleration.
#[derive(Clone)]
pub struct Sha256 {
    h: [u32; 8],
    buf: [u8; 64],
    buf_len: usize,
    total_len: u64,
    use_shani: bool,
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
            use_shani: has_shani(),
        }
    }

    /// Force pure scalar mode (used for cross-verification unit tests).
    pub fn new_scalar() -> Self {
        let mut s = Self::new();
        s.use_shani = false;
        s
    }

    #[inline]
    fn compress_slice(&mut self, full_blocks: &[u8]) {
        if full_blocks.is_empty() {
            return;
        }
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if self.use_shani {
            unsafe { compress_blocks_shani(&mut self.h, full_blocks) };
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

fn reduce_merkle_leaves(mut level: Vec<[u8; 32]>) -> [u8; 32] {
    let n = level.len();
    let tree_digest = if n == 0 {
        sha256(&[])
    } else {
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
                    nxt.push(level[i]); // promote-odd
                }
                i += 2;
            }
            level = nxt;
        }
        level[0]
    };
    let mut root_in = Vec::with_capacity(48);
    root_in.push(0x02);
    encode_uvarint(n as u64, &mut root_in);
    root_in.extend_from_slice(&tree_digest);
    sha256(&root_in)
}

/// RFC 6962 domain-separated, promote-odd (CVE-2012-2459 safe), count-bound Merkle root.
pub fn merkle_root_rfc6962(frames: &[&[u8]]) -> [u8; 34] {
    let leaves: Vec<[u8; 32]> = frames.iter().map(|f| merkle_leaf(f)).collect();
    let root = reduce_merkle_leaves(leaves);
    let mut out = [0u8; 34];
    out[0] = 0x12;
    out[1] = 0x20;
    out[2..].copy_from_slice(&root);
    out
}

/// Parallel 4 MiB chunked RFC 6962 Merkle SHA-256 digest for large `StatePage` payloads.
/// 100% deterministic regardless of core count; saturates multi-core SHA-NI (>10 GB/s).
pub fn sha256_parallel_merkle(data: &[u8]) -> [u8; 32] {
    if data.is_empty() {
        return reduce_merkle_leaves(Vec::new());
    }
    let chunks: Vec<&[u8]> = data.chunks(STATEPAGE_CHUNK_BYTES).collect();
    let n = chunks.len();
    if n <= 1 {
        return reduce_merkle_leaves(vec![merkle_leaf(chunks[0])]);
    }
    let nthreads = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
        .min(n);

    let mut leaves = vec![[0u8; 32]; n];
    let per_thread = (n + nthreads - 1) / nthreads;
    std::thread::scope(|s| {
        let mut rem_leaves: &mut [[u8; 32]] = &mut leaves;
        let mut start = 0usize;
        while start < n {
            let take = per_thread.min(n - start);
            let (out_slice, rest) = rem_leaves.split_at_mut(take);
            rem_leaves = rest;
            let in_slice = &chunks[start..start + take];
            s.spawn(move || {
                for (dst, &ch) in out_slice.iter_mut().zip(in_slice.iter()) {
                    *dst = merkle_leaf(ch);
                }
            });
            start += take;
        }
    });
    reduce_merkle_leaves(leaves)
}

pub fn sha256_parallel_merkle_hex(data: &[u8]) -> String {
    to_hex(&sha256_parallel_merkle(data))
}
