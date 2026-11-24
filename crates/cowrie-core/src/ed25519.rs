//! Pure-Rust, zero-dependency RFC 8032 `Ed25519` signatures + FIPS 180-4 `SHA-512`
//! for asymmetric owner declassification tokens (`COWRIE_DECLASSIFY_V1`) over causal taint DAGs.

use crate::sha256::ct_eq_32;
use crate::trust::ToolAction;
use crate::CowrieError;

const K512: [u64; 80] = [
    0x428a2f98d728ae22, 0x7137449123ef65cd, 0xb5c0fbcfec4d3b2f, 0xe9b5dba58189dbbc,
    0x3956c25bf348b538, 0x59f111f1b605d019, 0x923f82a4af194f9b, 0xab1c5ed5da6d8118,
    0xd807aa98a3030242, 0x12835b0145706fbe, 0x243185be4ee4b28c, 0x550c7dc3d5ffb4e2,
    0x72be5d74f27b896f, 0x80deb1fe3b1696b1, 0x9bdc06a725c71235, 0xc19bf174cf692694,
    0xe49b69c19ef14ad2, 0xefbe4786384f25e3, 0x0fc19dc68b8cd5b5, 0x240ca1cc77ac9c65,
    0x2de92c6f592b0275, 0x4a7484aa6ea6e483, 0x5cb0a9dcbd41fbd4, 0x76f988da831153b5,
    0x983e5152ee66dfab, 0xa831c66d2db43210, 0xb00327c898fb213f, 0xbf597fc7beef0ee4,
    0xc6e00bf33da88fc2, 0xd5a79147930aa725, 0x06ca6351e003826f, 0x142929670a0e6e70,
    0x27b70a8546d22ffc, 0x2e1b21385c26c926, 0x4d2c6dfc5ac42aed, 0x53380d139d95b3df,
    0x650a73548baf63de, 0x766a0abb3c77b2a8, 0x81c2c92e47edaee6, 0x92722c851482353b,
    0xa2bfe8a14cf10364, 0xa81a664bbc423001, 0xc24b8b70d0f89791, 0xc76c51a30654be30,
    0xd192e819d6ef5218, 0xd69906245565a910, 0xf40e35855771202a, 0x106aa07032bbd1b8,
    0x19a4c116b8d2d0c8, 0x1e376c085141ab53, 0x2748774cdf8eeb99, 0x34b0bcb5e19b48a8,
    0x391c0cb3c5c95a63, 0x4ed8aa4ae3418acb, 0x5b9cca4f7763e373, 0x682e6ff3d6b2b8a3,
    0x748f82ee5defb2fc, 0x78a5636f43172f60, 0x84c87814a1f0ab72, 0x8cc702081a6439ec,
    0x90befffa23631e28, 0xa4506cebde82bde9, 0xbef9a3f7b2c67915, 0xc67178f2e372532b,
    0xca273eceea26619c, 0xd186b8c721c0c207, 0xeada7dd6cde0eb1e, 0xf57d4f7fee6ed178,
    0x06f067aa72176fba, 0x0a637dc5a2c898a6, 0x113f9804bef90dae, 0x1b710b35131c471b,
    0x28db77f523047d84, 0x32caab7b40c72493, 0x3c9ebe0a15c9bebc, 0x431d67c49c100d4c,
    0x4cc5d4becb3e42b6, 0x597f299cfc657e2a, 0x5fcb6fab3ad6faec, 0x6c44198c4a475817,
];

#[derive(Clone)]
pub struct Sha512 {
    h: [u64; 8],
    buf: [u8; 128],
    buf_len: usize,
    total_len: u128,
}

impl Default for Sha512 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha512 {
    pub fn new() -> Self {
        Self {
            h: [
                0x6a09e667f3bcc908, 0xbb67ae8584caa73b, 0x3c6ef372fe94f82b, 0xa54ff53a5f1d36f1,
                0x510e527fade682d1, 0x9b05688c2b3e6c1f, 0x1f83d9abfb41bd6b, 0x5be0cd19137e2179,
            ],
            buf: [0u8; 128],
            buf_len: 0,
            total_len: 0,
        }
    }

    fn compress_block(&mut self, block: &[u8; 128]) {
        let mut w = [0u64; 80];
        for i in 0..16 {
            w[i] = u64::from_be_bytes(block[8 * i..8 * (i + 1)].try_into().unwrap());
        }
        for i in 16..80 {
            let s0 = w[i - 15].rotate_right(1) ^ w[i - 15].rotate_right(8) ^ (w[i - 15] >> 7);
            let s1 = w[i - 2].rotate_right(19) ^ w[i - 2].rotate_right(61) ^ (w[i - 2] >> 6);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = self.h;
        for i in 0..80 {
            let s1 = e.rotate_right(14) ^ e.rotate_right(18) ^ e.rotate_right(41);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh.wrapping_add(s1).wrapping_add(ch).wrapping_add(K512[i]).wrapping_add(w[i]);
            let s0 = a.rotate_right(28) ^ a.rotate_right(34) ^ a.rotate_right(39);
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
        self.h[0] = self.h[0].wrapping_add(a);
        self.h[1] = self.h[1].wrapping_add(b);
        self.h[2] = self.h[2].wrapping_add(c);
        self.h[3] = self.h[3].wrapping_add(d);
        self.h[4] = self.h[4].wrapping_add(e);
        self.h[5] = self.h[5].wrapping_add(f);
        self.h[6] = self.h[6].wrapping_add(g);
        self.h[7] = self.h[7].wrapping_add(hh);
    }

    pub fn update(&mut self, mut data: &[u8]) {
        self.total_len += data.len() as u128;
        if self.buf_len > 0 {
            let need = 128 - self.buf_len;
            if data.len() < need {
                self.buf[self.buf_len..self.buf_len + data.len()].copy_from_slice(data);
                self.buf_len += data.len();
                return;
            }
            self.buf[self.buf_len..128].copy_from_slice(&data[..need]);
            let blk = self.buf;
            self.compress_block(&blk);
            self.buf_len = 0;
            data = &data[need..];
        }
        while data.len() >= 128 {
            let blk: &[u8; 128] = data[..128].try_into().unwrap();
            self.compress_block(blk);
            data = &data[128..];
        }
        if !data.is_empty() {
            self.buf[..data.len()].copy_from_slice(data);
            self.buf_len = data.len();
        }
    }

    pub fn finalize(mut self) -> [u8; 64] {
        let bit_len = self.total_len * 8;
        self.buf[self.buf_len] = 0x80;
        self.buf_len += 1;
        if self.buf_len > 112 {
            for b in &mut self.buf[self.buf_len..128] {
                *b = 0;
            }
            let blk = self.buf;
            self.compress_block(&blk);
            self.buf_len = 0;
        }
        for b in &mut self.buf[self.buf_len..112] {
            *b = 0;
        }
        self.buf[112..128].copy_from_slice(&bit_len.to_be_bytes());
        let blk = self.buf;
        self.compress_block(&blk);

        let mut out = [0u8; 64];
        for (i, w) in self.h.iter().enumerate() {
            out[8 * i..8 * (i + 1)].copy_from_slice(&w.to_be_bytes());
        }
        out
    }
}

pub fn sha512(data: &[u8]) -> [u8; 64] {
    let mut h = Sha512::new();
    h.update(data);
    h.finalize()
}

// =========================================================================
// GF(2^255 - 19) & TWISTED EDWARDS ARITHMETIC (CONSTANT-TIME RADIX 2^16)
// =========================================================================

type Fe = [i64; 16];

const FE_0: Fe = [0; 16];
const FE_1: Fe = [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

// d = -121665 / 121666 mod p
const FE_D: Fe = [
    0x78a3, 0x1359, 0x4dca, 0x75eb, 0xd8ab, 0x4141, 0x0a4d, 0x0070,
    0xe898, 0x7779, 0x4079, 0x8cc7, 0xfe73, 0x2b6f, 0x6cee, 0x5203,
];
// 2 * d mod p
const FE_D2: Fe = [
    0xf159, 0x26b2, 0x9b94, 0xebd6, 0xb156, 0x8283, 0x149a, 0x00e0,
    0xd130, 0xeef3, 0x80f2, 0x198e, 0xfce7, 0x56df, 0xd9dc, 0x2406,
];
// sqrt(-1) = 2^((p-1)/4) mod p
const FE_SQRT_M1: Fe = [
    0xa0b0, 0x4a0e, 0x1b27, 0xc4ee, 0xe478, 0xad2f, 0x1806, 0x2f43,
    0xd7a7, 0x3dfb, 0x0099, 0x2b4d, 0xdf0b, 0x4fc1, 0x2480, 0x2b83,
];
// Base point X, Y
const FE_BX: Fe = [
    0xd51a, 0x8f25, 0x2d60, 0xc956, 0xa7b2, 0x9525, 0xc760, 0x692c,
    0xdc5c, 0xfdd6, 0xe231, 0xc0a4, 0x53fe, 0xcd6e, 0x36d3, 0x2169,
];
const FE_BY: Fe = [
    0x6658, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666,
    0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666,
];

// Group order L = 2^252 + 27742317777372353535851937790883648493 (little-endian bytes)
const ORDER_L: [i64; 32] = [
    0xed, 0xd3, 0xf5, 0x5c, 0x1a, 0x63, 0x12, 0x58,
    0xd6, 0x9c, 0xf7, 0xa2, 0xde, 0xf9, 0xde, 0x14,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
];

#[inline]
fn car25519(o: &mut Fe) {
    for i in 0..16 {
        o[i] += 1 << 16;
        let c = o[i] >> 16;
        if i < 15 {
            o[i + 1] += c - 1;
        } else {
            o[0] += 38 * (c - 1);
        }
        o[i] -= c << 16;
    }
}

#[inline]
fn fadd(a: &Fe, b: &Fe) -> Fe {
    let mut o = FE_0;
    for i in 0..16 {
        o[i] = a[i] + b[i];
    }
    o
}

#[inline]
fn fsub(a: &Fe, b: &Fe) -> Fe {
    let mut o = FE_0;
    for i in 0..16 {
        o[i] = a[i] - b[i];
    }
    o
}

fn fmul(a: &Fe, b: &Fe) -> Fe {
    let mut t = [0i64; 31];
    for i in 0..16 {
        for j in 0..16 {
            t[i + j] += a[i] * b[j];
        }
    }
    for i in 0..15 {
        t[i] += 38 * t[i + 16];
    }
    let mut o = FE_0;
    o.copy_from_slice(&t[..16]);
    car25519(&mut o);
    car25519(&mut o);
    o
}

#[inline]
fn fsq(a: &Fe) -> Fe {
    fmul(a, a)
}

fn finv(i: &Fe) -> Fe {
    let mut c = *i;
    for a in (0..=253).rev() {
        c = fsq(&c);
        if a != 2 && a != 4 {
            c = fmul(&c, i);
        }
    }
    c
}

fn pow2523(i: &Fe) -> Fe {
    let mut c = *i;
    for a in (0..=250).rev() {
        c = fsq(&c);
        if a != 1 {
            c = fmul(&c, i);
        }
    }
    c
}

fn fpack(n: &Fe) -> [u8; 32] {
    let mut t = *n;
    car25519(&mut t);
    car25519(&mut t);
    car25519(&mut t);
    let mut m = FE_0;
    for _ in 0..2 {
        m[0] = t[0] - 0xffed;
        for i in 1..15 {
            m[i] = t[i] - 0xffff - ((m[i - 1] >> 16) & 1);
            m[i - 1] &= 0xffff;
        }
        m[15] = t[15] - 0x7fff - ((m[14] >> 16) & 1);
        let b = (m[15] >> 16) & 1;
        m[14] &= 0xffff;
        let mask = !(b - 1); // 0 if b==0 (t >= p, pick m), -1 if b==1 (t < p, keep t)
        for i in 0..16 {
            let x = mask & (t[i] ^ m[i]);
            t[i] = m[i] ^ x;
        }
    }
    let mut out = [0u8; 32];
    for i in 0..16 {
        out[2 * i] = (t[i] & 0xff) as u8;
        out[2 * i + 1] = (t[i] >> 8) as u8;
    }
    out
}

fn funpack_strict(b: &[u8; 32]) -> Option<Fe> {
    let mut o = FE_0;
    for i in 0..16 {
        o[i] = (b[2 * i] as i64) + ((b[2 * i + 1] as i64) << 8);
    }
    o[15] &= 0x7fff;
    // RFC 8032 strict canonical y < p check: re-packing must match b with cleared top bit
    let mut b_masked = *b;
    b_masked[31] &= 0x7f;
    if fpack(&o) != b_masked {
        return None;
    }
    Some(o)
}

#[derive(Clone, Copy)]
struct GeP3 {
    x: Fe,
    y: Fe,
    z: Fe,
    t: Fe,
}

impl GeP3 {
    fn neutral() -> Self {
        Self { x: FE_0, y: FE_1, z: FE_1, t: FE_0 }
    }

    fn basepoint() -> Self {
        Self {
            x: FE_BX,
            y: FE_BY,
            z: FE_1,
            t: fmul(&FE_BX, &FE_BY),
        }
    }

    fn add(&self, q: &Self) -> Self {
        let a = fmul(&fsub(&self.y, &self.x), &fsub(&q.y, &q.x));
        let b = fmul(&fadd(&self.y, &self.x), &fadd(&q.y, &q.x));
        let c = fmul(&FE_D2, &fmul(&self.t, &q.t));
        let z2 = fmul(&self.z, &q.z);
        let d = fadd(&z2, &z2);
        let e = fsub(&b, &a);
        let f = fsub(&d, &c);
        let g = fadd(&d, &c);
        let h = fadd(&b, &a);
        Self {
            x: fmul(&e, &f),
            y: fmul(&g, &h),
            z: fmul(&f, &g),
            t: fmul(&e, &h),
        }
    }

    fn cswap(a: &mut Self, b: &mut Self, bit: i64) {
        let mask = -bit;
        for i in 0..16 {
            let tx = mask & (a.x[i] ^ b.x[i]); a.x[i] ^= tx; b.x[i] ^= tx;
            let ty = mask & (a.y[i] ^ b.y[i]); a.y[i] ^= ty; b.y[i] ^= ty;
            let tz = mask & (a.z[i] ^ b.z[i]); a.z[i] ^= tz; b.z[i] ^= tz;
            let tt = mask & (a.t[i] ^ b.t[i]); a.t[i] ^= tt; b.t[i] ^= tt;
        }
    }

    fn scalarmult(&self, s: &[u8; 32]) -> Self {
        let mut r0 = Self::neutral();
        let mut r1 = *self;
        for i in (0..256).rev() {
            let bit = ((s[i >> 3] >> (i & 7)) & 1) as i64;
            Self::cswap(&mut r0, &mut r1, bit);
            r1 = r0.add(&r1);
            r0 = r0.add(&r0);
            Self::cswap(&mut r0, &mut r1, bit);
        }
        r0
    }

    fn to_bytes(&self) -> [u8; 32] {
        let zi = finv(&self.z);
        let tx = fpack(&fmul(&self.x, &zi));
        let mut ty = fpack(&fmul(&self.y, &zi));
        ty[31] ^= (tx[0] & 1) << 7;
        ty
    }

    fn from_bytes_neg_strict(s: &[u8; 32]) -> Option<Self> {
        let y = funpack_strict(s)?;
        let sign = (s[31] >> 7) & 1;
        let y2 = fsq(&y);
        let u = fsub(&y2, &FE_1);
        let v = fadd(&fmul(&FE_D, &y2), &FE_1);
        let v3 = fmul(&v, &fsq(&v));
        let v7 = fmul(&v, &fsq(&v3));
        let uv7 = fmul(&u, &v7);
        let mut x = fmul(&fmul(&u, &v3), &pow2523(&uv7));

        let check = fmul(&v, &fsq(&x));
        if fpack(&check) != fpack(&u) {
            x = fmul(&x, &FE_SQRT_M1);
            let check2 = fmul(&v, &fsq(&x));
            if fpack(&check2) != fpack(&u) {
                return None;
            }
        }
        let xp = fpack(&x);
        if xp == [0u8; 32] && sign == 1 {
            return None; // RFC 8032 §5.1.3: x == 0 with sign == 1 is invalid
        }
        if (xp[0] & 1) != sign {
            x = fsub(&FE_0, &x);
        }
        // Return -A = (-x, y, 1, -x*y) for verification `S*B + k*(-A) == R`
        let neg_x = fsub(&FE_0, &x);
        Some(Self {
            x: neg_x,
            y,
            z: FE_1,
            t: fmul(&neg_x, &y),
        })
    }
}

fn mod_l_wide(wide: &[u8; 64]) -> [u8; 32] {
    const L64: [u64; 4] = [
        0x5812631a5cf5d3ed,
        0x14def9dea2f79cd6,
        0x0000000000000000,
        0x1000000000000000,
    ];
    let mut rem = [0u64; 4];
    for bit_idx in (0..512).rev() {
        let bit = ((wide[bit_idx >> 3] >> (bit_idx & 7)) & 1) as u64;
        rem[3] = (rem[3] << 1) | (rem[2] >> 63);
        rem[2] = (rem[2] << 1) | (rem[1] >> 63);
        rem[1] = (rem[1] << 1) | (rem[0] >> 63);
        rem[0] = (rem[0] << 1) | bit;

        let (d0, b0) = rem[0].overflowing_sub(L64[0]);
        let (t1, b1a) = rem[1].overflowing_sub(L64[1]);
        let (d1, b1b) = t1.overflowing_sub(b0 as u64);
        let b1 = b1a || b1b;
        let (t2, b2a) = rem[2].overflowing_sub(L64[2]);
        let (d2, b2b) = t2.overflowing_sub(b1 as u64);
        let b2 = b2a || b2b;
        let (t3, b3a) = rem[3].overflowing_sub(L64[3]);
        let (d3, b3b) = t3.overflowing_sub(b2 as u64);
        let borrow = b3a || b3b;

        let mask = (borrow as u64).wrapping_sub(1);
        rem[0] ^= mask & (rem[0] ^ d0);
        rem[1] ^= mask & (rem[1] ^ d1);
        rem[2] ^= mask & (rem[2] ^ d2);
        rem[3] ^= mask & (rem[3] ^ d3);
    }
    let mut out = [0u8; 32];
    for (i, w) in rem.iter().enumerate() {
        out[8 * i..8 * (i + 1)].copy_from_slice(&w.to_le_bytes());
    }
    out
}

fn is_canonical_scalar(s: &[u8; 32]) -> bool {
    for j in (0..32).rev() {
        let limb = ORDER_L[j] as u8;
        if s[j] < limb {
            return true;
        }
        if s[j] > limb {
            return false;
        }
    }
    false // s == L is non-canonical
}

/// Derive a 32-byte `Ed25519` public key from a 32-byte secret seed (`RFC 8032` §5.1.5).
pub fn ed25519_public_key(seed: &[u8; 32]) -> [u8; 32] {
    let mut az = sha512(seed);
    az[0] &= 248;
    az[31] = (az[31] & 63) | 64;
    let scalar: [u8; 32] = az[..32].try_into().unwrap();
    GeP3::basepoint().scalarmult(&scalar).to_bytes()
}

/// Sign an arbitrary byte message with a 32-byte `Ed25519` secret seed (`RFC 8032` §5.1.6).
pub fn ed25519_sign(seed: &[u8; 32], msg: &[u8]) -> [u8; 64] {
    let mut az = sha512(seed);
    az[0] &= 248;
    az[31] = (az[31] & 63) | 64;
    let a_scalar: [u8; 32] = az[..32].try_into().unwrap();
    let pk = GeP3::basepoint().scalarmult(&a_scalar).to_bytes();

    let mut hr = Sha512::new();
    hr.update(&az[32..64]);
    hr.update(msg);
    let r_full = hr.finalize();
    let r_scalar = mod_l_wide(&r_full);
    let r_bytes = GeP3::basepoint().scalarmult(&r_scalar).to_bytes();

    let mut hk = Sha512::new();
    hk.update(&r_bytes);
    hk.update(&pk);
    hk.update(msg);
    let k_full = hk.finalize();
    let k_scalar = mod_l_wide(&k_full);

    // S = (r + k * a) mod L
    let mut acc = [0u64; 64];
    for i in 0..32 {
        acc[i] = r_scalar[i] as u64;
    }
    for i in 0..32 {
        for j in 0..32 {
            acc[i + j] += (k_scalar[i] as u64) * (a_scalar[j] as u64);
        }
    }
    let mut s_wide = [0u8; 64];
    let mut carry = 0u64;
    for i in 0..64 {
        let v = acc[i] + carry;
        s_wide[i] = (v & 0xff) as u8;
        carry = v >> 8;
    }
    debug_assert_eq!(carry, 0);
    let s_scalar = mod_l_wide(&s_wide);

    let mut sig = [0u8; 64];
    sig[..32].copy_from_slice(&r_bytes);
    sig[32..].copy_from_slice(&s_scalar);
    sig
}

/// Strictly verify an `Ed25519` signature (`RFC 8032` §5.1.7, rejecting non-canonical `S >= L` and `y >= p`).
pub fn ed25519_verify_strict(pubkey: &[u8; 32], msg: &[u8], sig: &[u8; 64]) -> bool {
    let r_bytes: &[u8; 32] = sig[..32].try_into().unwrap();
    let s_bytes: &[u8; 32] = sig[32..64].try_into().unwrap();
    if !is_canonical_scalar(s_bytes) {
        return false;
    }
    let Some(neg_a) = GeP3::from_bytes_neg_strict(pubkey) else {
        return false;
    }
;
    let mut hk = Sha512::new();
    hk.update(r_bytes);
    hk.update(pubkey);
    hk.update(msg);
    let k_full = hk.finalize();
    let k_scalar = mod_l_wide(&k_full);

    let sb = GeP3::basepoint().scalarmult(s_bytes);
    let ka = neg_a.scalarmult(&k_scalar);
    let check_r = sb.add(&ka).to_bytes();
    ct_eq_32(&check_r, r_bytes)
}

// =========================================================================
// COWRIE DECLASSIFICATION TOKENS (`COWRIE_DECLASSIFY_V1`)
// =========================================================================

pub const DECLASSIFY_DOMAIN: &[u8] = b"COWRIE_DECLASSIFY_V1\0";

#[derive(Clone, Copy, Debug)]
pub struct DeclassificationToken {
    pub target_cid: [u8; 34],
    pub action: ToolAction,
    pub expiry_unix_secs: u64,
    pub signature: [u8; 64],
}

pub fn action_to_code(action: ToolAction) -> u8 {
    match action {
        ToolAction::ReadOnlySafe => 0,
        ToolAction::ShellExec => 1,
        ToolAction::CredRead => 2,
        ToolAction::FsWrite => 3,
        ToolAction::NetExfil => 4,
    }
}

pub fn action_from_code(code: u8) -> Option<ToolAction> {
    match code {
        0 => Some(ToolAction::ReadOnlySafe),
        1 => Some(ToolAction::ShellExec),
        2 => Some(ToolAction::CredRead),
        3 => Some(ToolAction::FsWrite),
        4 => Some(ToolAction::NetExfil),
        _ => None,
    }
}

fn build_declassify_msg(target_cid: &[u8; 34], action: ToolAction, expiry_unix_secs: u64) -> [u8; 64] {
    let mut msg = [0u8; 64];
    let dlen = DECLASSIFY_DOMAIN.len(); // 21
    msg[..dlen].copy_from_slice(DECLASSIFY_DOMAIN);
    msg[dlen..dlen + 34].copy_from_slice(target_cid);
    msg[dlen + 34] = action_to_code(action);
    msg[dlen + 35..dlen + 43].copy_from_slice(&expiry_unix_secs.to_le_bytes());
    msg
}

impl DeclassificationToken {
    pub fn sign(
        owner_seed: &[u8; 32],
        target_cid: [u8; 34],
        action: ToolAction,
        expiry_unix_secs: u64,
    ) -> Self {
        let msg = build_declassify_msg(&target_cid, action, expiry_unix_secs);
        let signature = ed25519_sign(owner_seed, &msg);
        Self {
            target_cid,
            action,
            expiry_unix_secs,
            signature,
        }
    }

    pub fn verify(
        &self,
        owner_pubkey: &[u8; 32],
        expected_action: ToolAction,
        now_unix_secs: u64,
    ) -> Result<(), CowrieError> {
        if self.action != expected_action || now_unix_secs > self.expiry_unix_secs {
            return Err(CowrieError::AuthMacMismatch);
        }
        let msg = build_declassify_msg(&self.target_cid, self.action, self.expiry_unix_secs);
        if !ed25519_verify_strict(owner_pubkey, &msg, &self.signature) {
            return Err(CowrieError::AuthMacMismatch);
        }
        Ok(())
    }
}
