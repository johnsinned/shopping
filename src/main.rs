mod parquet;

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::*;
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use parquet::{
    CampaignDailyRow, UserDailyRow, write_campaign_daily_parquet, write_user_daily_parquet,
};

const DAY_MS: i64 = 86_400_000;
const SESSION_GAP_MS: u32 = 1_800_000;
const CLICK_REACH_MS: u32 = 7 * 86_400_000;
const NUM_SHARDS: usize = 256;
const SHARD_SHIFT: u32 = 8; // log2(NUM_SHARDS)
const CHUNK_SIZE: usize = 2 * 1024 * 1024;
const INVALID_PAYLOAD: u64 = u64::MAX;

unsafe extern "C" {
    fn mmap(
        addr: *mut std::ffi::c_void,
        len: usize,
        prot: i32,
        flags: i32,
        fd: i32,
        offset: i64,
    ) -> *mut std::ffi::c_void;
    fn memchr(s: *const std::ffi::c_void, c: i32, n: usize) -> *const std::ffi::c_void;
}

const PROT_READ: i32 = 1;
const MAP_PRIVATE: i32 = 2;
const MAP_FAILED: *mut std::ffi::c_void = !0usize as *mut std::ffi::c_void;

#[inline(always)]
fn find_nl(haystack: &[u8]) -> Option<usize> {
    if haystack.is_empty() {
        return None;
    }
    let p = unsafe {
        memchr(
            haystack.as_ptr() as *const std::ffi::c_void,
            b'\n' as i32,
            haystack.len(),
        )
    };
    if p.is_null() {
        None
    } else {
        Some((p as usize) - (haystack.as_ptr() as usize))
    }
}

struct MmapFile {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for MmapFile {}
unsafe impl Sync for MmapFile {}

impl MmapFile {
    fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        if len == 0 {
            return Ok(Self {
                ptr: std::ptr::null_mut(),
                len: 0,
            });
        }
        let ptr = unsafe {
            mmap(
                std::ptr::null_mut(),
                len,
                PROT_READ,
                MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            ptr: ptr as *mut u8,
            len,
        })
    }

    #[inline(always)]
    fn as_slice(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

/// Packs a 1..=9 byte ASCII string into the top 63 bits of a u64, preserving
/// exact byte-by-byte lexicographical order.
#[inline(always)]
fn pack7(s: &[u8]) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        let len = s.len().min(9);
        let mut buf = [0u8; 8];
        let copy_len = len.min(8);
        unsafe {
            std::ptr::copy_nonoverlapping(s.as_ptr(), buf.as_mut_ptr(), copy_len);
        }
        let be = u64::from_be_bytes(buf);
        let hi = unsafe { _pext_u64(be, 0x7F7F_7F7F_7F7F_7F7F) } << 7;
        if len == 9 {
            hi | ((unsafe { *s.get_unchecked(8) } as u64) & 0x7F)
        } else {
            hi
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let mut out: u64 = 0;
        let mut shift = 56u32;
        let len = s.len().min(9);
        let mut i = 0;
        while i < len {
            out |= (unsafe { *s.get_unchecked(i) } as u64) << shift;
            shift = shift.wrapping_sub(7);
            i += 1;
        }
        out
    }
}

#[inline(always)]
fn mix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;
    k
}

#[inline(always)]
const fn days_from_civil_const(year: u32, month: u32, day: u32) -> i64 {
    let y = if month <= 2 {
        year.wrapping_sub(1)
    } else {
        year
    };
    let era = y / 400;
    let yoe = y - era * 400;
    let m_adj = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * m_adj + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era as i64) * 146_097 + (doe as i64) - 719_468
}

const FAST_YEAR_BASE: u32 = 2020;
const FAST_YEARS: usize = 12;
const MONTH_TABLE: [[(i64, u32); 13]; FAST_YEARS] = {
    let mut table = [[(0i64, 0u32); 13]; FAST_YEARS];
    let mut yi = 0usize;
    while yi < FAST_YEARS {
        let y = (FAST_YEAR_BASE as usize + yi) as u32;
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        let mut m = 1u32;
        while m <= 12 {
            let max_d = match m {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                2 => {
                    if leap {
                        29
                    } else {
                        28
                    }
                }
                _ => 0,
            };
            let base = days_from_civil_const(y, m, 1) - 1;
            table[yi][m as usize] = (base, max_d);
            m += 1;
        }
        yi += 1;
    }
    table
};

#[inline(always)]
fn parse_8d_swar(mut v: u64) -> Option<u64> {
    v = v.wrapping_sub(0x3030_3030_3030_3030);
    if ((v | v.wrapping_add(0x7676_7676_7676_7676)) & 0x8080_8080_8080_8080) != 0 {
        return None;
    }
    v = (v.wrapping_mul(2561) >> 8) & 0x00FF_00FF_00FF_00FF;
    v = (v.wrapping_mul(6_553_601) >> 16) & 0x0000_FFFF_0000_FFFF;
    Some(v.wrapping_mul(42_949_672_960_001) >> 32)
}

#[inline(always)]
fn parse_2d(s: &[u8], idx: usize) -> Option<u32> {
    let d1 = unsafe { *s.get_unchecked(idx) }.wrapping_sub(b'0');
    let d0 = unsafe { *s.get_unchecked(idx + 1) }.wrapping_sub(b'0');
    if d1 <= 9 && d0 <= 9 {
        Some((d1 as u32) * 10 + (d0 as u32))
    } else {
        None
    }
}

#[inline(always)]
fn parse_4d(s: &[u8], idx: usize) -> Option<u32> {
    let d3 = unsafe { *s.get_unchecked(idx) }.wrapping_sub(b'0');
    let d2 = unsafe { *s.get_unchecked(idx + 1) }.wrapping_sub(b'0');
    let d1 = unsafe { *s.get_unchecked(idx + 2) }.wrapping_sub(b'0');
    let d0 = unsafe { *s.get_unchecked(idx + 3) }.wrapping_sub(b'0');
    if d3 <= 9 && d2 <= 9 && d1 <= 9 && d0 <= 9 {
        Some((d3 as u32) * 1000 + (d2 as u32) * 100 + (d1 as u32) * 10 + (d0 as u32))
    } else {
        None
    }
}

#[inline(always)]
fn parse_civil_19(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = parse_4d(s, 0)?;
    let month = parse_2d(s, 5)?;
    let day = parse_2d(s, 8)?;
    let hour = parse_2d(s, 11)?;
    let min = parse_2d(s, 14)?;
    let sec = parse_2d(s, 17)?;
    if month == 0 || month > 12 || hour > 23 || min > 59 || sec > 59 || day == 0 {
        return None;
    }
    let yi = year.wrapping_sub(FAST_YEAR_BASE) as usize;
    let days = if yi < FAST_YEARS {
        let (base, max_day) =
            unsafe { *MONTH_TABLE.get_unchecked(yi).get_unchecked(month as usize) };
        if day > max_day {
            return None;
        }
        base + (day as i64)
    } else {
        if year == 0 {
            return None;
        }
        let max_day = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                    29
                } else {
                    28
                }
            }
            _ => return None,
        };
        if day > max_day {
            return None;
        }
        days_from_civil_const(year, month, day)
    };
    Some(days * DAY_MS + ((hour * 3600 + min * 60 + sec) as i64) * 1000)
}

#[inline(always)]
fn parse_f1(s: &[u8]) -> Option<i64> {
    if s.len() == 20 && s[19] == b'Z' {
        parse_civil_19(s)
    } else {
        None
    }
}

#[inline(always)]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let len = s.len();
    if len == 0 {
        return None;
    }
    if len == 20 && s[19] == b'Z' {
        return parse_civil_19(s);
    }
    if len == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let local_ms = parse_civil_19(s)?;
        let off_h = parse_2d(s, 20)?;
        let off_m = parse_2d(s, 23)?;
        if off_h > 23 || off_m > 59 {
            return None;
        }
        let off_ms = ((off_h * 60 + off_m) as i64) * 60_000;
        return if s[19] == b'+' {
            Some(local_ms - off_ms)
        } else {
            Some(local_ms + off_ms)
        };
    }
    // F3: ASCII digits 0..=9 only
    let mut val: i64 = 0;
    if len <= 18 {
        for &b in s {
            let d = b.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            val = val * 10 + (d as i64);
        }
        Some(val)
    } else {
        for &b in s {
            let d = b.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            val = val.saturating_mul(10).saturating_add(d as i64);
        }
        Some(val)
    }
}

#[inline(always)]
fn unquote_field(seg: &[u8]) -> Option<&[u8]> {
    let len = seg.len();
    if len == 0 {
        return Some(seg);
    }
    if seg[0] == b'"' {
        if len < 2 || seg[len - 1] != b'"' {
            return None;
        }
        let inner = &seg[1..len - 1];
        for &b in inner {
            if b == b'"' {
                return None;
            }
        }
        Some(inner)
    } else {
        for &b in seg {
            if b == b'"' {
                return None;
            }
        }
        Some(seg)
    }
}

#[inline(always)]
fn parse_csv_line_fallback<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut field_idx = 0;
    let mut start = 0;
    for (i, &b) in line.iter().enumerate() {
        if b == b',' {
            if field_idx + 1 >= N {
                return None;
            }
            out[field_idx] = unquote_field(&line[start..i])?;
            field_idx += 1;
            start = i + 1;
        }
    }
    if field_idx + 1 != N {
        return None;
    }
    out[field_idx] = unquote_field(&line[start..])?;
    Some(out)
}

#[inline(always)]
fn get_bit_96(lo: u64, hi: u32, idx: usize) -> u32 {
    let b_lo = ((lo >> (idx & 63)) & 1) as u32;
    let b_hi = (hi >> (idx & 31)) & 1;
    if idx < 64 { b_lo } else { b_hi }
}

/// Unified branchless AVX2 line + CSV field parser using native u64/u32 bitmasks.
/// Zero branches on line length (<64 vs >=64) and zero branches on quoted vs unquoted fields!
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn try_parse_record_avx2<const N: usize>(
    rem: &[u8],
) -> Option<(usize, Option<[&[u8]; N]>)> {
    if rem.len() < 96 {
        return None;
    }
    unsafe {
        let ptr = rem.as_ptr();
        let v_nl = _mm256_set1_epi8(b'\n' as i8);
        let v_comma = _mm256_set1_epi8(b',' as i8);
        let v_quote = _mm256_set1_epi8(b'"' as i8);

        let v0 = _mm256_loadu_si256(ptr as *const __m256i);
        let v1 = _mm256_loadu_si256(ptr.add(32) as *const __m256i);
        let v2 = _mm256_loadu_si256(ptr.add(64) as *const __m256i);

        let nl0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, v_nl)) as u32 as u64;
        let nl1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, v_nl)) as u32 as u64;
        let nl2 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v2, v_nl)) as u32;
        let nl_lo = nl0 | (nl1 << 32);

        if nl_lo == 0 && nl2 == 0 {
            return None;
        }
        let len = if nl_lo != 0 {
            nl_lo.trailing_zeros() as usize
        } else {
            64 + (nl2.trailing_zeros() as usize)
        };
        if len == 0 {
            return Some((0, None));
        }

        let (lo_mask, hi_mask) = if len < 64 {
            ((1u64 << len) - 1, 0u32)
        } else if len == 64 {
            (!0u64, 0u32)
        } else {
            (!0u64, (1u32 << (len - 64)) - 1)
        };

        let c0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, v_comma)) as u32 as u64;
        let c1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, v_comma)) as u32 as u64;
        let c2 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v2, v_comma)) as u32;
        let mut c_lo = (c0 | (c1 << 32)) & lo_mask;
        let mut c_hi = c2 & hi_mask;

        if c_lo.count_ones() + c_hi.count_ones() != (N - 1) as u32 {
            return Some((len, None));
        }

        let q0 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, v_quote)) as u32 as u64;
        let q1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, v_quote)) as u32 as u64;
        let q2 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v2, v_quote)) as u32;
        let q_lo = (q0 | (q1 << 32)) & lo_mask;
        let q_hi = q2 & hi_mask;

        let total_quotes = q_lo.count_ones() + q_hi.count_ones();
        let mut bad_quotes = 0u32;
        let mut boundary_quotes = 0u32;
        let mut out = [&[][..]; N];
        let mut start = 0usize;
        let mut k = 0usize;

        while k + 1 < N {
            let use_lo = c_lo != 0;
            let end = if use_lo {
                c_lo.trailing_zeros() as usize
            } else {
                64 + (c_hi.trailing_zeros() as usize)
            };
            if use_lo {
                c_lo &= c_lo - 1;
            } else {
                c_hi &= c_hi - 1;
            }
            let flen = end - start;
            let qs = if flen > 0 { get_bit_96(q_lo, q_hi, start) } else { 0 };
            let qe = if flen > 1 { get_bit_96(q_lo, q_hi, end - 1) } else { 0 };
            bad_quotes |= qs ^ qe;
            boundary_quotes += qs + qe;
            let trim = (qs & qe) as usize;
            out[k] = std::slice::from_raw_parts(ptr.add(start + trim), flen - 2 * trim);
            start = end + 1;
            k += 1;
        }
        {
            let flen = len - start;
            let qs = if flen > 0 { get_bit_96(q_lo, q_hi, start) } else { 0 };
            let qe = if flen > 1 { get_bit_96(q_lo, q_hi, len - 1) } else { 0 };
            bad_quotes |= qs ^ qe;
            boundary_quotes += qs + qe;
            let trim = (qs & qe) as usize;
            out[N - 1] = std::slice::from_raw_parts(ptr.add(start + trim), flen - 2 * trim);
        }

        if bad_quotes == 0 && boundary_quotes == total_quotes {
            Some((len, Some(out)))
        } else {
            Some((len, None))
        }
    }
}

#[inline(always)]
fn for_each_csv_record<const N: usize>(chunk: &[u8], mut f: impl FnMut([&[u8]; N])) {
    let mut pos = 0usize;
    let total_len = chunk.len();
    #[cfg(target_arch = "x86_64")]
    {
        while pos + 96 <= total_len {
            if let Some((line_len, maybe_fields)) =
                unsafe { try_parse_record_avx2::<N>(&chunk[pos..]) }
            {
                pos += line_len + 1;
                if let Some(fields) = maybe_fields {
                    f(fields);
                }
            } else {
                let rem = &chunk[pos..];
                let line_len = find_nl(rem).unwrap_or(rem.len());
                let line = &rem[..line_len];
                pos += line_len + 1;
                if let Some(fields) = parse_csv_line_fallback::<N>(line) {
                    f(fields);
                }
            }
        }
    }
    if pos < total_len {
        for line in chunk[pos..].split(|&b| b == b'\n') {
            if let Some(fields) = parse_csv_line_fallback::<N>(line) {
                f(fields);
            }
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct CanonEntry {
    key: u64,
    val: u64,
}

/// Fast interleaved open-addressed hash table with a 2 MB L2/L3-resident Bloom filter
/// that filters out 99.3% of already-canonical user_ids in 1 cache hit!
struct CanonMap {
    bloom: Vec<u64>,
    bloom_mask: usize,
    table: Vec<CanonEntry>,
    mask: usize,
}

impl CanonMap {
    fn build(thread_edges: &[Vec<(u64, u64)>]) -> Self {
        let total_edges: usize = thread_edges.iter().map(|v| v.len()).sum();
        if total_edges == 0 {
            return Self {
                bloom: vec![0; 2],
                bloom_mask: 1,
                table: vec![CanonEntry::default(); 2],
                mask: 1,
            };
        }
        let cap = (total_edges * 2).next_power_of_two().max(64);
        let mask = cap - 1;
        let mut table = vec![CanonEntry::default(); cap];
        let est_nodes = total_edges * 5 / 4 + 64;
        let mut parent: Vec<u32> = Vec::with_capacity(est_nodes);
        let mut min_id: Vec<u64> = Vec::with_capacity(est_nodes);
        let mut node_slots: Vec<u32> = Vec::with_capacity(est_nodes);

        let get_or_insert =
            |u: u64,
             table: &mut [CanonEntry],
             parent: &mut Vec<u32>,
             min_id: &mut Vec<u64>,
             node_slots: &mut Vec<u32>|
             -> u32 {
                let mut idx = (mix64(u) as usize) & mask;
                loop {
                    let entry = unsafe { table.get_unchecked_mut(idx) };
                    if entry.key == u {
                        return entry.val as u32;
                    }
                    if entry.key == 0 {
                        let id = parent.len() as u32;
                        parent.push(id);
                        min_id.push(u);
                        node_slots.push(idx as u32);
                        entry.key = u;
                        entry.val = id as u64;
                        return id;
                    }
                    idx = (idx + 1) & mask;
                }
            };

        for edges in thread_edges {
            let len = edges.len();
            for (i, &(a, b)) in edges.iter().enumerate() {
                #[cfg(target_arch = "x86_64")]
                if i + 16 < len {
                    let (fa, fb) = unsafe { *edges.get_unchecked(i + 16) };
                    unsafe {
                        _mm_prefetch(
                            table.as_ptr().add((mix64(fa) as usize) & mask) as *const i8,
                            _MM_HINT_T0,
                        );
                        _mm_prefetch(
                            table.as_ptr().add((mix64(fb) as usize) & mask) as *const i8,
                            _MM_HINT_T0,
                        );
                    }
                }
                let mut ia =
                    get_or_insert(a, &mut table, &mut parent, &mut min_id, &mut node_slots);
                let mut ib =
                    get_or_insert(b, &mut table, &mut parent, &mut min_id, &mut node_slots);
                while unsafe { *parent.get_unchecked(ia as usize) } != ia {
                    let p = unsafe { *parent.get_unchecked(ia as usize) };
                    unsafe {
                        *parent.get_unchecked_mut(ia as usize) =
                            *parent.get_unchecked(p as usize);
                    }
                    ia = p;
                }
                while unsafe { *parent.get_unchecked(ib as usize) } != ib {
                    let p = unsafe { *parent.get_unchecked(ib as usize) };
                    unsafe {
                        *parent.get_unchecked_mut(ib as usize) =
                            *parent.get_unchecked(p as usize);
                    }
                    ib = p;
                }
                if ia != ib {
                    if ia < ib {
                        parent[ib as usize] = ia;
                        if min_id[ib as usize] < min_id[ia as usize] {
                            min_id[ia as usize] = min_id[ib as usize];
                        }
                    } else {
                        parent[ia as usize] = ib;
                        if min_id[ia as usize] < min_id[ib as usize] {
                            min_id[ib as usize] = min_id[ia as usize];
                        }
                    }
                }
            }
        }

        // Compact table to only non-trivial mappings (where entry.key != canon)
        let bloom_words = (node_slots.len() / 2).next_power_of_two().clamp(64, 1 << 18);
        let bloom_mask = bloom_words - 1;
        let mut bloom = vec![0u64; bloom_words];
        let mut non_trivial = 0usize;

        for (i, &slot) in node_slots.iter().enumerate() {
            let mut r = unsafe { *parent.get_unchecked(i) };
            while unsafe { *parent.get_unchecked(r as usize) } != r {
                r = unsafe { *parent.get_unchecked(r as usize) };
            }
            let canon = unsafe { *min_id.get_unchecked(r as usize) };
            let entry = unsafe { table.get_unchecked_mut(slot as usize) };
            entry.val = canon;
            if entry.key != canon {
                non_trivial += 1;
                let h = mix64(entry.key);
                let w_idx = ((h >> 32) as usize) & bloom_mask;
                let b_mask = (1u64 << (h & 63)) | (1u64 << ((h >> 6) & 63));
                unsafe {
                    *bloom.get_unchecked_mut(w_idx) |= b_mask;
                }
            }
        }

        let compact_cap = (non_trivial * 2).next_power_of_two().max(64);
        let compact_mask = compact_cap - 1;
        let mut compact_table = vec![CanonEntry::default(); compact_cap];
        for &slot in &node_slots {
            let entry = unsafe { *table.get_unchecked(slot as usize) };
            if entry.key != entry.val {
                let mut idx = (mix64(entry.key) as usize) & compact_mask;
                loop {
                    let dst = unsafe { compact_table.get_unchecked_mut(idx) };
                    if dst.key == 0 {
                        *dst = entry;
                        break;
                    }
                    idx = (idx + 1) & compact_mask;
                }
            }
        }

        std::mem::forget(table);
        std::mem::forget(parent);
        std::mem::forget(min_id);
        std::mem::forget(node_slots);

        Self {
            bloom,
            bloom_mask,
            table: compact_table,
            mask: compact_mask,
        }
    }

    #[inline(always)]
    fn prefetch(&self, u: u64) {
        #[cfg(target_arch = "x86_64")]
        if self.mask != 1 {
            let h = mix64(u);
            let w_idx = ((h >> 32) as usize) & self.bloom_mask;
            let b_mask = (1u64 << (h & 63)) | (1u64 << ((h >> 6) & 63));
            if (unsafe { *self.bloom.get_unchecked(w_idx) } & b_mask) == b_mask {
                let idx = (h as usize) & self.mask;
                unsafe {
                    _mm_prefetch(self.table.as_ptr().add(idx) as *const i8, _MM_HINT_T0);
                }
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = u;
    }

    #[inline(always)]
    fn resolve(&self, u: u64) -> u64 {
        if self.mask == 1 {
            return u;
        }
        let h = mix64(u);
        let w_idx = ((h >> 32) as usize) & self.bloom_mask;
        let b_mask = (1u64 << (h & 63)) | (1u64 << ((h >> 6) & 63));
        if (unsafe { *self.bloom.get_unchecked(w_idx) } & b_mask) != b_mask {
            return u;
        }
        let mut idx = (h as usize) & self.mask;
        loop {
            let entry = unsafe { *self.table.get_unchecked(idx) };
            if entry.key == u {
                return entry.val;
            }
            if entry.key == 0 {
                return u;
            }
            idx = (idx + 1) & self.mask;
        }
    }
}

struct Catalog {
    window_start: i64,
    window_end: i64,
    base_ts: i64,
    window_start_day: i32,
    num_days: usize,
    // Fast 8-byte "YYYY-MM-" prefix cache for the window's months
    ym0_prefix: u64,
    ym0_base_days: i64,
    ym0_max_day: u32,
    ym1_prefix: u64,
    ym1_base_days: i64,
    ym1_max_day: u32,
    categories: Vec<Vec<u8>>,
    unknown_cat: u8,
    fast_prod: Vec<u8>,
    fallback_prod: HashMap<Vec<u8>, u8>,
    campaigns: Vec<(Vec<u8>, Vec<u8>)>,
    fast_camp: Vec<u16>,
    fallback_camp: HashMap<Vec<u8>, u16>,
}

#[inline(always)]
fn parse_positive_index(s: &[u8]) -> Option<usize> {
    if s.is_empty() || s[0] == b'0' || s.len() > 8 {
        return None;
    }
    let mut val = 0usize;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = val * 10 + (d as usize);
    }
    Some(val)
}

fn make_ym_prefix(year: u32, month: u32) -> (u64, i64, u32) {
    let s = format!("{year:04}-{month:02}-");
    let mut buf = [0u8; 8];
    buf.copy_from_slice(s.as_bytes());
    let prefix = u64::from_ne_bytes(buf);
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_d = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        _ => 31,
    };
    let base = days_from_civil_const(year, month, 1) - 1;
    (prefix, base, max_d)
}

impl Catalog {
    fn load(data_dir: &Path) -> io::Result<Self> {
        let manifest_str = fs::read_to_string(data_dir.join("manifest.json"))?;
        let extract_json_str = |key: &str| -> Option<(&[u8], i64)> {
            let pos = manifest_str.find(key)?;
            let rest = &manifest_str[pos + key.len()..];
            let q1 = rest.find('"')?;
            let after_q1 = &rest[q1 + 1..];
            let q2 = after_q1.find('"')?;
            let raw = &after_q1.as_bytes()[..q2];
            Some((raw, parse_f1(raw)?))
        };
        let (ws_raw, window_start) = extract_json_str("\"window_start\"")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad window_start"))?;
        let (_we_raw, window_end) = extract_json_str("\"window_end\"")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad window_end"))?;
        let base_ts = window_start - (CLICK_REACH_MS as i64);
        let window_start_day = window_start.div_euclid(DAY_MS) as i32;
        let window_end_day = (window_end - 1).div_euclid(DAY_MS) as i32;
        let num_days = (window_end_day - window_start_day + 2).max(1) as usize;

        let ws_year = parse_4d(ws_raw, 0).unwrap_or(2026);
        let ws_month = parse_2d(ws_raw, 5).unwrap_or(10);
        let (ym0_prefix, ym0_base_days, ym0_max_day) = make_ym_prefix(ws_year, ws_month);
        let (next_y, next_m) = if ws_month == 12 {
            (ws_year + 1, 1)
        } else {
            (ws_year, ws_month + 1)
        };
        let (ym1_prefix, ym1_base_days, ym1_max_day) = make_ym_prefix(next_y, next_m);

        let prod_bytes = fs::read(data_dir.join("products.csv"))?;
        let mut raw_cats: Vec<Vec<u8>> = vec![b"unknown".to_vec()];
        let mut parsed_prods: Vec<(&[u8], u8)> = Vec::with_capacity(50_000);
        if let Some(nl) = find_nl(&prod_bytes) {
            let mut body = &prod_bytes[nl + 1..];
            if body.ends_with(b"\n") {
                body = &body[..body.len() - 1];
            }
            if !body.is_empty() {
                for line in body.split(|&b| b == b'\n') {
                    if let Some([pid, cat]) = parse_csv_line_fallback::<2>(line) {
                        if !pid.is_empty() && !cat.is_empty() {
                            let raw_id = match raw_cats.iter().position(|c| c.as_slice() == cat) {
                                Some(idx) => idx as u8,
                                None => {
                                    let idx = raw_cats.len() as u8;
                                    raw_cats.push(cat.to_vec());
                                    idx
                                }
                            };
                            parsed_prods.push((pid, raw_id));
                        }
                    }
                }
            }
        }
        let mut sorted_cats = raw_cats.clone();
        sorted_cats.sort();
        sorted_cats.dedup();
        let mut raw_to_sorted = vec![0u8; raw_cats.len()];
        for (i, c) in raw_cats.iter().enumerate() {
            raw_to_sorted[i] = sorted_cats.binary_search(c).unwrap() as u8;
        }
        let unknown_cat = raw_to_sorted[0];

        let mut fast_prod = vec![unknown_cat; 65_536];
        let mut fallback_prod = HashMap::new();
        for (pid, raw_c) in parsed_prods {
            let cid = raw_to_sorted[raw_c as usize];
            if pid.starts_with(b"p") {
                if let Some(idx) = parse_positive_index(&pid[1..]) {
                    if idx < fast_prod.len() {
                        fast_prod[idx] = cid;
                        continue;
                    }
                }
            }
            fallback_prod.insert(pid.to_vec(), cid);
        }

        let camp_bytes = fs::read(data_dir.join("campaigns.csv"))?;
        let mut campaigns: Vec<(Vec<u8>, Vec<u8>)> =
            vec![(b"organic".to_vec(), b"organic".to_vec())];
        let mut fast_camp = vec![0u16; 16_384];
        let mut fallback_camp = HashMap::new();
        if let Some(nl) = find_nl(&camp_bytes) {
            let mut body = &camp_bytes[nl + 1..];
            if body.ends_with(b"\n") {
                body = &body[..body.len() - 1];
            }
            if !body.is_empty() {
                for line in body.split(|&b| b == b'\n') {
                    if let Some([cid_bytes, chan]) = parse_csv_line_fallback::<2>(line) {
                        if !cid_bytes.is_empty() && !chan.is_empty() {
                            let id_num = campaigns.len() as u16;
                            campaigns.push((cid_bytes.to_vec(), chan.to_vec()));
                            if cid_bytes.starts_with(b"c_") {
                                if let Some(idx) = parse_positive_index(&cid_bytes[2..]) {
                                    if idx < fast_camp.len() {
                                        fast_camp[idx] = id_num;
                                        continue;
                                    }
                                }
                            }
                            fallback_camp.insert(cid_bytes.to_vec(), id_num);
                        }
                    }
                }
            }
        }

        Ok(Self {
            window_start,
            window_end,
            base_ts,
            window_start_day,
            num_days,
            ym0_prefix,
            ym0_base_days,
            ym0_max_day,
            ym1_prefix,
            ym1_base_days,
            ym1_max_day,
            categories: sorted_cats,
            unknown_cat,
            fast_prod,
            fallback_prod,
            campaigns,
            fast_camp,
            fallback_camp,
        })
    }

    /// Ultra-fast F1 parser for `ingest_ts` (`rules.md`: "`ingest_ts` is always valid F1").
    #[inline(always)]
    fn parse_ingest_fast(&self, s: &[u8]) -> Option<i64> {
        if s.len() != 20 {
            return None;
        }
        let ym = unsafe { (s.as_ptr() as *const u64).read_unaligned() };
        let base_days = if ym == self.ym0_prefix {
            self.ym0_base_days
        } else if ym == self.ym1_prefix {
            self.ym1_base_days
        } else {
            return parse_f1(s);
        };
        let ptr = s.as_ptr();
        unsafe {
            let day = ((*ptr.add(8) - b'0') as i64) * 10 + ((*ptr.add(9) - b'0') as i64);
            let hour = ((*ptr.add(11) - b'0') as i64) * 10 + ((*ptr.add(12) - b'0') as i64);
            let min = ((*ptr.add(14) - b'0') as i64) * 10 + ((*ptr.add(15) - b'0') as i64);
            let sec = ((*ptr.add(17) - b'0') as i64) * 10 + ((*ptr.add(18) - b'0') as i64);
            Some((base_days + day) * DAY_MS + (hour * 3600 + min * 60 + sec) * 1000)
        }
    }

    /// Fast timestamp parser utilizing `ym0_prefix` / `ym1_prefix` 8-byte match and SWAR 13-digit F3.
    #[inline(always)]
    fn parse_ts_fast(&self, s: &[u8]) -> Option<i64> {
        let len = s.len();
        if len == 20 && unsafe { *s.get_unchecked(19) } == b'Z' {
            let ym = unsafe { (s.as_ptr() as *const u64).read_unaligned() };
            let (base_days, max_day) = if ym == self.ym0_prefix {
                (self.ym0_base_days, self.ym0_max_day)
            } else if ym == self.ym1_prefix {
                (self.ym1_base_days, self.ym1_max_day)
            } else {
                return parse_civil_19(s);
            };
            if unsafe {
                *s.get_unchecked(10) != b'T'
                    || *s.get_unchecked(13) != b':'
                    || *s.get_unchecked(16) != b':'
            } {
                return None;
            }
            let day = parse_2d(s, 8)?;
            let hour = parse_2d(s, 11)?;
            let min = parse_2d(s, 14)?;
            let sec = parse_2d(s, 17)?;
            if day == 0 || day > max_day || hour > 23 || min > 59 || sec > 59 {
                return None;
            }
            return Some(
                (base_days + day as i64) * DAY_MS + ((hour * 3600 + min * 60 + sec) as i64) * 1000,
            );
        }
        if len == 13 {
            let raw8 = unsafe { (s.as_ptr() as *const u64).read_unaligned() };
            let hi8 = parse_8d_swar(u64::from_le(raw8))?;
            let mid4 = parse_4d(s, 8)? as u64;
            let d12 = unsafe { *s.get_unchecked(12) }.wrapping_sub(b'0');
            if d12 > 9 {
                return None;
            }
            return Some((hi8 * 100_000 + mid4 * 10 + (d12 as u64)) as i64);
        }
        if len == 25 {
            let b19 = unsafe { *s.get_unchecked(19) };
            if (b19 == b'+' || b19 == b'-') && unsafe { *s.get_unchecked(22) } == b':' {
                let ym = unsafe { (s.as_ptr() as *const u64).read_unaligned() };
                let local_ms = if ym == self.ym0_prefix || ym == self.ym1_prefix {
                    let (base_days, max_day) = if ym == self.ym0_prefix {
                        (self.ym0_base_days, self.ym0_max_day)
                    } else {
                        (self.ym1_base_days, self.ym1_max_day)
                    };
                    if unsafe {
                        *s.get_unchecked(10) != b'T'
                            || *s.get_unchecked(13) != b':'
                            || *s.get_unchecked(16) != b':'
                    } {
                        return None;
                    }
                    let day = parse_2d(s, 8)?;
                    let hour = parse_2d(s, 11)?;
                    let min = parse_2d(s, 14)?;
                    let sec = parse_2d(s, 17)?;
                    if day == 0 || day > max_day || hour > 23 || min > 59 || sec > 59 {
                        return None;
                    }
                    (base_days + day as i64) * DAY_MS
                        + ((hour * 3600 + min * 60 + sec) as i64) * 1000
                } else {
                    parse_civil_19(s)?
                };
                let off_h = parse_2d(s, 20)?;
                let off_m = parse_2d(s, 23)?;
                if off_h > 23 || off_m > 59 {
                    return None;
                }
                let off_ms = ((off_h * 60 + off_m) as i64) * 60_000;
                return if b19 == b'+' {
                    Some(local_ms - off_ms)
                } else {
                    Some(local_ms + off_ms)
                };
            }
        }
        parse_ts(s)
    }

    #[inline(always)]
    fn lookup_product_cat(&self, pid: &[u8]) -> u8 {
        if pid.starts_with(b"p") {
            if let Some(idx) = parse_positive_index(&pid[1..]) {
                if idx < self.fast_prod.len() {
                    return unsafe { *self.fast_prod.get_unchecked(idx) };
                }
            }
        }
        if !self.fallback_prod.is_empty() {
            if let Some(&c) = self.fallback_prod.get(pid) {
                return c;
            }
        }
        self.unknown_cat
    }

    #[inline(always)]
    fn lookup_campaign(&self, cid: &[u8]) -> Option<u16> {
        if cid.starts_with(b"c_") {
            if let Some(idx) = parse_positive_index(&cid[2..]) {
                if idx < self.fast_camp.len() {
                    let v = unsafe { *self.fast_camp.get_unchecked(idx) };
                    if v != 0 {
                        return Some(v);
                    }
                }
            }
        }
        if !self.fallback_camp.is_empty() {
            if let Some(&v) = self.fallback_camp.get(cid) {
                return Some(v);
            }
        }
        None
    }
}

fn list_csv_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !dir.is_dir() {
        return Ok(out);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("csv") && path.is_file() {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

fn build_chunks<'a>(mmaps: &'a [MmapFile]) -> (Vec<&'a [u8]>, usize) {
    let mut chunks = Vec::new();
    let mut total_bytes = 0usize;
    for m in mmaps {
        let data = m.as_slice();
        let Some(first_nl) = find_nl(data) else {
            continue;
        };
        let mut body = &data[first_nl + 1..];
        if body.ends_with(b"\n") {
            body = &body[..body.len() - 1];
        }
        if body.is_empty() {
            continue;
        }
        total_bytes += body.len();
        let mut pos = 0;
        while pos < body.len() {
            let target = (pos + CHUNK_SIZE).min(body.len());
            if target == body.len() {
                chunks.push(&body[pos..]);
                break;
            }
            if let Some(rel_nl) = find_nl(&body[target..]) {
                let end = target + rel_nl;
                chunks.push(&body[pos..end]);
                pos = end + 1;
            } else {
                chunks.push(&body[pos..]);
                break;
            }
        }
    }
    (chunks, total_bytes)
}

/// Cold-field split shard storage: `evs` is 24 bytes (`[eid, user_id, payload]`, 8-byte aligned),
/// while `ings` (`i32` seconds) is stored in a parallel Vec that is ONLY touched when a duplicate `eid` occurs!
#[derive(Default)]
struct EventShard {
    evs: Vec<[u64; 3]>,
    ings: Vec<i32>,
}

impl EventShard {
    fn with_capacity(cap: usize) -> Self {
        Self {
            evs: Vec::with_capacity(cap),
            ings: Vec::with_capacity(cap),
        }
    }

    #[inline(always)]
    fn push(&mut self, eid: u64, user_id: u64, payload: u64, ingest_sec: i32) {
        self.evs.push([eid, user_id, payload]);
        self.ings.push(ingest_sec);
    }
}

/// 8-byte DedupSlot: fits 8 slots per 64-byte cache line!
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct DedupSlot {
    tag: u16,
    epoch: u16,
    best_ref: u32, // (best_t << 24) | best_idx
}

#[repr(C)]
#[derive(Clone, Copy)]
struct ShardedClick {
    person_id: u64,
    click_id: u64,
    rel_ts: u32,
    camp_id: u16,
}

/// 8-byte UserSlot: fits 8 slots per 64-byte cache line!
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct UserSlot {
    tag: u16,
    epoch: u16,
    uidx: u32,
}

#[derive(Clone, Copy, Default)]
struct UserClick {
    rel_ts: u32,
    camp_id: u16,
    click_id: u64,
}

#[derive(Clone, Copy, Default)]
struct CampDayStat {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

#[inline(always)]
fn parse_price_cents(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut val: u64 = 0;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = (val * 10 + (d as u64)).min(100_000_000);
    }
    if (1..=10_000_000).contains(&val) {
        Some(val as u32)
    } else {
        None
    }
}

fn run(data_dir: &Path, out_dir: &Path, threads: usize) -> io::Result<()> {
    let prof = env::var("PROFILE").is_ok();
    let t0 = std::time::Instant::now();
    let catalog = Catalog::load(data_dir)?;
    let t_cat = t0.elapsed();

    // 1. Load and process links/*.csv
    let link_files = list_csv_files(&data_dir.join("links"))?;
    let link_mmaps: Vec<MmapFile> = link_files
        .iter()
        .map(|p| MmapFile::open(p))
        .collect::<io::Result<_>>()?;
    let (link_chunks, link_bytes) = build_chunks(&link_mmaps);

    let est_links_per_thread = (link_bytes / 14 / threads).max(64);
    let next_link = AtomicUsize::new(0);
    let mut thread_edges: Vec<Vec<(u64, u64)>> = (0..threads).map(|_| Vec::new()).collect();
    thread::scope(|s| {
        for edges_out in &mut thread_edges {
            let link_chunks = &link_chunks;
            let next_link = &next_link;
            s.spawn(move || {
                edges_out.reserve(est_links_per_thread);
                while let Some(&chunk) = link_chunks.get(next_link.fetch_add(1, Ordering::Relaxed))
                {
                    for_each_csv_record::<2>(chunk, |[a, b]| {
                        if !a.is_empty() && !b.is_empty() {
                            let ua = pack7(a);
                            let ub = pack7(b);
                            if ua != ub {
                                edges_out.push((ua, ub));
                            }
                        }
                    });
                }
            });
        }
    });
    std::mem::forget(link_mmaps);
    let t_links = t0.elapsed();

    // 2. Overlap CanonMap::build on Thread 0 with events/*.csv parsing on Threads 1..N,
    //    then parse clicks/*.csv across all threads once CanonMap is ready!
    let click_files = list_csv_files(&data_dir.join("clicks"))?;
    let click_mmaps: Vec<MmapFile> = click_files
        .iter()
        .map(|p| MmapFile::open(p))
        .collect::<io::Result<_>>()?;
    let (click_chunks, click_bytes) = build_chunks(&click_mmaps);

    let event_files = list_csv_files(&data_dir.join("events"))?;
    let event_mmaps: Vec<MmapFile> = event_files
        .iter()
        .map(|p| MmapFile::open(p))
        .collect::<io::Result<_>>()?;
    let (event_chunks, event_bytes) = build_chunks(&event_mmaps);

    let est_clicks_per_shard = (click_bytes / 38 / (threads * NUM_SHARDS)).max(8) + 32;
    let est_events_per_shard = (event_bytes / 62 / (threads * NUM_SHARDS)).max(8) + 64;

    let next_click = AtomicUsize::new(0);
    let next_event = AtomicUsize::new(0);
    let canon_slot: std::sync::OnceLock<CanonMap> = std::sync::OnceLock::new();

    let mut per_thread_clicks: Vec<Vec<Vec<ShardedClick>>> =
        (0..threads).map(|_| Vec::new()).collect();
    let mut per_thread_events: Vec<Vec<EventShard>> =
        (0..threads).map(|_| Vec::new()).collect();

    thread::scope(|s| {
        for (t_idx, (c_shards, e_shards)) in per_thread_clicks
            .iter_mut()
            .zip(per_thread_events.iter_mut())
            .enumerate()
        {
            let catalog = &catalog;
            let canon_slot = &canon_slot;
            let thread_edges = &thread_edges;
            let click_chunks = &click_chunks;
            let event_chunks = &event_chunks;
            let next_click = &next_click;
            let next_event = &next_event;
            s.spawn(move || {
                if t_idx == 0 {
                    let built = CanonMap::build(thread_edges);
                    let _ = canon_slot.set(built);
                }
                *c_shards = (0..NUM_SHARDS)
                    .map(|_| Vec::with_capacity(est_clicks_per_shard))
                    .collect();
                *e_shards = (0..NUM_SHARDS)
                    .map(|_| EventShard::with_capacity(est_events_per_shard))
                    .collect();

                // Parse events FIRST (does not need CanonMap!)
                while let Some(&chunk) =
                    event_chunks.get(next_event.fetch_add(1, Ordering::Relaxed))
                {
                    for_each_csv_record::<7>(
                        chunk,
                        |[eid_b, uid_b, ts_b, ing_b, etype_b, pid_b, price_b]| {
                            if eid_b.is_empty() {
                                return;
                            }
                            let Some(ingest_ts) = catalog.parse_ingest_fast(ing_b) else {
                                return;
                            };
                            let eid = pack7(eid_b);
                            let shard = (mix64(eid) as usize) & (NUM_SHARDS - 1);

                            let payload = 'val: {
                                if uid_b.is_empty() || pid_b.is_empty() {
                                    break 'val INVALID_PAYLOAD;
                                }
                                let is_purchase = match etype_b {
                                    b"view" | b"cart" => false,
                                    b"purchase" => true,
                                    _ => break 'val INVALID_PAYLOAD,
                                };
                                let (price, cat_id) = if is_purchase {
                                    let Some(p) = parse_price_cents(price_b) else {
                                        break 'val INVALID_PAYLOAD;
                                    };
                                    (p, catalog.lookup_product_cat(pid_b))
                                } else {
                                    (0u32, 0u8)
                                };
                                let Some(ts) = catalog.parse_ts_fast(ts_b) else {
                                    break 'val INVALID_PAYLOAD;
                                };
                                if ts < catalog.window_start || ts >= catalog.window_end {
                                    break 'val INVALID_PAYLOAD;
                                }
                                if ingest_ts - ts > DAY_MS {
                                    break 'val INVALID_PAYLOAD;
                                }
                                let rel_ts = (ts - catalog.base_ts) as u32;
                                ((rel_ts as u64) << 32) | ((price as u64) << 8) | (cat_id as u64)
                            };

                            let user_id = if payload != INVALID_PAYLOAD {
                                pack7(uid_b)
                            } else {
                                0
                            };
                            let ingest_sec = ((ingest_ts - catalog.base_ts) / 1000)
                                .clamp(i32::MIN as i64, i32::MAX as i64)
                                as i32;
                            e_shards[shard].push(eid, user_id, payload, ingest_sec);
                        },
                    );
                }

                // Wait for CanonMap (already finished on Thread 0 during events parsing) and parse clicks
                let canon_map = canon_slot.wait();
                while let Some(&chunk) =
                    click_chunks.get(next_click.fetch_add(1, Ordering::Relaxed))
                {
                    for_each_csv_record::<4>(chunk, |[cid_b, uid_b, camp_b, ts_b]| {
                        if cid_b.is_empty() || uid_b.is_empty() {
                            return;
                        }
                        let Some(camp_id) = catalog.lookup_campaign(camp_b) else {
                            return;
                        };
                        let Some(ts) = catalog.parse_ts_fast(ts_b) else {
                            return;
                        };
                        if ts < catalog.base_ts || ts >= catalog.window_end {
                            return;
                        }
                        let person_id = canon_map.resolve(pack7(uid_b));
                        let shard = (mix64(person_id) as usize) & (NUM_SHARDS - 1);
                        c_shards[shard].push(ShardedClick {
                            person_id,
                            click_id: pack7(cid_b),
                            rel_ts: (ts - catalog.base_ts) as u32,
                            camp_id,
                        });
                    });
                }
            });
        }
    });
    std::mem::forget(thread_edges);
    std::mem::forget(click_mmaps);
    std::mem::forget(event_mmaps);
    let canon_map = canon_slot.into_inner().unwrap();
    let t_parse = t0.elapsed();

    // 3. Deduplicate events per event-shard in parallel (using 16-byte DedupSlot + cold ingest_sec)
    let next_dedup_shard = AtomicUsize::new(0);
    let mut per_thread_user_events: Vec<Vec<Vec<(u64, u64)>>> =
        (0..threads).map(|_| Vec::new()).collect();

    thread::scope(|s| {
        for out_user_shards in &mut per_thread_user_events {
            let per_thread_events = &per_thread_events;
            let canon_map = &canon_map;
            let next_dedup_shard = &next_dedup_shard;
            s.spawn(move || {
                *out_user_shards = (0..NUM_SHARDS)
                    .map(|_| Vec::with_capacity(est_events_per_shard))
                    .collect();

                let mut ht_cap = 1usize << 16;
                let mut ht: Vec<DedupSlot> = vec![DedupSlot::default(); ht_cap];
                let mut cur_gen: u16 = 0;
                let mut killed: Vec<Vec<u64>> = (0..threads).map(|_| Vec::new()).collect();
                let mut any_duplicates;

                loop {
                    let shard = next_dedup_shard.fetch_add(1, Ordering::Relaxed);
                    if shard >= NUM_SHARDS {
                        break;
                    }
                    let total_in_shard: usize =
                        per_thread_events.iter().map(|t| t[shard].evs.len()).sum();
                    if total_in_shard == 0 {
                        continue;
                    }
                    let needed_cap = (total_in_shard * 2).next_power_of_two().max(1 << 16);
                    if needed_cap > ht_cap {
                        ht_cap = needed_cap;
                        ht = vec![DedupSlot::default(); ht_cap];
                        cur_gen = 0;
                    }
                    cur_gen = cur_gen.wrapping_add(1);
                    if cur_gen == 0 {
                        for slot in &mut ht {
                            slot.epoch = 0;
                        }
                        cur_gen = 1;
                    }
                    let mask = ht_cap - 1;
                    any_duplicates = false;

                    for (t_idx, t_vec) in per_thread_events.iter().enumerate() {
                        let evs = &t_vec[shard].evs;
                        let slen = evs.len();
                        for (i, &[eid, _, _]) in evs.iter().enumerate() {
                            #[cfg(target_arch = "x86_64")]
                            if i + 12 < slen {
                                let feid = unsafe { evs.get_unchecked(i + 12)[0] };
                                let fidx = ((mix64(feid) >> SHARD_SHIFT) as usize) & mask;
                                unsafe {
                                    _mm_prefetch(ht.as_ptr().add(fidx) as *const i8, _MM_HINT_T0);
                                }
                            }
                            let h = mix64(eid);
                            let tag = (h >> 48) as u16;
                            let mut idx = ((h >> SHARD_SHIFT) as usize) & mask;
                            loop {
                                let slot = unsafe { ht.get_unchecked_mut(idx) };
                                if slot.epoch != cur_gen {
                                    slot.epoch = cur_gen;
                                    slot.tag = tag;
                                    slot.best_ref = ((t_idx as u32) << 24) | (i as u32);
                                    break;
                                }
                                if slot.tag == tag {
                                    let prev_ref = slot.best_ref;
                                    let prev_t = (prev_ref >> 24) as usize;
                                    let prev_i = (prev_ref & 0x00FF_FFFF) as usize;
                                    if unsafe {
                                        per_thread_events
                                            .get_unchecked(prev_t)
                                            .get_unchecked(shard)
                                            .evs
                                            .get_unchecked(prev_i)[0]
                                    } == eid
                                    {
                                        if !any_duplicates {
                                            any_duplicates = true;
                                            for (kt, tv) in killed.iter_mut().enumerate() {
                                                let words =
                                                    (per_thread_events[kt][shard].evs.len() + 63)
                                                        / 64;
                                                tv.clear();
                                                tv.resize(words, 0u64);
                                            }
                                        }
                                        let prev_ing =
                                            per_thread_events[prev_t][shard].ings[prev_i];
                                        let cur_ing = t_vec[shard].ings[i];
                                        if cur_ing < prev_ing {
                                            killed[prev_t][prev_i >> 6] |= 1u64 << (prev_i & 63);
                                            slot.best_ref = ((t_idx as u32) << 24) | (i as u32);
                                        } else {
                                            killed[t_idx][i >> 6] |= 1u64 << (i & 63);
                                        }
                                        break;
                                    }
                                }
                                idx = (idx + 1) & mask;
                            }
                        }
                    }

                    if !any_duplicates {
                        for t_vec in per_thread_events {
                            let evs = &t_vec[shard].evs;
                            let slen = evs.len();
                            for (i, &[_, user_id, payload]) in evs.iter().enumerate() {
                                if i + 16 < slen {
                                    canon_map
                                        .prefetch(unsafe { evs.get_unchecked(i + 16)[1] });
                                }
                                if payload != INVALID_PAYLOAD {
                                    let person_id = canon_map.resolve(user_id);
                                    let u_shard = (mix64(person_id) as usize) & (NUM_SHARDS - 1);
                                    out_user_shards[u_shard].push((person_id, payload));
                                }
                            }
                        }
                    } else {
                        for (t_idx, t_vec) in per_thread_events.iter().enumerate() {
                            let kw = &killed[t_idx];
                            let evs = &t_vec[shard].evs;
                            let slen = evs.len();
                            for (i, &[_, user_id, payload]) in evs.iter().enumerate() {
                                if i + 16 < slen {
                                    canon_map
                                        .prefetch(unsafe { evs.get_unchecked(i + 16)[1] });
                                }
                                if payload != INVALID_PAYLOAD
                                    && (kw[i >> 6] & (1u64 << (i & 63))) == 0
                                {
                                    let person_id = canon_map.resolve(user_id);
                                    let u_shard = (mix64(person_id) as usize) & (NUM_SHARDS - 1);
                                    out_user_shards[u_shard].push((person_id, payload));
                                }
                            }
                        }
                    }
                }
            });
        }
    });
    std::mem::forget(per_thread_events);
    let t_dedup = t0.elapsed();

    // 4. Process user-shards in parallel: sessionize, attribute clicks, count assists, emit UserDailyRow
    let next_user_shard = AtomicUsize::new(0);
    let num_camps = catalog.campaigns.len();
    let num_days = catalog.num_days;
    let num_cats = catalog.categories.len();

    let mut thread_user_rows: Vec<Vec<UserDailyRow>> = (0..threads).map(|_| Vec::new()).collect();
    let mut thread_camp_stats: Vec<Vec<CampDayStat>> = (0..threads)
        .map(|_| vec![CampDayStat::default(); num_camps * num_days])
        .collect();

    thread::scope(|s| {
        for (u_rows_out, c_stats_out) in thread_user_rows
            .iter_mut()
            .zip(thread_camp_stats.iter_mut())
        {
            let per_thread_user_events = &per_thread_user_events;
            let per_thread_clicks = &per_thread_clicks;
            let catalog = &catalog;
            let next_user_shard = &next_user_shard;
            s.spawn(move || {
                u_rows_out.reserve(est_events_per_shard * NUM_SHARDS / 6);
                let mut ht_cap = 1usize << 14;
                let mut ht: Vec<UserSlot> = vec![UserSlot::default(); ht_cap];
                let mut cur_gen: u16 = 0;

                let mut user_ids: Vec<u64> = Vec::new();
                let mut ev_counts: Vec<u32> = Vec::new();
                let mut cl_counts: Vec<u32> = Vec::new();
                let mut ev_offsets: Vec<u32> = Vec::new();
                let mut cl_offsets: Vec<u32> = Vec::new();
                // Cache uidx from Pass 1 so Pass 2 does ZERO hash table lookups!
                let mut resolved_ev_uidx: Vec<u32> = Vec::new();
                let mut resolved_clicks: Vec<(u32, UserClick)> = Vec::new();
                let mut csr_events: Vec<u64> = Vec::new();
                let mut csr_clicks: Vec<UserClick> = Vec::new();

                let mut seen_camp_stamp = vec![0u32; num_camps];
                let mut stamp_token: u32 = 0;
                let mut whale_latest_ts = vec![0u32; num_camps];
                let mut whale_camp_stamp = vec![0u32; num_camps];
                let mut whale_token: u32 = 0;
                let mut whale_camps: Vec<u16> = Vec::new();
                let mut cat_rev = vec![0i64; num_cats];

                loop {
                    let shard = next_user_shard.fetch_add(1, Ordering::Relaxed);
                    if shard >= NUM_SHARDS {
                        break;
                    }
                    let total_ev: usize = per_thread_user_events
                        .iter()
                        .map(|t| t[shard].len())
                        .sum();
                    if total_ev == 0 {
                        continue;
                    }
                    // Each person has ~30 events on average; size ht for unique users (fits in L2 cache!)
                    let needed_cap = (total_ev / 4).next_power_of_two().max(1 << 13);
                    if needed_cap > ht_cap {
                        ht_cap = needed_cap;
                        ht = vec![UserSlot::default(); ht_cap];
                        cur_gen = 0;
                    }
                    cur_gen = cur_gen.wrapping_add(1);
                    if cur_gen == 0 {
                        for slot in &mut ht {
                            slot.epoch = 0;
                        }
                        cur_gen = 1;
                    }
                    let mut mask = ht_cap - 1;

                    user_ids.clear();
                    ev_counts.clear();
                    cl_counts.clear();
                    resolved_ev_uidx.clear();
                    resolved_ev_uidx.reserve(total_ev);
                    resolved_clicks.clear();

                    // Pass 1: count events per unique person and cache uidx
                    for t_vec in per_thread_user_events {
                        let slice = &t_vec[shard];
                        for &(pid, _) in slice {
                            if user_ids.len() * 2 >= ht_cap {
                                ht_cap *= 2;
                                mask = ht_cap - 1;
                                ht = vec![UserSlot::default(); ht_cap];
                                cur_gen = 1;
                                for (u_i, &existing_pid) in user_ids.iter().enumerate() {
                                    let eh = mix64(existing_pid);
                                    let etag = (eh >> 48) as u16;
                                    let mut eidx = ((eh >> SHARD_SHIFT) as usize) & mask;
                                    loop {
                                        let eslot = unsafe { ht.get_unchecked_mut(eidx) };
                                        if eslot.epoch == 0 {
                                            eslot.epoch = 1;
                                            eslot.tag = etag;
                                            eslot.uidx = u_i as u32;
                                            break;
                                        }
                                        eidx = (eidx + 1) & mask;
                                    }
                                }
                            }
                            let h = mix64(pid);
                            let tag = (h >> 48) as u16;
                            let mut idx = ((h >> SHARD_SHIFT) as usize) & mask;
                            let uidx = loop {
                                let slot = unsafe { ht.get_unchecked_mut(idx) };
                                if slot.epoch != cur_gen {
                                    let u = user_ids.len() as u32;
                                    slot.epoch = cur_gen;
                                    slot.tag = tag;
                                    slot.uidx = u;
                                    user_ids.push(pid);
                                    ev_counts.push(0);
                                    cl_counts.push(0);
                                    break u;
                                }
                                if slot.tag == tag
                                    && unsafe { *user_ids.get_unchecked(slot.uidx as usize) } == pid
                                {
                                    break slot.uidx;
                                }
                                idx = (idx + 1) & mask;
                            };
                            ev_counts[uidx as usize] += 1;
                            resolved_ev_uidx.push(uidx);
                        }
                    }

                    // Pass 2: match clicks against users with events and cache (uidx, UserClick)
                    for t_vec in per_thread_clicks {
                        for cl in &t_vec[shard] {
                            let h = mix64(cl.person_id);
                            let tag = (h >> 48) as u16;
                            let mut idx = ((h >> SHARD_SHIFT) as usize) & mask;
                            loop {
                                let slot = unsafe { *ht.get_unchecked(idx) };
                                if slot.epoch != cur_gen {
                                    break;
                                }
                                if slot.tag == tag
                                    && unsafe { *user_ids.get_unchecked(slot.uidx as usize) }
                                        == cl.person_id
                                {
                                    let u = slot.uidx;
                                    cl_counts[u as usize] += 1;
                                    resolved_clicks.push((
                                        u,
                                        UserClick {
                                            rel_ts: cl.rel_ts,
                                            camp_id: cl.camp_id,
                                            click_id: cl.click_id,
                                        },
                                    ));
                                    break;
                                }
                                idx = (idx + 1) & mask;
                            }
                        }
                    }

                    let num_users = user_ids.len();
                    ev_offsets.clear();
                    ev_offsets.reserve(num_users + 1);
                    let mut acc_ev = 0u32;
                    for &c in &ev_counts {
                        ev_offsets.push(acc_ev);
                        acc_ev += c;
                    }
                    ev_offsets.push(acc_ev);

                    cl_offsets.clear();
                    cl_offsets.reserve(num_users + 1);
                    let mut acc_cl = 0u32;
                    for &c in &cl_counts {
                        cl_offsets.push(acc_cl);
                        acc_cl += c;
                    }
                    cl_offsets.push(acc_cl);

                    let total_cl = resolved_clicks.len();
                    csr_events.resize(total_ev, 0);
                    csr_clicks.resize(total_cl, UserClick::default());

                    ev_counts.copy_from_slice(&ev_offsets[..num_users]);
                    cl_counts.copy_from_slice(&cl_offsets[..num_users]);

                    // Scatter events using cached resolved_ev_uidx (ZERO hash probes!)
                    let mut flat_idx = 0usize;
                    for t_vec in per_thread_user_events {
                        for &(_, payload) in &t_vec[shard] {
                            let u = unsafe { *resolved_ev_uidx.get_unchecked(flat_idx) } as usize;
                            flat_idx += 1;
                            let pos = unsafe { *ev_counts.get_unchecked(u) } as usize;
                            unsafe {
                                *ev_counts.get_unchecked_mut(u) = (pos + 1) as u32;
                                *csr_events.get_unchecked_mut(pos) = payload;
                            }
                        }
                    }

                    // Scatter clicks using cached resolved_clicks (ZERO hash probes!)
                    for &(u, ucl) in &resolved_clicks {
                        let u_idx = u as usize;
                        let pos = unsafe { *cl_counts.get_unchecked(u_idx) } as usize;
                        unsafe {
                            *cl_counts.get_unchecked_mut(u_idx) = (pos + 1) as u32;
                            *csr_clicks.get_unchecked_mut(pos) = ucl;
                        }
                    }

                    for u in 0..num_users {
                        let pid = user_ids[u];
                        let u_evs = &mut csr_events
                            [ev_offsets[u] as usize..ev_offsets[u + 1] as usize];
                        let u_cls = &mut csr_clicks
                            [cl_offsets[u] as usize..cl_offsets[u + 1] as usize];

                        u_evs.sort_unstable();
                        if u_cls.len() > 1 {
                            u_cls.sort_unstable_by(|a, b| {
                                a.rel_ts
                                    .cmp(&b.rel_ts)
                                    .then_with(|| b.click_id.cmp(&a.click_id))
                            });
                        }

                        let is_whale_clicks = u_cls.len() > 64;
                        if is_whale_clicks {
                            whale_token = whale_token.wrapping_add(1);
                            if whale_token == 0 {
                                whale_camp_stamp.fill(0);
                                whale_token = 1;
                            }
                            whale_camps.clear();
                        }

                        let mut cl_left = 0usize;
                        let mut cl_right = 0usize;

                        let first_ev = u_evs[0];
                        let mut sess_start_rel = (first_ev >> 32) as u32;
                        let mut prev_rel_ts = sess_start_rel;
                        let mut sess_day = (((catalog.base_ts + sess_start_rel as i64) as u64)
                            / (DAY_MS as u64)) as i32;
                        let mut sess_rev = ((first_ev as u32) >> 8) as i64;

                        let mut cur_day = sess_day;
                        let mut day_sessions = 1u32;
                        let mut day_events = 1u32;
                        let mut day_rev = sess_rev;
                        cat_rev.fill(0);
                        if sess_rev > 0 {
                            cat_rev[(first_ev & 0xFF) as usize] += sess_rev;
                        }

                        let mut credit_session =
                            |s_rel: u32, s_day: i32, s_rev: i64| {
                                let day_idx = (s_day - catalog.window_start_day) as usize;
                                while cl_right < u_cls.len() && u_cls[cl_right].rel_ts <= s_rel {
                                    if is_whale_clicks {
                                        let c = &u_cls[cl_right];
                                        let cid = c.camp_id as usize;
                                        if whale_camp_stamp[cid] != whale_token {
                                            whale_camp_stamp[cid] = whale_token;
                                            whale_camps.push(c.camp_id);
                                        }
                                        whale_latest_ts[cid] = c.rel_ts;
                                    }
                                    cl_right += 1;
                                }
                                let min_rel = s_rel.saturating_sub(CLICK_REACH_MS);
                                while cl_left < cl_right && u_cls[cl_left].rel_ts < min_rel {
                                    cl_left += 1;
                                }
                                if cl_left == cl_right {
                                    let st = &mut c_stats_out[day_idx];
                                    st.sessions += 1;
                                    st.purchasing_sessions += i64::from(s_rev > 0);
                                    st.revenue_cents += s_rev;
                                } else {
                                    let win_camp = u_cls[cl_right - 1].camp_id as usize;
                                    let st = &mut c_stats_out[win_camp * num_days + day_idx];
                                    st.sessions += 1;
                                    st.purchasing_sessions += i64::from(s_rev > 0);
                                    st.revenue_cents += s_rev;

                                    if cl_right - cl_left == 1 {
                                        st.assisted_sessions += 1;
                                    } else if is_whale_clicks {
                                        for &cid_u16 in &whale_camps {
                                            let cid = cid_u16 as usize;
                                            if whale_latest_ts[cid] >= min_rel {
                                                c_stats_out[cid * num_days + day_idx]
                                                    .assisted_sessions += 1;
                                            }
                                        }
                                    } else {
                                        stamp_token = stamp_token.wrapping_add(1);
                                        if stamp_token == 0 {
                                            seen_camp_stamp.fill(0);
                                            stamp_token = 1;
                                        }
                                        for cl in &u_cls[cl_left..cl_right] {
                                            let cid = cl.camp_id as usize;
                                            if seen_camp_stamp[cid] != stamp_token {
                                                seen_camp_stamp[cid] = stamp_token;
                                                c_stats_out[cid * num_days + day_idx]
                                                    .assisted_sessions += 1;
                                            }
                                        }
                                    }
                                }
                            };

                        for &ev in &u_evs[1..] {
                            let rel_ts = (ev >> 32) as u32;
                            let price = ((ev as u32) >> 8) as i64;
                            if rel_ts - prev_rel_ts > SESSION_GAP_MS {
                                credit_session(sess_start_rel, sess_day, sess_rev);

                                sess_start_rel = rel_ts;
                                sess_day = (((catalog.base_ts + sess_start_rel as i64) as u64)
                                    / (DAY_MS as u64))
                                    as i32;
                                sess_rev = price;

                                if sess_day != cur_day {
                                    let top_cat = if day_rev == 0 {
                                        0xFF
                                    } else {
                                        let mut best_c = 0u8;
                                        let mut best_r = 0i64;
                                        for (ci, &r) in cat_rev.iter().enumerate() {
                                            if r > best_r {
                                                best_r = r;
                                                best_c = ci as u8;
                                            }
                                        }
                                        best_c
                                    };
                                    u_rows_out.push(UserDailyRow {
                                        person_id: pid,
                                        day: cur_day,
                                        sessions: day_sessions,
                                        events: day_events,
                                        revenue_cents: day_rev,
                                        top_cat,
                                    });
                                    cur_day = sess_day;
                                    day_sessions = 0;
                                    day_events = 0;
                                    day_rev = 0;
                                    cat_rev.fill(0);
                                }
                                day_sessions += 1;
                            } else {
                                sess_rev += price;
                            }
                            prev_rel_ts = rel_ts;
                            day_events += 1;
                            day_rev += price;
                            if price > 0 {
                                cat_rev[(ev & 0xFF) as usize] += price;
                            }
                        }

                        credit_session(sess_start_rel, sess_day, sess_rev);
                        let top_cat = if day_rev == 0 {
                            0xFF
                        } else {
                            let mut best_c = 0u8;
                            let mut best_r = 0i64;
                            for (ci, &r) in cat_rev.iter().enumerate() {
                                if r > best_r {
                                    best_r = r;
                                    best_c = ci as u8;
                                }
                            }
                            best_c
                        };
                        u_rows_out.push(UserDailyRow {
                            person_id: pid,
                            day: cur_day,
                            sessions: day_sessions,
                            events: day_events,
                            revenue_cents: day_rev,
                            top_cat,
                        });
                    }
                }
            });
        }
    });
    std::mem::forget(per_thread_user_events);
    std::mem::forget(per_thread_clicks);

    // 5. Write output Parquet files in parallel
    let t_sess = t0.elapsed();
    let user_daily_dir = out_dir.join("user_daily");
    let camp_daily_dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&user_daily_dir)?;
    fs::create_dir_all(&camp_daily_dir)?;

    let mut total_camp_stats = vec![CampDayStat::default(); num_camps * num_days];
    for t_stats in &thread_camp_stats {
        for (dst, src) in total_camp_stats.iter_mut().zip(t_stats.iter()) {
            dst.sessions += src.sessions;
            dst.purchasing_sessions += src.purchasing_sessions;
            dst.revenue_cents += src.revenue_cents;
            dst.assisted_sessions += src.assisted_sessions;
        }
    }

    let mut camp_rows: Vec<CampaignDailyRow> = Vec::new();
    for cid in 0..num_camps {
        let (ref camp_name, ref chan_name) = catalog.campaigns[cid];
        for d in 0..num_days {
            let st = &total_camp_stats[cid * num_days + d];
            if st.sessions > 0 || st.assisted_sessions > 0 {
                camp_rows.push(CampaignDailyRow {
                    campaign_id: camp_name,
                    channel: chan_name,
                    day: catalog.window_start_day + (d as i32),
                    sessions: st.sessions,
                    purchasing_sessions: st.purchasing_sessions,
                    revenue_cents: st.revenue_cents,
                    assisted_sessions: if cid == 0 { 0 } else { st.assisted_sessions },
                });
            }
        }
    }

    let total_user_rows: usize = thread_user_rows.iter().map(|v| v.len()).sum();
    let write_err: std::sync::Mutex<Option<io::Error>> = std::sync::Mutex::new(None);

    thread::scope(|s| {
        s.spawn(|| {
            if let Err(e) =
                write_campaign_daily_parquet(&camp_daily_dir.join("part-0.parquet"), &camp_rows)
            {
                *write_err.lock().unwrap() = Some(e);
            }
        });

        if total_user_rows == 0 {
            s.spawn(|| {
                if let Err(e) = write_user_daily_parquet(
                    &user_daily_dir.join("part-0.parquet"),
                    &[],
                    &catalog.categories,
                ) {
                    *write_err.lock().unwrap() = Some(e);
                }
            });
        } else {
            for (t_idx, rows) in thread_user_rows.iter().enumerate() {
                if rows.is_empty() {
                    continue;
                }
                let user_daily_dir = &user_daily_dir;
                let categories = &catalog.categories;
                let write_err = &write_err;
                s.spawn(move || {
                    let path = user_daily_dir.join(format!("part-{t_idx}.parquet"));
                    if let Err(e) = write_user_daily_parquet(&path, rows, categories) {
                        *write_err.lock().unwrap() = Some(e);
                    }
                });
            }
        }
    });

    if let Some(e) = write_err.into_inner().unwrap() {
        return Err(e);
    }
    if prof {
        let t_end = t0.elapsed();
        eprintln!(
            "cat: {:?}, links: {:?}, parse: {:?}, dedup: {:?}, sess: {:?}, write: {:?}, total: {:?}",
            t_cat,
            t_links - t_cat,
            t_parse - t_links,
            t_dedup - t_parse,
            t_sess - t_dedup,
            t_end - t_sess,
            t_end
        );
    }

    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let data_dir = env::var("DATA_DIR")
        .ok()
        .or_else(|| {
            if args.len() >= 3 {
                Some(args[args.len() - 2].clone())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "/data".to_string());
    let out_dir = env::var("OUTPUT_DIR")
        .ok()
        .or_else(|| {
            if args.len() >= 3 {
                Some(args[args.len() - 1].clone())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "/output".to_string());

    let threads = env::var("NCPU")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| thread::available_parallelism().map_or(8, usize::from));

    match run(Path::new(&data_dir), Path::new(&out_dir), threads) {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("shopping-sessions error: {e}");
            std::process::exit(1);
        }
    }
}
