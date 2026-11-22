use std::cmp::Reverse;
use std::env;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use memchr::{memchr, memchr2};
use memmap2::{Advice, Mmap};
use mimalloc::MiMalloc;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};
use serde::Deserialize;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const NUM_PARTS: usize = 32;
const PART_SHIFT: u32 = 5;
const WRITE_SHARDS: usize = 8;
const MS_PER_DAY: i64 = 86_400_000;
const LATE_LIMIT_MS: i64 = 86_400_000;
const SESSION_GAP_MS: i64 = 1_800_000;
const SEVEN_DAYS_MS: i64 = 604_800_000;

#[derive(Deserialize)]
struct Manifest {
    window_start: String,
    window_end: String,
}

#[inline]
fn hash_bytes(b: &[u8]) -> u64 {
    let mut h = FxHasher::default();
    b.hash(&mut h);
    h.finish()
}

/// Strict single-pass SIMD CSV line parser using memchr2(b',', b'"').
#[inline]
fn parse_csv_line<'a, const N: usize>(line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    let len = line.len();
    if len == 0 {
        return None;
    }
    let mut out = [&b""[..]; N];
    let mut pos = 0usize;
    let mut col = 0usize;

    while col < N - 1 {
        if pos >= len {
            return None;
        }
        if line[pos] == b'"' {
            let start = pos + 1;
            let off = memchr2(b',', b'"', &line[start..])?;
            if line[start + off] != b'"' {
                return None;
            }
            let end_q = start + off;
            if end_q + 1 >= len || line[end_q + 1] != b',' {
                return None;
            }
            out[col] = &line[start..end_q];
            pos = end_q + 2;
        } else {
            let off = memchr2(b',', b'"', &line[pos..])?;
            if line[pos + off] != b',' {
                return None;
            }
            out[col] = &line[pos..pos + off];
            pos += off + 1;
        }
        col += 1;
    }

    if pos < len && line[pos] == b'"' {
        let start = pos + 1;
        let off = memchr2(b',', b'"', &line[start..])?;
        if line[start + off] != b'"' || start + off + 1 != len {
            return None;
        }
        out[N - 1] = &line[start..start + off];
    } else {
        if memchr2(b',', b'"', &line[pos..]).is_some() {
            return None;
        }
        out[N - 1] = &line[pos..len];
    }
    Some(out)
}

#[inline]
fn parse_2(b: &[u8]) -> Option<u32> {
    let d0 = b[0].wrapping_sub(b'0');
    let d1 = b[1].wrapping_sub(b'0');
    if d0 <= 9 && d1 <= 9 {
        Some((d0 as u32) * 10 + (d1 as u32))
    } else {
        None
    }
}

#[inline]
fn parse_4(b: &[u8]) -> Option<i64> {
    let d0 = b[0].wrapping_sub(b'0');
    let d1 = b[1].wrapping_sub(b'0');
    let d2 = b[2].wrapping_sub(b'0');
    let d3 = b[3].wrapping_sub(b'0');
    if d0 <= 9 && d1 <= 9 && d2 <= 9 && d3 <= 9 {
        Some((d0 as i64) * 1000 + (d1 as i64) * 100 + (d2 as i64) * 10 + (d3 as i64))
    } else {
        None
    }
}

#[inline]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

#[inline]
fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

#[inline]
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = y - if m <= 2 { 1 } else { 0 };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400) as u64;
    let m = m as u64;
    let d = d as u64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

#[derive(Clone, Copy)]
struct DateCache {
    date: [u8; 10],
    day_ms: i64,
}

impl DateCache {
    fn new() -> Self {
        Self {
            date: [0; 10],
            day_ms: 0,
        }
    }

    #[inline]
    fn parse_iso_prefix(&mut self, b: &[u8]) -> Option<i64> {
        if b[10] != b'T' || b[13] != b':' || b[16] != b':' {
            return None;
        }
        let hour = parse_2(&b[11..13])?;
        let min = parse_2(&b[14..16])?;
        let sec = parse_2(&b[17..19])?;
        if hour > 23 || min > 59 || sec > 59 {
            return None;
        }
        let day_ms = if &b[0..10] == &self.date {
            self.day_ms
        } else {
            if b[4] != b'-' || b[7] != b'-' {
                return None;
            }
            let year = parse_4(&b[0..4])?;
            let month = parse_2(&b[5..7])?;
            let day = parse_2(&b[8..10])?;
            if month < 1 || month > 12 || day < 1 || day > days_in_month(year, month) {
                return None;
            }
            let ms = days_from_civil(year, month, day) * MS_PER_DAY;
            self.date.copy_from_slice(&b[0..10]);
            self.day_ms = ms;
            ms
        };
        Some(day_ms + ((hour as i64) * 3600 + (min as i64) * 60 + (sec as i64)) * 1000)
    }

    #[inline]
    fn parse_f1(&mut self, b: &[u8]) -> Option<i64> {
        if b.len() != 20 || b[19] != b'Z' {
            return None;
        }
        self.parse_iso_prefix(b)
    }

    #[inline]
    fn parse_ts(&mut self, b: &[u8]) -> Option<i64> {
        match b.len() {
            0 => None,
            20 if b[19] == b'Z' => self.parse_iso_prefix(b),
            25 if (b[19] == b'+' || b[19] == b'-') && b[22] == b':' => {
                let base = self.parse_iso_prefix(b)?;
                let oh = parse_2(&b[20..22])?;
                let om = parse_2(&b[23..25])?;
                if oh > 23 || om > 59 {
                    return None;
                }
                let off_ms = ((oh as i64) * 3600 + (om as i64) * 60) * 1000;
                if b[19] == b'+' {
                    Some(base - off_ms)
                } else {
                    Some(base + off_ms)
                }
            }
            len if len <= 18 => {
                let mut val: i64 = 0;
                for &c in b {
                    let d = c.wrapping_sub(b'0');
                    if d > 9 {
                        return None;
                    }
                    val = val * 10 + (d as i64);
                }
                Some(val)
            }
            _ => {
                let mut val: i64 = 0;
                let mut overflow = false;
                for &c in b {
                    let d = c.wrapping_sub(b'0');
                    if d > 9 {
                        return None;
                    }
                    if !overflow {
                        match val.checked_mul(10).and_then(|v| v.checked_add(d as i64)) {
                            Some(v) => val = v,
                            None => overflow = true,
                        }
                    }
                }
                if overflow {
                    Some(i64::MAX)
                } else {
                    Some(val)
                }
            }
        }
    }
}

#[inline]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut val: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if val <= 10_000_000 {
            val = val * 10 + (d as u64);
        }
    }
    if val >= 1 && val <= 10_000_000 {
        Some(val as u32)
    } else {
        None
    }
}

fn mmap_csv_dir(dir: &Path) -> Vec<Mmap> {
    let mut paths: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("csv"))
            .collect(),
        Err(_) => Vec::new(),
    };
    paths.sort();
    let mut mmaps = Vec::with_capacity(paths.len());
    for p in paths {
        if let Ok(f) = File::open(&p) {
            if let Ok(meta) = f.metadata() {
                if meta.len() > 0 {
                    if let Ok(m) = unsafe { Mmap::map(&f) } {
                        let _ = m.advise(Advice::Sequential);
                        mmaps.push(m);
                    }
                }
            }
        }
    }
    mmaps
}

fn split_chunks<'a>(mmaps: &'a [Mmap], chunk_bytes: usize) -> Vec<&'a [u8]> {
    let mut chunks = Vec::new();
    for m in mmaps {
        let data = &m[..];
        let Some(nl) = memchr(b'\n', data) else {
            continue;
        };
        let mut pos = nl + 1;
        let len = data.len();
        while pos < len {
            let mut end = (pos + chunk_bytes).min(len);
            if end < len {
                match memchr(b'\n', &data[end..]) {
                    Some(off) => end += off + 1,
                    None => end = len,
                }
            }
            chunks.push(&data[pos..end]);
            pos = end;
        }
    }
    chunks
}

struct LineIter<'a>(&'a [u8]);
impl<'a> Iterator for LineIter<'a> {
    type Item = &'a [u8];
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        match memchr(b'\n', self.0) {
            Some(i) => {
                let line = &self.0[..i];
                self.0 = &self.0[i + 1..];
                Some(line)
            }
            None => {
                let line = self.0;
                self.0 = &[];
                Some(line)
            }
        }
    }
}

/// Compact 8-byte open-addressed link resolver with precomputed canonical hashes.
struct LinkResolver<'a> {
    table: Vec<(u32, u32)>,
    mask: usize,
    keys: Vec<&'a [u8]>,
    canon: Vec<(&'a [u8], u64)>,
}

impl<'a> LinkResolver<'a> {
    fn build(mmaps: &'a [Mmap]) -> Self {
        let chunks = split_chunks(mmaps, 2 * 1024 * 1024);
        let pairs: Vec<Vec<(&'a [u8], &'a [u8])>> = chunks
            .into_par_iter()
            .map(|ch| {
                let mut v = Vec::new();
                for line in LineIter(ch) {
                    if let Some([a, b]) = parse_csv_line::<2>(line) {
                        if !a.is_empty() && !b.is_empty() {
                            v.push((a, b));
                        }
                    }
                }
                v
            })
            .collect();

        let total: usize = pairs.iter().map(|v| v.len()).sum();
        if total == 0 {
            return LinkResolver {
                table: Vec::new(),
                mask: 0,
                keys: Vec::new(),
                canon: Vec::new(),
            };
        }

        let mut map: FxHashMap<&'a [u8], u32> =
            FxHashMap::with_capacity_and_hasher(total, Default::default());
        let mut keys: Vec<&'a [u8]> = Vec::with_capacity(total);
        let mut parent: Vec<u32> = Vec::with_capacity(total);
        let mut min_name: Vec<&'a [u8]> = Vec::with_capacity(total);

        #[inline]
        fn find(parent: &mut [u32], mut x: u32) -> u32 {
            while parent[x as usize] != x {
                parent[x as usize] = parent[parent[x as usize] as usize];
                x = parent[x as usize];
            }
            x
        }

        for batch in pairs {
            for (a, b) in batch {
                let ia = match map.get(a) {
                    Some(&i) => i,
                    None => {
                        let i = parent.len() as u32;
                        map.insert(a, i);
                        keys.push(a);
                        parent.push(i);
                        min_name.push(a);
                        i
                    }
                };
                let ib = match map.get(b) {
                    Some(&i) => i,
                    None => {
                        let i = parent.len() as u32;
                        map.insert(b, i);
                        keys.push(b);
                        parent.push(i);
                        min_name.push(b);
                        i
                    }
                };
                let ra = find(&mut parent, ia);
                let rb = find(&mut parent, ib);
                if ra != rb {
                    parent[rb as usize] = ra;
                    if min_name[rb as usize] < min_name[ra as usize] {
                        min_name[ra as usize] = min_name[rb as usize];
                    }
                }
            }
        }
        drop(map);

        let n = parent.len();
        let cap = (n * 2).next_power_of_two().max(16);
        let mask = cap - 1;
        let mut table: Vec<(u32, u32)> = vec![(0, u32::MAX); cap];
        let mut canon: Vec<(&'a [u8], u64)> = Vec::with_capacity(n);

        for i in 0..n {
            let r = find(&mut parent, i as u32);
            let cname = min_name[r as usize];
            canon.push((cname, hash_bytes(cname)));

            let kh = hash_bytes(keys[i]);
            let h32 = (kh >> 32) as u32;
            let mut slot = (kh as usize) & mask;
            while table[slot].1 != u32::MAX {
                slot = (slot + 1) & mask;
            }
            table[slot] = (h32, i as u32);
        }

        LinkResolver {
            table,
            mask,
            keys,
            canon,
        }
    }

    #[inline]
    fn resolve_with_hash(&self, uid: &'a [u8]) -> (&'a [u8], u64) {
        let h = hash_bytes(uid);
        if self.table.is_empty() {
            return (uid, h);
        }
        let h32 = (h >> 32) as u32;
        let mut slot = (h as usize) & self.mask;
        loop {
            let (th, idx) = unsafe { *self.table.get_unchecked(slot) };
            if idx == u32::MAX {
                return (uid, h);
            }
            if th == h32 && unsafe { *self.keys.get_unchecked(idx as usize) } == uid {
                return unsafe { *self.canon.get_unchecked(idx as usize) };
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

#[derive(Clone, Copy)]
struct RawEvent<'a> {
    eid_hash: u64,
    event_id: &'a [u8],
    user_id: &'a [u8],
    ts: i64,
    ingest_ts: i64,
    price_cents: u32,
    cat_id: u16,
    flags: u8,
}

#[derive(Clone, Copy)]
struct CleanEvent<'a> {
    person_hash: u64,
    person: &'a [u8],
    ts: i64,
    price_cents: u32,
    cat_id: u16,
    is_purchase: u8,
}

#[derive(Clone, Copy, Default)]
struct CompactEv {
    ts: i64,
    price_cents: u32,
    cat_id: u16,
    is_purchase: u8,
}

#[derive(Clone, Copy)]
struct RawClick<'a> {
    cid_hash: u64,
    click_id: &'a [u8],
    user_id: &'a [u8],
    ts: i64,
    camp_id: u16,
}

#[derive(Clone, Copy)]
struct CleanClick<'a> {
    person_hash: u64,
    person: &'a [u8],
    ts: i64,
    click_id: &'a [u8],
    camp_id: u16,
}

#[derive(Clone, Copy)]
struct CompactCk<'a> {
    ts: i64,
    click_id: &'a [u8],
    camp_id: u16,
}

struct Session {
    start_ts: i64,
    day_idx: u16,
    revenue: u64,
    has_purchase: bool,
}

struct UserDailyRow<'a> {
    user_id: &'a [u8],
    day: i32,
    sessions: i64,
    events: i64,
    revenue_cents: i64,
    top_cat: Option<u16>,
}

fn load_products(path: &Path) -> (FxHashMap<Vec<u8>, u16>, Vec<String>, u16) {
    let data = fs::read(path).expect("read products.csv");
    let mut rows: Vec<(Vec<u8>, String)> = Vec::new();
    let mut cats_set: FxHashSet<String> = FxHashSet::default();
    cats_set.insert("unknown".to_string());

    if let Some(nl) = memchr(b'\n', &data) {
        for line in LineIter(&data[nl + 1..]) {
            if let Some([pid, cat]) = parse_csv_line::<2>(line) {
                if !pid.is_empty() {
                    let cat_s = String::from_utf8_lossy(cat).into_owned();
                    cats_set.insert(cat_s.clone());
                    rows.push((pid.to_vec(), cat_s));
                }
            }
        }
    }
    let mut cats: Vec<String> = cats_set.into_iter().collect();
    cats.sort();
    let cat_to_id: FxHashMap<String, u16> = cats
        .iter()
        .enumerate()
        .map(|(i, c)| (c.clone(), i as u16))
        .collect();
    let unknown_id = cat_to_id["unknown"];

    let mut prod_map: FxHashMap<Vec<u8>, u16> =
        FxHashMap::with_capacity_and_hasher(rows.len(), Default::default());
    for (pid, cat_s) in rows {
        prod_map.insert(pid, cat_to_id[&cat_s]);
    }
    (prod_map, cats, unknown_id)
}

fn load_campaigns(path: &Path) -> (FxHashMap<Vec<u8>, u16>, Vec<(String, String)>) {
    let data = fs::read(path).expect("read campaigns.csv");
    let mut camp_list = vec![("organic".to_string(), "organic".to_string())];
    let mut camp_map: FxHashMap<Vec<u8>, u16> = FxHashMap::default();

    if let Some(nl) = memchr(b'\n', &data) {
        for line in LineIter(&data[nl + 1..]) {
            if let Some([cid, ch]) = parse_csv_line::<2>(line) {
                if !cid.is_empty() {
                    let idx = camp_list.len() as u16;
                    camp_list.push((
                        String::from_utf8_lossy(cid).into_owned(),
                        String::from_utf8_lossy(ch).into_owned(),
                    ));
                    camp_map.insert(cid.to_vec(), idx);
                }
            }
        }
    }
    (camp_map, camp_list)
}

fn fast_writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build()
}

fn main() {
    let timing = env::var("TIMING").is_ok();
    let t_total = Instant::now();

    let data_dir = PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let output_dir = PathBuf::from(env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));

    let manifest: Manifest =
        serde_json::from_slice(&fs::read(data_dir.join("manifest.json")).expect("manifest.json"))
            .expect("parse manifest.json");
    let mut mcache = DateCache::new();
    let ws = mcache
        .parse_f1(manifest.window_start.as_bytes())
        .expect("valid window_start");
    let we = mcache
        .parse_f1(manifest.window_end.as_bytes())
        .expect("valid window_end");
    let click_ws = ws - SEVEN_DAYS_MS;
    let ws_day = ws.div_euclid(MS_PER_DAY) as i32;
    let we_day = (we - 1).div_euclid(MS_PER_DAY) as i32;
    let num_days = (we_day - ws_day + 1).max(1) as usize;

    let (prod_map, categories, unknown_cat_id) = load_products(&data_dir.join("products.csv"));
    let (camp_map, camp_list) = load_campaigns(&data_dir.join("campaigns.csv"));
    let num_cats = categories.len();
    let num_camps = camp_list.len();

    let link_mmaps = mmap_csv_dir(&data_dir.join("links"));
    let event_mmaps = mmap_csv_dir(&data_dir.join("events"));
    let click_mmaps = mmap_csv_dir(&data_dir.join("clicks"));

    // 1 & 2. Overlap LinkResolver::build with parallel events_parse
    let t0 = Instant::now();
    let event_chunks = split_chunks(&event_mmaps, 4 * 1024 * 1024);
    let (resolver, raw_event_parts): (LinkResolver, Vec<Vec<Vec<RawEvent>>>) = rayon::join(
        || LinkResolver::build(&link_mmaps),
        || {
            event_chunks
                .into_par_iter()
                .fold(
                    || {
                        (
                            DateCache::new(),
                            DateCache::new(),
                            (0..NUM_PARTS).map(|_| Vec::new()).collect::<Vec<_>>(),
                        )
                    },
                    |(mut ts_cache, mut ing_cache, mut acc), ch| {
                        for line in LineIter(ch) {
                            let Some([eid, uid, ts_b, ing_b, etype, pid, price_b]) =
                                parse_csv_line::<7>(line)
                            else {
                                continue;
                            };
                            if eid.is_empty() {
                                continue;
                            }
                            let Some(ingest_ts) = ing_cache.parse_f1(ing_b) else {
                                continue;
                            };

                            let mut flags = 0u8;
                            let mut ts = 0i64;
                            let mut price_cents = 0u32;
                            let mut cat_id = unknown_cat_id;

                            if !uid.is_empty() && !pid.is_empty() {
                                if let Some(t) = ts_cache.parse_ts(ts_b) {
                                    if t >= ws && t < we && (ingest_ts - t) <= LATE_LIMIT_MS {
                                        match etype {
                                            b"view" | b"cart" => {
                                                ts = t;
                                                flags = 1;
                                            }
                                            b"purchase" => {
                                                if let Some(p) = parse_price(price_b) {
                                                    ts = t;
                                                    price_cents = p;
                                                    cat_id = *prod_map
                                                        .get(pid)
                                                        .unwrap_or(&unknown_cat_id);
                                                    flags = 3;
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }

                            let eid_hash = hash_bytes(eid);
                            let part = (eid_hash as usize) & (NUM_PARTS - 1);
                            acc[part].push(RawEvent {
                                eid_hash,
                                event_id: eid,
                                user_id: uid,
                                ts,
                                ingest_ts,
                                price_cents,
                                cat_id,
                                flags,
                            });
                        }
                        (ts_cache, ing_cache, acc)
                    },
                )
                .map(|(_, _, acc)| acc)
                .collect()
        },
    );
    if timing {
        eprintln!(
            "[phase] links+events_parse: {:.1} ms",
            t0.elapsed().as_secs_f64() * 1e3
        );
    }

    // 3. Dedup events -> compact flat resolve_with_hash -> scatter by person
    let t0 = Instant::now();
    let clean_event_scatters: Vec<Vec<Vec<CleanEvent>>> = (0..NUM_PARTS)
        .into_par_iter()
        .map(|p| {
            let total: usize = raw_event_parts.iter().map(|t| t[p].len()).sum();
            let cap = (total * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            let mut table: Vec<(u32, u32)> = vec![(0, u32::MAX); cap];
            let mut kept: Vec<RawEvent> = Vec::with_capacity(total);

            for t in &raw_event_parts {
                for &ev in &t[p] {
                    let h32 = (ev.eid_hash >> 32) as u32;
                    let mut slot = ((ev.eid_hash >> PART_SHIFT) as usize) & mask;
                    loop {
                        let (th, idx) = unsafe { *table.get_unchecked(slot) };
                        if idx == u32::MAX {
                            let new_idx = kept.len() as u32;
                            unsafe { *table.get_unchecked_mut(slot) = (h32, new_idx) };
                            kept.push(ev);
                            break;
                        }
                        if th == h32 && kept[idx as usize].event_id == ev.event_id {
                            if ev.ingest_ts < kept[idx as usize].ingest_ts {
                                kept[idx as usize] = ev;
                            }
                            break;
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            }
            drop(table);

            let per_p = (kept.len() / NUM_PARTS).max(16);
            let mut out: Vec<Vec<CleanEvent>> = (0..NUM_PARTS)
                .map(|_| Vec::with_capacity(per_p))
                .collect();
            for ev in kept {
                if (ev.flags & 1) != 0 {
                    let (person, ph) = resolver.resolve_with_hash(ev.user_id);
                    let pp = (ph as usize) & (NUM_PARTS - 1);
                    out[pp].push(CleanEvent {
                        person_hash: ph,
                        person,
                        ts: ev.ts,
                        price_cents: ev.price_cents,
                        cat_id: ev.cat_id,
                        is_purchase: ev.flags >> 1,
                    });
                }
            }
            out
        })
        .collect();
    drop(raw_event_parts);
    if timing {
        eprintln!(
            "[phase] events_dedup: {:.1} ms",
            t0.elapsed().as_secs_f64() * 1e3
        );
    }

    // 4. Parse & dedup clicks -> scatter by person
    let t0 = Instant::now();
    let click_chunks = split_chunks(&click_mmaps, 4 * 1024 * 1024);
    let raw_click_parts: Vec<Vec<Vec<RawClick>>> = click_chunks
        .into_par_iter()
        .fold(
            || {
                (
                    DateCache::new(),
                    (0..NUM_PARTS).map(|_| Vec::new()).collect::<Vec<_>>(),
                )
            },
            |(mut dcache, mut acc), ch| {
                for line in LineIter(ch) {
                    let Some([cid, uid, camp_b, ts_b]) = parse_csv_line::<4>(line) else {
                        continue;
                    };
                    if cid.is_empty() || uid.is_empty() {
                        continue;
                    }
                    let Some(&camp_id) = camp_map.get(camp_b) else {
                        continue;
                    };
                    let Some(ts) = dcache.parse_ts(ts_b) else {
                        continue;
                    };
                    if ts < click_ws || ts >= we {
                        continue;
                    }
                    let cid_hash = hash_bytes(cid);
                    let part = (cid_hash as usize) & (NUM_PARTS - 1);
                    acc[part].push(RawClick {
                        cid_hash,
                        click_id: cid,
                        user_id: uid,
                        ts,
                        camp_id,
                    });
                }
                (dcache, acc)
            },
        )
        .map(|(_, acc)| acc)
        .collect();

    let clean_click_scatters: Vec<Vec<Vec<CleanClick>>> = (0..NUM_PARTS)
        .into_par_iter()
        .map(|p| {
            let total: usize = raw_click_parts.iter().map(|t| t[p].len()).sum();
            let cap = (total * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            let mut table: Vec<(u32, u32)> = vec![(0, u32::MAX); cap];
            let mut kept_cids: Vec<&[u8]> = Vec::with_capacity(total);
            let mut out: Vec<Vec<CleanClick>> = (0..NUM_PARTS).map(|_| Vec::new()).collect();

            for t in &raw_click_parts {
                for &ck in &t[p] {
                    let h32 = (ck.cid_hash >> 32) as u32;
                    let mut slot = ((ck.cid_hash >> PART_SHIFT) as usize) & mask;
                    let mut is_new = false;
                    loop {
                        let (th, idx) = unsafe { *table.get_unchecked(slot) };
                        if idx == u32::MAX {
                            let new_idx = kept_cids.len() as u32;
                            unsafe { *table.get_unchecked_mut(slot) = (h32, new_idx) };
                            kept_cids.push(ck.click_id);
                            is_new = true;
                            break;
                        }
                        if th == h32 && kept_cids[idx as usize] == ck.click_id {
                            break;
                        }
                        slot = (slot + 1) & mask;
                    }
                    if is_new {
                        let (person, ph) = resolver.resolve_with_hash(ck.user_id);
                        let pp = (ph as usize) & (NUM_PARTS - 1);
                        out[pp].push(CleanClick {
                            person_hash: ph,
                            person,
                            ts: ck.ts,
                            click_id: ck.click_id,
                            camp_id: ck.camp_id,
                        });
                    }
                }
            }
            out
        })
        .collect();
    drop(raw_click_parts);
    if timing {
        eprintln!(
            "[phase] clicks_all: {:.1} ms",
            t0.elapsed().as_secs_f64() * 1e3
        );
    }

    // 5. Flat person bucketing + L1-cache sessionization & attribution
    let t0 = Instant::now();
    let part_results: Vec<(Vec<UserDailyRow>, Vec<[u64; 4]>)> = (0..NUM_PARTS)
        .into_par_iter()
        .map(|p| {
            let ev_len: usize = clean_event_scatters.iter().map(|s| s[p].len()).sum();
            let est_persons = (ev_len / 12).max(16);
            let cap = (est_persons * 2).next_power_of_two().max(32);
            let mut mask = cap - 1;
            let mut ptable: Vec<(u32, u32)> = vec![(0, u32::MAX); cap];
            let mut person_names: Vec<&[u8]> = Vec::with_capacity(est_persons);
            let mut person_hashes: Vec<u64> = Vec::with_capacity(est_persons);
            let mut ev_counts: Vec<u32> = Vec::with_capacity(est_persons);
            let mut ev_pids: Vec<u32> = Vec::with_capacity(ev_len);

            for s in &clean_event_scatters {
                for ev in &s[p] {
                    if person_names.len() * 2 >= ptable.len() {
                        let new_cap = ptable.len() * 2;
                        mask = new_cap - 1;
                        let mut new_t = vec![(0u32, u32::MAX); new_cap];
                        for (id, &ph) in person_hashes.iter().enumerate() {
                            let h32 = (ph >> 32) as u32;
                            let mut sl = ((ph >> PART_SHIFT) as usize) & mask;
                            while new_t[sl].1 != u32::MAX {
                                sl = (sl + 1) & mask;
                            }
                            new_t[sl] = (h32, id as u32);
                        }
                        ptable = new_t;
                    }

                    let h32 = (ev.person_hash >> 32) as u32;
                    let mut slot = ((ev.person_hash >> PART_SHIFT) as usize) & mask;
                    let pid = loop {
                        let (th, id) = unsafe { *ptable.get_unchecked(slot) };
                        if id == u32::MAX {
                            let new_id = person_names.len() as u32;
                            unsafe { *ptable.get_unchecked_mut(slot) = (h32, new_id) };
                            person_names.push(ev.person);
                            person_hashes.push(ev.person_hash);
                            ev_counts.push(1);
                            break new_id;
                        }
                        if th == h32 && person_names[id as usize] == ev.person {
                            ev_counts[id as usize] += 1;
                            break id;
                        }
                        slot = (slot + 1) & mask;
                    };
                    ev_pids.push(pid);
                }
            }
            drop(person_hashes);

            let num_persons = person_names.len();
            let mut ev_offsets: Vec<usize> = Vec::with_capacity(num_persons + 1);
            let mut acc = 0usize;
            for &c in &ev_counts {
                ev_offsets.push(acc);
                acc += c as usize;
            }
            ev_offsets.push(acc);

            let mut ev_write = ev_offsets[..num_persons].to_vec();
            let mut flat_evs: Vec<CompactEv> = vec![CompactEv::default(); ev_len];
            let mut idx = 0usize;
            for s in &clean_event_scatters {
                for ev in &s[p] {
                    let pid = ev_pids[idx] as usize;
                    idx += 1;
                    let pos = ev_write[pid];
                    ev_write[pid] = pos + 1;
                    flat_evs[pos] = CompactEv {
                        ts: ev.ts,
                        price_cents: ev.price_cents,
                        cat_id: ev.cat_id,
                        is_purchase: ev.is_purchase,
                    };
                }
            }
            drop(ev_pids);
            drop(ev_write);
            drop(ev_counts);

            let mut ck_counts: Vec<u32> = vec![0; num_persons];
            let mut ck_total = 0usize;
            let lookup_pid = |ph: u64, person: &[u8]| -> Option<u32> {
                let h32 = (ph >> 32) as u32;
                let mut slot = ((ph >> PART_SHIFT) as usize) & mask;
                loop {
                    let (th, id) = unsafe { *ptable.get_unchecked(slot) };
                    if id == u32::MAX {
                        return None;
                    }
                    if th == h32 && person_names[id as usize] == person {
                        return Some(id);
                    }
                    slot = (slot + 1) & mask;
                }
            };

            let mut ck_pids: Vec<u32> = Vec::new();
            for s in &clean_click_scatters {
                for ck in &s[p] {
                    if let Some(pid) = lookup_pid(ck.person_hash, ck.person) {
                        ck_counts[pid as usize] += 1;
                        ck_pids.push(pid);
                        ck_total += 1;
                    } else {
                        ck_pids.push(u32::MAX);
                    }
                }
            }
            drop(ptable);

            let mut ck_offsets: Vec<usize> = Vec::with_capacity(num_persons + 1);
            let mut cacc = 0usize;
            for &c in &ck_counts {
                ck_offsets.push(cacc);
                cacc += c as usize;
            }
            ck_offsets.push(cacc);

            let mut ck_write = ck_offsets[..num_persons].to_vec();
            let mut flat_cks: Vec<CompactCk> = vec![
                CompactCk {
                    ts: 0,
                    click_id: &[],
                    camp_id: 0,
                };
                ck_total
            ];
            let mut cidx = 0usize;
            for s in &clean_click_scatters {
                for ck in &s[p] {
                    let pid = ck_pids[cidx];
                    cidx += 1;
                    if pid != u32::MAX {
                        let pos = ck_write[pid as usize];
                        ck_write[pid as usize] = pos + 1;
                        flat_cks[pos] = CompactCk {
                            ts: ck.ts,
                            click_id: ck.click_id,
                            camp_id: ck.camp_id,
                        };
                    }
                }
            }
            drop(ck_pids);
            drop(ck_write);
            drop(ck_counts);

            let mut user_rows: Vec<UserDailyRow> = Vec::with_capacity(num_persons * 3);
            let mut camp_daily: Vec<[u64; 4]> = vec![[0; 4]; num_camps * num_days];

            let mut sessions: Vec<Session> = Vec::new();
            let mut day_bounds: Vec<(usize, usize, usize)> = Vec::new();
            let mut cat_rev: Vec<u64> = vec![0; num_cats];
            let mut touched_cats: Vec<u16> = Vec::new();
            let mut active_camps: Vec<u16> = Vec::new();
            let mut camp_iv: Vec<(i64, i64)> = vec![(0, 0); num_camps];
            let mut camp_active: Vec<bool> = vec![false; num_camps];

            for pid in 0..num_persons {
                let person = person_names[pid];
                let p_evs = &mut flat_evs[ev_offsets[pid]..ev_offsets[pid + 1]];
                if p_evs.len() > 1 {
                    p_evs.sort_unstable_by_key(|e| e.ts);
                }

                let p_cks = &mut flat_cks[ck_offsets[pid]..ck_offsets[pid + 1]];
                if p_cks.len() > 1 {
                    p_cks.sort_unstable_by(|a, b| {
                        a.ts.cmp(&b.ts)
                            .then_with(|| Reverse(a.click_id).cmp(&Reverse(b.click_id)))
                    });
                }

                sessions.clear();
                day_bounds.clear();

                let mut cur_start = p_evs[0].ts;
                let mut cur_last = p_evs[0].ts;
                let mut cur_day = (cur_start.div_euclid(MS_PER_DAY) as i32 - ws_day) as usize;
                let mut s_rev = 0u64;
                let mut s_purch = false;

                let mut ud_day = cur_day;
                let mut ud_sessions = 0i64;
                let mut ud_events = 0i64;
                let mut ud_rev = 0i64;
                let mut ud_has_purch = false;

                let mut db_day = cur_day;
                let mut db_start = 0usize;

                for e in p_evs.iter() {
                    if e.ts - cur_last > SESSION_GAP_MS {
                        sessions.push(Session {
                            start_ts: cur_start,
                            day_idx: cur_day as u16,
                            revenue: s_rev,
                            has_purchase: s_purch,
                        });
                        ud_sessions += 1;

                        let next_day = (e.ts.div_euclid(MS_PER_DAY) as i32 - ws_day) as usize;
                        if next_day != ud_day {
                            let top_cat = if ud_has_purch {
                                let mut best_c = 0u16;
                                let mut best_v = 0u64;
                                for &c in &touched_cats {
                                    let v = cat_rev[c as usize];
                                    if v > best_v || (v == best_v && c < best_c) {
                                        best_v = v;
                                        best_c = c;
                                    }
                                    cat_rev[c as usize] = 0;
                                }
                                touched_cats.clear();
                                Some(best_c)
                            } else {
                                None
                            };
                            user_rows.push(UserDailyRow {
                                user_id: person,
                                day: ws_day + ud_day as i32,
                                sessions: ud_sessions,
                                events: ud_events,
                                revenue_cents: ud_rev,
                                top_cat,
                            });
                            day_bounds.push((db_day, db_start, sessions.len()));
                            db_day = next_day;
                            db_start = sessions.len();

                            ud_day = next_day;
                            ud_sessions = 0;
                            ud_events = 0;
                            ud_rev = 0;
                            ud_has_purch = false;
                        }

                        cur_start = e.ts;
                        cur_day = next_day;
                        s_rev = 0;
                        s_purch = false;
                    }
                    cur_last = e.ts;
                    ud_events += 1;
                    if e.is_purchase != 0 {
                        let pr = e.price_cents as u64;
                        s_rev += pr;
                        s_purch = true;
                        ud_rev += pr as i64;
                        ud_has_purch = true;
                        if cat_rev[e.cat_id as usize] == 0 {
                            touched_cats.push(e.cat_id);
                        }
                        cat_rev[e.cat_id as usize] += pr;
                    }
                }

                sessions.push(Session {
                    start_ts: cur_start,
                    day_idx: cur_day as u16,
                    revenue: s_rev,
                    has_purchase: s_purch,
                });
                ud_sessions += 1;
                let top_cat = if ud_has_purch {
                    let mut best_c = 0u16;
                    let mut best_v = 0u64;
                    for &c in &touched_cats {
                        let v = cat_rev[c as usize];
                        if v > best_v || (v == best_v && c < best_c) {
                            best_v = v;
                            best_c = c;
                        }
                        cat_rev[c as usize] = 0;
                    }
                    touched_cats.clear();
                    Some(best_c)
                } else {
                    None
                };
                user_rows.push(UserDailyRow {
                    user_id: person,
                    day: ws_day + ud_day as i32,
                    sessions: ud_sessions,
                    events: ud_events,
                    revenue_cents: ud_rev,
                    top_cat,
                });
                day_bounds.push((db_day, db_start, sessions.len()));

                let mut ptr = 0usize;
                for s in &sessions {
                    while ptr < p_cks.len() && p_cks[ptr].ts <= s.start_ts {
                        ptr += 1;
                    }
                    let camp_id = if ptr > 0 && (s.start_ts - p_cks[ptr - 1].ts) <= SEVEN_DAYS_MS {
                        p_cks[ptr - 1].camp_id as usize
                    } else {
                        0
                    };
                    let slot = &mut camp_daily[camp_id * num_days + s.day_idx as usize];
                    slot[0] += 1;
                    if s.has_purchase {
                        slot[1] += 1;
                        slot[2] += s.revenue;
                    }
                }

                if !p_cks.is_empty() {
                    let flush_iv = |camp_id: usize,
                                    iv_start: i64,
                                    iv_end: i64,
                                    sessions: &[Session],
                                    day_bounds: &[(usize, usize, usize)],
                                    camp_daily: &mut [[u64; 4]]| {
                        let lo = sessions.partition_point(|s| s.start_ts < iv_start);
                        if lo == sessions.len() {
                            return;
                        }
                        let hi = sessions.partition_point(|s| s.start_ts <= iv_end);
                        if lo >= hi {
                            return;
                        }
                        for &(d_idx, d_lo, d_hi) in day_bounds {
                            if d_hi <= lo {
                                continue;
                            }
                            if d_lo >= hi {
                                break;
                            }
                            let ov = hi.min(d_hi) - lo.max(d_lo);
                            camp_daily[camp_id * num_days + d_idx][3] += ov as u64;
                        }
                    };

                    for ck in p_cks.iter() {
                        let c = ck.camp_id as usize;
                        let iv_e = ck.ts + SEVEN_DAYS_MS;
                        if !camp_active[c] {
                            camp_active[c] = true;
                            camp_iv[c] = (ck.ts, iv_e);
                            active_camps.push(ck.camp_id);
                        } else if ck.ts <= camp_iv[c].1 {
                            camp_iv[c].1 = iv_e;
                        } else {
                            let (old_s, old_e) = camp_iv[c];
                            flush_iv(c, old_s, old_e, &sessions, &day_bounds, &mut camp_daily);
                            camp_iv[c] = (ck.ts, iv_e);
                        }
                    }
                    for &c16 in &active_camps {
                        let c = c16 as usize;
                        let (old_s, old_e) = camp_iv[c];
                        flush_iv(c, old_s, old_e, &sessions, &day_bounds, &mut camp_daily);
                        camp_active[c] = false;
                    }
                    active_camps.clear();
                }
            }

            (user_rows, camp_daily)
        })
        .collect();
    if timing {
        eprintln!(
            "[phase] sessionize_agg: {:.1} ms",
            t0.elapsed().as_secs_f64() * 1e3
        );
    }

    // 6. Merge campaign_daily & write Parquet in parallel
    let t0 = Instant::now();
    let mut total_camp = vec![[0u64; 4]; num_camps * num_days];
    let mut total_user_rows = 0usize;
    for (urows, cd) in &part_results {
        total_user_rows += urows.len();
        for (dst, src) in total_camp.iter_mut().zip(cd.iter()) {
            dst[0] += src[0];
            dst[1] += src[1];
            dst[2] += src[2];
            dst[3] += src[3];
        }
    }

    let ud_dir = output_dir.join("user_daily");
    let cd_dir = output_dir.join("campaign_daily");
    fs::create_dir_all(&ud_dir).expect("mkdir user_daily");
    fs::create_dir_all(&cd_dir).expect("mkdir campaign_daily");

    rayon::join(
        || write_user_daily_parallel(&ud_dir, &part_results, total_user_rows, &categories),
        || {
            write_campaign_daily(
                &cd_dir.join("part-0.parquet"),
                &total_camp,
                &camp_list,
                ws_day,
                num_days,
            )
        },
    );
    if timing {
        eprintln!(
            "[phase] parquet_write: {:.1} ms",
            t0.elapsed().as_secs_f64() * 1e3
        );
        eprintln!("[phase] TOTAL: {:.1} ms", t_total.elapsed().as_secs_f64() * 1e3);
    }
}

fn write_user_daily_parallel(
    dir: &Path,
    part_results: &[(Vec<UserDailyRow>, Vec<[u64; 4]>)],
    total_rows: usize,
    categories: &[String],
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));

    if total_rows == 0 {
        let file = File::create(dir.join("part-0.parquet")).expect("create empty user_daily");
        let writer =
            ArrowWriter::try_new(file, schema, Some(fast_writer_props())).expect("ArrowWriter");
        writer.close().expect("close empty user_daily");
        return;
    }

    let parts_per_shard = (part_results.len() + WRITE_SHARDS - 1) / WRITE_SHARDS;
    (0..WRITE_SHARDS).into_par_iter().for_each(|shard| {
        let p_start = shard * parts_per_shard;
        let p_end = ((shard + 1) * parts_per_shard).min(part_results.len());
        if p_start >= p_end {
            return;
        }
        let shard_rows: usize = part_results[p_start..p_end]
            .iter()
            .map(|(u, _)| u.len())
            .sum();
        if shard_rows == 0 {
            return;
        }

        let mut uids = Vec::with_capacity(shard_rows);
        let mut days = Vec::with_capacity(shard_rows);
        let mut sess = Vec::with_capacity(shard_rows);
        let mut evts = Vec::with_capacity(shard_rows);
        let mut revs = Vec::with_capacity(shard_rows);
        let mut cats = Vec::with_capacity(shard_rows);

        for (urows, _) in &part_results[p_start..p_end] {
            for r in urows {
                uids.push(std::str::from_utf8(r.user_id).unwrap_or(""));
                days.push(r.day);
                sess.push(r.sessions);
                evts.push(r.events);
                revs.push(r.revenue_cents);
                cats.push(r.top_cat.map(|c| categories[c as usize].as_str()));
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(uids)),
                Arc::new(Date32Array::from(days)),
                Arc::new(Int64Array::from(sess)),
                Arc::new(Int64Array::from(evts)),
                Arc::new(Int64Array::from(revs)),
                Arc::new(StringArray::from(cats)),
            ],
        )
        .expect("user_daily batch");

        let path = dir.join(format!("part-{shard}.parquet"));
        let file = File::create(&path).expect("create user_daily shard");
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(fast_writer_props()))
            .expect("ArrowWriter");
        writer.write(&batch).expect("write user_daily shard");
        writer.close().expect("close user_daily shard");
    });
}

fn write_campaign_daily(
    path: &Path,
    total_camp: &[[u64; 4]],
    camp_list: &[(String, String)],
    ws_day: i32,
    num_days: usize,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));

    let mut cids = Vec::new();
    let mut chs = Vec::new();
    let mut days = Vec::new();
    let mut sess = Vec::new();
    let mut psess = Vec::new();
    let mut revs = Vec::new();
    let mut assists = Vec::new();

    for (c, (cid, ch)) in camp_list.iter().enumerate() {
        for d in 0..num_days {
            let v = total_camp[c * num_days + d];
            if v[0] > 0 || v[3] > 0 {
                cids.push(cid.as_str());
                chs.push(ch.as_str());
                days.push(ws_day + d as i32);
                sess.push(v[0] as i64);
                psess.push(v[1] as i64);
                revs.push(v[2] as i64);
                assists.push(v[3] as i64);
            }
        }
    }

    let n = cids.len();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(cids)),
            Arc::new(StringArray::from(chs)),
            Arc::new(Date32Array::from(days)),
            Arc::new(Int64Array::from(sess)),
            Arc::new(Int64Array::from(psess)),
            Arc::new(Int64Array::from(revs)),
            Arc::new(Int64Array::from(assists)),
        ],
    )
    .expect("campaign_daily batch");

    let file = File::create(path).expect("create campaign_daily parquet");
    let mut writer =
        ArrowWriter::try_new(file, schema, Some(fast_writer_props())).expect("ArrowWriter");
    if n > 0 {
        writer.write(&batch).expect("write campaign_daily");
    }
    writer.close().expect("close campaign_daily");
}
