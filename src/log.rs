//! A partition's append-only commit log, split into segment files.
//!
//! On-disk record layout (big endian):
//! ```text
//! [len u32][crc32 u32][offset u64][timestamp i64][key_len i32][key bytes][value bytes]
//!           \__________________ crc covers everything after the crc ________________/
//! ```
//! Segments are named `<base offset, 20 digits>.log`. A sparse in-memory index maps
//! offsets to byte positions every `index_interval` bytes and is rebuilt on open.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub struct LogConfig {
    pub segment_bytes: u64,
    pub retention_bytes: Option<u64>,
    pub index_interval: u64,
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig { segment_bytes: 8 * 1024 * 1024, retention_bytes: None, index_interval: 4096 }
    }
}

const HEADER: usize = 4 + 4 + 8 + 8 + 4;

pub fn crc32(data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let t = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, slot) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            }
            *slot = c;
        }
        t
    });
    !data.iter().fold(!0u32, |c, &b| t[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8))
}

fn encode(r: &Record) -> Vec<u8> {
    let key_len = r.key.as_ref().map_or(-1, |k| k.len() as i32);
    let mut body = Vec::with_capacity(HEADER + r.value.len());
    body.extend_from_slice(&r.offset.to_be_bytes());
    body.extend_from_slice(&r.timestamp.to_be_bytes());
    body.extend_from_slice(&key_len.to_be_bytes());
    if let Some(k) = &r.key {
        body.extend_from_slice(k);
    }
    body.extend_from_slice(&r.value);
    let mut out = Vec::with_capacity(body.len() + 8);
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(&crc32(&body).to_be_bytes());
    out.extend(body);
    out
}

/// Reads one record; Ok(None) at a clean EOF or a torn/corrupt tail.
fn read_one(r: &mut impl Read) -> io::Result<Option<(Record, u64)>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len) as usize;
    if !(HEADER - 4..=64 * 1024 * 1024).contains(&len) {
        return Ok(None);
    }
    let mut buf = vec![0u8; len];
    if let Err(e) = r.read_exact(&mut buf) {
        return if e.kind() == io::ErrorKind::UnexpectedEof { Ok(None) } else { Err(e) };
    }
    let crc = u32::from_be_bytes(buf[0..4].try_into().unwrap());
    let body = &buf[4..];
    if crc32(body) != crc {
        return Ok(None);
    }
    let offset = u64::from_be_bytes(body[0..8].try_into().unwrap());
    let timestamp = i64::from_be_bytes(body[8..16].try_into().unwrap());
    let key_len = i32::from_be_bytes(body[16..20].try_into().unwrap());
    let rest = &body[20..];
    let (key, value) = if key_len < 0 {
        (None, rest.to_vec())
    } else {
        let k = key_len as usize;
        if k > rest.len() {
            return Ok(None);
        }
        (Some(rest[..k].to_vec()), rest[k..].to_vec())
    };
    Ok(Some((Record { offset, timestamp, key, value }, (len + 4) as u64)))
}

struct Segment {
    base: u64,
    path: PathBuf,
    file: File,
    size: u64,
    next_offset: u64,
    index: Vec<(u64, u64)>,
    bytes_since_index: u64,
}

impl Segment {
    fn path_for(dir: &Path, base: u64) -> PathBuf {
        dir.join(format!("{base:020}.log"))
    }

    fn create(dir: &Path, base: u64) -> io::Result<Self> {
        let path = Self::path_for(dir, base);
        let file = OpenOptions::new().create(true).read(true).append(true).open(&path)?;
        Ok(Segment { base, path, file, size: 0, next_offset: base, index: vec![], bytes_since_index: u64::MAX })
    }

    /// Opens an existing segment, rebuilding the index and truncating any corrupt tail.
    fn open(path: PathBuf, base: u64, interval: u64) -> io::Result<Self> {
        let mut index = Vec::new();
        let mut next_offset = base;
        let mut pos = 0u64;
        let mut since = u64::MAX;
        {
            let mut reader = BufReader::new(File::open(&path)?);
            while let Some((rec, n)) = read_one(&mut reader)? {
                if since >= interval {
                    index.push((rec.offset, pos));
                    since = 0;
                }
                since += n;
                pos += n;
                next_offset = rec.offset + 1;
            }
        }
        if fs::metadata(&path)?.len() != pos {
            // drop a torn write from a crash (append-mode handles cannot truncate on Windows)
            OpenOptions::new().write(true).open(&path)?.set_len(pos)?;
        }
        let file = OpenOptions::new().read(true).append(true).open(&path)?;
        Ok(Segment { base, path, file, size: pos, next_offset, index, bytes_since_index: since })
    }

    fn note_index(&mut self, offset: u64, pos: u64, n: u64, interval: u64) {
        if self.bytes_since_index >= interval {
            self.index.push((offset, pos));
            self.bytes_since_index = 0;
        }
        self.bytes_since_index += n;
    }

    fn position_for(&self, offset: u64) -> u64 {
        match self.index.binary_search_by_key(&offset, |e| e.0) {
            Ok(i) => self.index[i].1,
            Err(0) => 0,
            Err(i) => self.index[i - 1].1,
        }
    }
}

pub struct PartitionLog {
    dir: PathBuf,
    cfg: LogConfig,
    segments: Vec<Segment>,
}

impl PartitionLog {
    pub fn open(dir: impl AsRef<Path>, cfg: LogConfig) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let mut bases: Vec<u64> = fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_str()?.strip_suffix(".log")?.parse().ok())
            .collect();
        bases.sort_unstable();
        let mut segments = Vec::new();
        for b in bases {
            segments.push(Segment::open(Segment::path_for(&dir, b), b, cfg.index_interval)?);
        }
        if segments.is_empty() {
            segments.push(Segment::create(&dir, 0)?);
        }
        Ok(PartitionLog { dir, cfg, segments })
    }

    pub fn next_offset(&self) -> u64 {
        self.segments.last().unwrap().next_offset
    }

    pub fn start_offset(&self) -> u64 {
        self.segments[0].base
    }

    pub fn size_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.size).sum()
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn append(&mut self, key: Option<Vec<u8>>, value: Vec<u8>, timestamp: i64) -> io::Result<u64> {
        let offset = self.next_offset();
        if self.segments.last().unwrap().size >= self.cfg.segment_bytes {
            self.segments.push(Segment::create(&self.dir, offset)?);
            self.enforce_retention()?;
        }
        let bytes = encode(&Record { offset, timestamp, key, value });
        let interval = self.cfg.index_interval;
        let seg = self.segments.last_mut().unwrap();
        seg.file.write_all(&bytes)?;
        let pos = seg.size;
        seg.note_index(offset, pos, bytes.len() as u64, interval);
        seg.size += bytes.len() as u64;
        seg.next_offset = offset + 1;
        Ok(offset)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.segments.last_mut().unwrap().file.sync_data()
    }

    fn enforce_retention(&mut self) -> io::Result<()> {
        let Some(limit) = self.cfg.retention_bytes else { return Ok(()) };
        while self.segments.len() > 1 && self.size_bytes() > limit {
            let old = self.segments.remove(0);
            drop(old.file);
            fs::remove_file(&old.path)?;
        }
        Ok(())
    }

    /// Reads up to `max` records starting at `from` (clamped to the log start).
    pub fn read(&self, from: u64, max: usize) -> io::Result<Vec<Record>> {
        let from = from.max(self.start_offset());
        let mut out = Vec::new();
        let first = self.segments.partition_point(|s| s.base <= from).saturating_sub(1);
        for seg in &self.segments[first..] {
            if out.len() >= max {
                break;
            }
            let mut f = BufReader::new(File::open(&seg.path)?);
            let start = if seg.base <= from { seg.position_for(from) } else { 0 };
            f.seek(SeekFrom::Start(start))?;
            let mut pos = start;
            while pos < seg.size && out.len() < max {
                let Some((rec, n)) = read_one(&mut f)? else { break };
                pos += n;
                if rec.offset >= from {
                    out.push(rec);
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(seg: u64) -> LogConfig {
        LogConfig { segment_bytes: seg, retention_bytes: None, index_interval: 64 }
    }

    #[test]
    fn crc_matches_reference() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn append_read_and_seek() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), cfg(1 << 20)).unwrap();
        for i in 0..500u64 {
            let key = (i % 3 == 0).then(|| format!("k{i}").into_bytes());
            assert_eq!(log.append(key, format!("value-{i}").into_bytes(), i as i64).unwrap(), i);
        }
        let rs = log.read(250, 3).unwrap();
        assert_eq!(rs.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![250, 251, 252]);
        assert_eq!(rs[0].value, b"value-250");
        assert_eq!(rs[1].key, None);
        assert_eq!(log.read(498, 10).unwrap().len(), 2);
        assert!(log.read(500, 10).unwrap().is_empty());
    }

    #[test]
    fn rolls_segments_and_reads_across_them() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), cfg(1000)).unwrap();
        for i in 0..200 {
            log.append(None, vec![i as u8; 40], 0).unwrap();
        }
        assert!(log.segment_count() > 5);
        let all = log.read(0, 1000).unwrap();
        assert_eq!(all.len(), 200);
        assert!(all.windows(2).all(|w| w[1].offset == w[0].offset + 1));
    }

    #[test]
    fn recovers_after_restart_and_torn_write() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = PartitionLog::open(dir.path(), cfg(1 << 20)).unwrap();
            for i in 0..10 {
                log.append(Some(b"k".to_vec()), format!("m{i}").into_bytes(), 0).unwrap();
            }
        }
        // simulate a crash mid-write: append garbage to the segment
        let seg = dir.path().join(format!("{:020}.log", 0));
        let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
        f.write_all(&[0, 0, 0, 50, 1, 2, 3]).unwrap();
        drop(f);

        let mut log = PartitionLog::open(dir.path(), cfg(1 << 20)).unwrap();
        assert_eq!(log.next_offset(), 10);
        assert_eq!(log.append(None, b"after".to_vec(), 0).unwrap(), 10);
        assert_eq!(log.read(9, 5).unwrap().iter().map(|r| r.value.clone()).collect::<Vec<_>>(), vec![b"m9".to_vec(), b"after".to_vec()]);
    }

    #[test]
    fn retention_deletes_old_segments() {
        let dir = tempfile::tempdir().unwrap();
        let c = LogConfig { segment_bytes: 500, retention_bytes: Some(1500), index_interval: 64 };
        let mut log = PartitionLog::open(dir.path(), c).unwrap();
        for _ in 0..300 {
            log.append(None, vec![7; 30], 0).unwrap();
        }
        assert!(log.start_offset() > 0);
        assert!(log.size_bytes() <= 1500 + 500);
        // reading from 0 is clamped to the earliest retained offset
        assert_eq!(log.read(0, 1).unwrap()[0].offset, log.start_offset());
    }
}
