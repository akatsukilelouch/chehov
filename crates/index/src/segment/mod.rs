use std::{collections::VecDeque, path::PathBuf};
use fxhash::FxHashMap;
use snafu::Snafu;
use tokio::{
    fs::{self, read_dir},
    io,
};

use crate::segment::memory::CachedSegment;

mod disk;
mod memory;

pub use disk::DiskResolutionError;

pub struct TieredSegmentMap {
    pub(super) directory: PathBuf,
    counter: usize,

    memory: VecDeque<memory::CachedSegment>,

    disk: VecDeque<disk::DiskSegment>,
}

#[derive(Debug, Snafu)]
pub enum SegmentMapError {
    #[snafu(transparent)]
    IoError { source: io::Error },

    #[snafu(display("unknown file found in segments directory"))]
    UnknownFile,

    #[snafu(display("file has invalid index"))]
    InvalidIndex,
}

impl TieredSegmentMap {
    pub async fn new(directory: PathBuf) -> Result<Self, SegmentMapError> {
        let mut iter = read_dir(&directory).await?;
        let mut maximum_index = 0usize;
        let mut disk_segments = VecDeque::new();

        while let Some(entry) = iter.next_entry().await? {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(SegmentMapError::UnknownFile);
            };

            if !name.starts_with("seg-") {
                return Err(SegmentMapError::UnknownFile);
            }

            let path_index = name
                .split('-')
                .nth(1)
                .expect("no index found in the segment name")
                .parse::<usize>()
                .map_err(|_| SegmentMapError::InvalidIndex)?;

            if maximum_index < path_index {
                maximum_index = path_index + 1;
            }

            disk_segments.push_back(disk::DiskSegment::open_or_create_segment(entry.path()).await?);
        }

        Ok(Self {
            directory,
            counter: maximum_index,
            memory: VecDeque::new(),
            disk: disk_segments,
        })
    }

    pub async fn insert<K: AsRef<str> + Ord + Eq, B: AsRef<str>>(
        &mut self,
        values: FxHashMap<K, Vec<B>>,
    ) -> Result<(), io::Error> {
        let memory_segment = memory::CachedSegment::new(values);

        if memory_segment.values.len() > 4096 {
            let disk_segment = self.write_segment(&memory_segment).await?;
            self.disk.push_back(disk_segment);
        } else {
            self.memory.push_back(memory_segment);
        }

        Ok(())
    }

    async fn write_segment(
        &mut self,
        memory_segment: &CachedSegment,
    ) -> Result<disk::DiskSegment, io::Error> {
        let path = {
            self.counter += 1;

            self.directory.join(format!("{}-segment", self.counter))
        };

        fs::create_dir_all(&path).await?;

        let disk_segment = disk::DiskSegment::open_or_create_segment(path).await?;
        disk_segment.flush_memory_segment(memory_segment).await?;

        Ok(disk_segment)
    }

    pub async fn find(
        &self,
        key: &str,
        mut limit: Option<usize>,
    ) -> Result<Vec<String>, disk::DiskResolutionError> {
        let at_most = limit;

        if let Some(0) = limit {
            return Ok(vec![]);
        }

        let mut entries = vec![];

        {
            let mut memory = self.memory.iter();

            while limit.map(|x| x > 0).unwrap_or(true) {
                let Some(segment) = memory.next() else {
                    break;
                };

                tracing::trace!(segment = ?(segment as *const CachedSegment).addr(), "trying memory segment");

                let new = segment.find(key);

                if let Some(value) = limit {
                    limit = Some(value.saturating_sub(new.len()));
                }

                entries.extend(new);
            }
        }

        let mut disk = self.disk.iter();

        while limit.map(|x| x > 0).unwrap_or(true) {
            let Some(segment) = disk.next() else {
                break;
            };

            tracing::trace!(segment = ?segment.directory, "trying disk segment");

            let new = segment.find(key).await?;

            if let Some(value) = limit {
                limit = Some(value.saturating_sub(new.len()));
            }

            entries.extend(new);
        }

        if let Some(at_most) = at_most
            && entries.len() > at_most
        {
            entries.drain(at_most..);
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::fs;

    #[tokio::test]
    async fn insert_and_find_in_memory_segment() {
        let tmp = tempdir().unwrap();
        let mut map = TieredSegmentMap {
            directory: tmp.path().to_path_buf(),
            counter: 0,
            memory: VecDeque::new(),
            disk: VecDeque::new(),
        };

        let mut entries = FxHashMap::default();
        entries.insert("k1", vec!["v1", "v2"]);

        map.insert(entries).await.unwrap();

        let found = map.find("k1", Some(10)).await.unwrap();
        assert_eq!(found, ["v1", "v2"]);
    }

    #[tokio::test]
    async fn insert_large_segment_goes_to_disk() {
        let tmp = tempdir().unwrap();
        fs::create_dir_all(tmp.path()).await.unwrap();

        let mut map = TieredSegmentMap {
            directory: tmp.path().to_path_buf(),
            counter: 0,
            memory: VecDeque::new(),
            disk: VecDeque::new(),
        };

        // simulate 4097 unique values -> should flush to disk
        let values: Vec<String> = (0..4097).map(|i| format!("val{i}")).collect();
        let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();

        let mut entries = FxHashMap::default();
        entries.insert("bigkey", refs.clone());

        map.insert(entries).await.unwrap();

        assert_eq!(map.find("bigkey", None).await.unwrap(), refs);

        // ensure no in-memory segments remain
        assert!(map.memory.is_empty());

        // directory should contain flushed segment files
        let mut entries = vec![];
        let mut read_dir = fs::read_dir(tmp.path()).await.unwrap();

        while let Some(next) = read_dir.next_entry().await.unwrap() {
            entries.push(next);
        }

        assert!(!entries.is_empty());
    }

    #[tokio::test]
    async fn find_limits_results() {
        let tmp = tempdir().unwrap();
        let mut map = TieredSegmentMap {
            directory: tmp.path().to_path_buf(),
            counter: 0,
            memory: VecDeque::new(),
            disk: VecDeque::new(),
        };

        let mut entries = FxHashMap::default();
        entries.insert("key", vec!["v1", "v2", "v3"]);

        map.insert(entries).await.unwrap();

        let found = map.find("key", Some(2)).await.unwrap();
        assert_eq!(found.len(), 2);
    }

    #[tokio::test]
    async fn find_nonexistent_returns_empty() {
        let tmp = tempdir().unwrap();
        let mut map = TieredSegmentMap {
            directory: tmp.path().to_path_buf(),
            counter: 0,
            memory: VecDeque::new(),
            disk: VecDeque::new(),
        };

        let mut entries = FxHashMap::default();
        entries.insert("exists", vec!["yes"]);

        map.insert(entries).await.unwrap();

        let found = map.find("nope", Some(10)).await.unwrap();
        assert!(found.is_empty());
    }
}
