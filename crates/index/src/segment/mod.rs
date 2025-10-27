use std::{collections::VecDeque, path::PathBuf};

use tokio::{fs, io};

use crate::segment::memory::MemorySegment;

mod disk;
mod memory;

pub struct TieredSegmentMap {
    directory: PathBuf,
    counter: usize,

    memory: VecDeque<memory::MemorySegment>,

    disk: VecDeque<disk::DiskSegment>,
}

impl TieredSegmentMap {
    pub async fn insert_elements<
        'key,
        'buffer,
        I: IntoIterator<Item = (&'key str, I2)>,
        I2: IntoIterator<Item = &'buffer str>,
    >(
        &mut self,
        values: I,
    ) -> Result<(), io::Error> {
        let memory_segment = memory::MemorySegment::new(values);

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
        memory_segment: &MemorySegment,
    ) -> Result<disk::DiskSegment, io::Error> {
        let path = {
            self.counter += 1;

            self.directory.join(format!("{}-segment", self.counter))
        };

        fs::create_dir_all(&path).await?;

        let disk_segment = disk::DiskSegment::create_empty_segment(path).await?;
        disk_segment.flush_memory_segment(memory_segment).await?;

        Ok(disk_segment)
    }

    pub async fn find(
        &mut self,
        key: &str,
        mut limit: Option<usize>,
    ) -> Result<Vec<String>, disk::ResolutionError> {
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

            let new = segment.find(key).await?;

            if let Some(value) = limit {
                limit = Some(value.saturating_sub(new.len()));
            }

            entries.extend(new);
        }

        if let Some(at_most) = at_most {
            if entries.len() > at_most {
                entries.drain(at_most..);
            }
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

        map.insert_elements([("k1", ["v1", "v2"])]).await.unwrap();

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

        map.insert_elements([("bigkey", refs.clone())])
            .await
            .unwrap();

        assert_eq!(map.find("bigkey", None).await.unwrap(), refs);

        // ensure no in-memory segments remain
        assert!(map.memory.is_empty());

        // directory should contain flushed segment files
        let mut entries = vec![];

        let mut read_dir = fs::read_dir(tmp.path()).await.unwrap();

        while let Some(next) = read_dir.next_entry().await.unwrap() {
            entries.push(next);
        }

        assert!(entries.len() > 0);
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

        map.insert_elements([("key", ["v1", "v2", "v3"])])
            .await
            .unwrap();

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

        map.insert_elements([("exists", ["yes"])]).await.unwrap();
        let found = map.find("nope", Some(10)).await.unwrap();
        assert!(found.is_empty());
    }
}
