use std::{
    backtrace::Backtrace,
    cmp::Ordering,
    io::{ErrorKind, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::Command,
};

use bitflags::bitflags;
use bloomfilter::Bloom;
use snafu::Snafu;
use tokio::{
    fs::{self, File},
    io::{self, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt, BufWriter},
};

use super::memory::{Entry, CachedSegment};

pub struct DiskSegment {
    directory: PathBuf,
}

impl DiskSegment {
    async fn write_lookup_table(
        &self,
        prefix: &str,
        offsets: impl IntoIterator<Item = u64>,
    ) -> Result<(), io::Error> {
        let file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.directory.join(format!("{prefix}.lookup.bin")))
            .await?;

        let mut file = BufWriter::new(file);

        for item in offsets {
            file.write_u64(item).await?;
        }

        file.flush().await?;

        Ok(())
    }

    async fn write_full_table<'entry>(
        &self,
        prefix: &str,
        table: impl IntoIterator<Item = &'entry Entry, IntoIter: ExactSizeIterator>,
    ) -> Result<(), io::Error> {
        let file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.directory.join(format!("{prefix}.data.bin")))
            .await?;

        let mut file = BufWriter::new(file);

        let table = table.into_iter();

        let mut offsets = Vec::with_capacity(table.len());

        for item in table {
            let position = file.stream_position().await?;

            match item {
                Entry::Compressed(buffer) => {
                    bitflags! {
                        struct EntryFlag: u32 {
                            const COMPRESSED = 0b1 << size_of::<u32>() * 8 - 1;
                        }
                    }

                    let size = EntryFlag::COMPRESSED.bits() | buffer.len() as u32;

                    file.write_u32(size).await?;
                    file.write_all(buffer).await?;
                }

                Entry::Uncompressed(buffer) => {
                    let buffer = buffer.as_bytes();

                    file.write_u32(buffer.len() as u32).await?;
                    file.write_all(buffer).await?;
                }
            }

            offsets.push(position);
        }

        file.flush().await?;

        self.write_lookup_table(prefix, offsets).await
    }

    async fn write_bloom_filter(&self, filter: &Bloom<str>) -> Result<(), io::Error> {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.directory.join("bloom.bin"))
            .await?;

        file.write_all(&filter.as_slice()).await
    }

    async fn write_entries(
        &self,
        entries: impl IntoIterator<Item = (u32, u32)>,
    ) -> Result<(), io::Error> {
        let file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.directory.join("entries.bin"))
            .await?;

        let mut file = BufWriter::new(file);

        for (key, value) in entries {
            file.write_u32(key).await?;
            file.write_u32(value).await?;
        }

        file.flush().await?;

        Ok(())
    }

    pub async fn flush_memory_segment(&self, segment: &CachedSegment) -> Result<(), io::Error> {
        self.write_full_table("keys", segment.keys.iter()).await?;
        self.write_full_table("values", segment.values.iter())
            .await?;

        self.write_bloom_filter(&segment.bloom).await?;

        self.write_entries(segment.entries.iter().cloned()).await?;

        Ok(())
    }

    #[inline]
    pub async fn open_or_create_segment(directory: PathBuf) -> Result<Self, io::Error> {
        Ok(Self { directory })
    }
}

#[derive(Debug, Snafu)]
pub enum DiskResolutionError {
    #[snafu(display("segment lookup table is of invalid size"))]
    LookupInvalidSize,

    #[snafu(display("segment data table is of invalid size"))]
    DataInvalidSize,

    #[snafu(display("can't load bloom"))]
    BloomLoadError,

    #[snafu(transparent)]
    Utf8Error { source: std::str::Utf8Error },

    #[snafu(transparent)]
    IoError {
        source: io::Error,
        backtrace: Backtrace,
    },
}

fn length(length: u64, factor: usize) -> u32 {
    (length / factor as u64) as u32
}

fn convert(index: u32, factor: usize) -> u64 {
    index as u64 * factor as u64
}

struct LinearMappedResolver {
    pub data: File,
    pub lookup: File,
    pub length: u64,
}
async fn read_offset(lookup: &mut File, offset: u64) -> Result<u64, DiskResolutionError> {
    lookup.seek(SeekFrom::Start(offset)).await?;

    Ok(lookup.read_u64().await?)
}

async fn read_entry_within(data: &mut File, offset: u64) -> Result<String, DiskResolutionError> {
    data.seek(SeekFrom::Start(offset)).await?;

    let length_and_flag = data.read_u32().await? as usize;
    let compressed = (length_and_flag & (0b1 << 31)) != 0;
    let length = length_and_flag & !(0b1 << 31);

    let mut buffer = vec![0u8; length];
    data.read_exact(&mut buffer).await?;

    let buffer = if compressed {
        Entry::Compressed(buffer)
    } else {
        Entry::Uncompressed(String::try_from(buffer).map_err(|err| err.utf8_error())?)
    };

    Ok(buffer.into_uncompressed())
}

impl LinearMappedResolver {
    pub async fn map_to_index(&mut self, key: &str) -> Result<Option<u32>, DiskResolutionError> {
        if self.length % size_of::<u64>() as u64 != 0 {
            return Err(DiskResolutionError::LookupInvalidSize);
        }

        let size = length(self.length, size_of::<u64>());
        let mut current = size / 2;

        for step in 2..self.length.ilog2() + 1 {
            let offset = read_offset(&mut self.lookup, convert(current, size_of::<u64>())).await?;

            let entry = read_entry_within(&mut self.data, offset).await?;

            match key.cmp(&entry) {
                Ordering::Less => {
                    let value = (size / 2u32.pow(step)).max(1);

                    current -= value;
                }
                Ordering::Greater => {
                    let value = (size / 2u32.pow(step)).max(1);

                    current += value
                }
                Ordering::Equal => {
                    return Ok(Some(current));
                }
            }
        }

        Ok(None)
    }

    pub async fn get_value_under(&mut self, index: u32) -> Result<String, DiskResolutionError> {
        if self.length % size_of::<u64>() as u64 != 0 {
            return Err(DiskResolutionError::LookupInvalidSize);
        }

        let offset = read_offset(&mut self.lookup, convert(index, size_of::<u64>())).await?;

        let entry = read_entry_within(&mut self.data, offset).await?;

        Ok(entry)
    }
}

struct EntriesAndLinearMappedValueResolver {
    pub values: LinearMappedResolver,
    pub entries: File,
    pub length: u64,
}

impl EntriesAndLinearMappedValueResolver {
    async fn read_sequential(&mut self, key: u32) -> Result<Vec<String>, DiskResolutionError> {
        let mut items = Vec::new();

        loop {
            let index = match self.entries.read_u32().await {
                Err(err) => {
                    if err.kind() == ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(err.into());
                    }
                }
                Ok(index) => index,
            };

            if index != key {
                break;
            }

            let value_index = self.entries.read_u32().await?;

            items.push(self.values.get_value_under(value_index).await?);
        }

        Ok(items)
    }

    pub async fn resolve_entries_with_key(
        mut self,
        key: u32,
    ) -> Result<Vec<String>, DiskResolutionError> {
        if self.length % (size_of::<u32>() * 2) as u64 != 0 {
            return Err(DiskResolutionError::LookupInvalidSize);
        }

        let size = length(self.length, size_of::<u32>() * 2);

        self.entries
            .seek(SeekFrom::Start(convert(size, size_of::<u32>() * 2)))
            .await?;

        let mut offset = size / 2;

        for step in 2..self.length.ilog2() + 1 {
            let pos = self
                .entries
                .seek(SeekFrom::Start(convert(offset, size_of::<u32>() * 2)))
                .await?;

            let index = self.entries.read_u32().await?;
            match key.cmp(&index) {
                Ordering::Less => {
                    offset -= (size / 2u32.pow(step)).max(1);
                    continue;
                }
                Ordering::Greater => {
                    offset += (size / 2u32.pow(step)).max(1);
                    continue;
                }
                _ => (),
            }

            while offset > 0 {
                self.entries
                    .seek(SeekFrom::Start(convert(offset - 1, size_of::<[u32; 2]>())))
                    .await?;

                let index = self.entries.read_u32().await?;

                if index != key {
                    break;
                } else {
                    offset -= 1;
                }
            }

            self.entries
                .seek(SeekFrom::Start(convert(offset, size_of::<[u32; 2]>())))
                .await?;

            return Ok(self.read_sequential(key).await?);
        }

        Ok(vec![])
    }
}

impl DiskSegment {
    pub async fn find(&self, key: &str) -> Result<Vec<String>, DiskResolutionError> {
        let contains = {
            let mut bloom = File::open(self.directory.join("bloom.bin")).await?;
            let mut buffer = Vec::with_capacity(4096);
            bloom.read_to_end(&mut buffer).await?;
            let bloom =
                Bloom::<str>::from_bytes(buffer).map_err(|_| DiskResolutionError::BloomLoadError)?;

            bloom.check(key)
        };

        if !contains {
            return Ok(vec![]);
        }

        let resolved_key = {
            let keys_lookup_file = File::open(self.directory.join("keys.lookup.bin")).await?;

            LinearMappedResolver {
                data: File::open(self.directory.join("keys.data.bin")).await?,
                length: keys_lookup_file.metadata().await?.len(),
                lookup: keys_lookup_file,
            }
            .map_to_index(key)
            .await?
        };

        let Some(key_index) = resolved_key else {
            return Ok(vec![]);
        };

        let values = {
            let entries_file = File::open(self.directory.join("entries.bin")).await?;

            let values_lookup_file = File::open(self.directory.join("values.lookup.bin")).await?;
            let values_lookup_length = values_lookup_file.metadata().await?.len();
            EntriesAndLinearMappedValueResolver {
                values: LinearMappedResolver {
                    lookup: values_lookup_file,
                    length: values_lookup_length,
                    data: File::open(self.directory.join("values.data.bin")).await?,
                },
                length: entries_file.metadata().await?.len(),
                entries: entries_file,
            }
            .resolve_entries_with_key(key_index)
            .await?
        };

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::memory::CachedSegment;
    use std::collections::HashSet;
    use fxhash::FxHashMap;
    use tempfile::tempdir;
    use tokio::fs;


    #[tokio::test]
    async fn flush_and_find_single_key_multiple_values() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path().join("seg");

        fs::create_dir_all(&dir).await.unwrap();

        let mut map = FxHashMap::default();
        map.insert("key", vec!["value", "value2"]);

        let mem_seg = CachedSegment::new(map);
        let disk_seg = DiskSegment::open_or_create_segment(dir.clone())
            .await
            .unwrap();
        disk_seg.flush_memory_segment(&mem_seg).await.unwrap();

        let resolved = disk_seg.find("key").await.unwrap();
        let set: HashSet<_> = resolved.iter().cloned().collect();
        assert_eq!(
            set,
            HashSet::from(["value".to_string(), "value2".to_string()])
        );
    }

    #[tokio::test]
    async fn flush_and_find_no_dup_keys_no_dup_values() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path().join("seg");

        fs::create_dir_all(&dir).await.unwrap();

        let mut map = FxHashMap::default();
        map.insert("a", vec!["1"]);
        map.insert("b", vec!["2"]);

        let mem_seg = CachedSegment::new(map);
        let disk_seg = DiskSegment::open_or_create_segment(dir.clone())
            .await
            .unwrap();
        disk_seg.flush_memory_segment(&mem_seg).await.unwrap();

        assert_eq!(disk_seg.find("a").await.unwrap(), ["1"]);
        assert_eq!(disk_seg.find("b").await.unwrap(), ["2"]);
    }

    #[tokio::test]
    async fn flush_and_find_dup_keys_no_dup_values() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path().join("seg");

        fs::create_dir_all(&dir).await.unwrap();

        // Merge duplicate keys before insertion
        let mut map = FxHashMap::default();
        map.insert("a", vec!["1", "2"]);

        let mem_seg = CachedSegment::new(map);
        let disk_seg = DiskSegment::open_or_create_segment(dir.clone())
            .await
            .unwrap();
        disk_seg.flush_memory_segment(&mem_seg).await.unwrap();

        let resolved = disk_seg.find("a").await.unwrap();
        assert_eq!(resolved, ["1", "2"]);
    }

    #[tokio::test]
    async fn flush_and_find_no_dup_keys_dup_values() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path().join("seg");

        fs::create_dir_all(&dir).await.unwrap();

        let mut map = FxHashMap::default();
        map.insert("a", vec!["1"]);
        map.insert("b", vec!["1"]);

        let mem_seg = CachedSegment::new(map);
        let disk_seg = DiskSegment::open_or_create_segment(dir.clone())
            .await
            .unwrap();
        disk_seg.flush_memory_segment(&mem_seg).await.unwrap();

        assert_eq!(disk_seg.find("a").await.unwrap(), ["1"]);
        assert_eq!(disk_seg.find("b").await.unwrap(), ["1"]);
    }

    #[tokio::test]
    async fn flush_and_find_dup_keys_dup_values() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path().join("seg");

        fs::create_dir_all(&dir).await.unwrap();

        // Duplicate keys and values collapse into one entry
        let mut map = FxHashMap::default();
        map.insert("a", vec!["1"]);

        let mem_seg = CachedSegment::new(map);
        let disk_seg = DiskSegment::open_or_create_segment(dir.clone())
            .await
            .unwrap();
        disk_seg.flush_memory_segment(&mem_seg).await.unwrap();

        assert_eq!(disk_seg.find("a").await.unwrap(), ["1"]);
    }

    #[tokio::test]
    async fn find_nonexistent_key_returns_empty() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path().join("seg");

        fs::create_dir_all(&dir).await.unwrap();

        let mut map = FxHashMap::default();
        map.insert("x", vec!["1"]);
        map.insert("y", vec!["2"]);

        let mem_seg = CachedSegment::new(map);
        let disk_seg = DiskSegment::open_or_create_segment(dir.clone())
            .await
            .unwrap();

        disk_seg.flush_memory_segment(&mem_seg).await.unwrap();

        assert_eq!(disk_seg.find("z").await.unwrap().len(), 0);
    }
}
