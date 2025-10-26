use std::path::{Path, PathBuf};

use bitflags::bitflags;
use bloomfilter::Bloom;
use tokio::{
    fs::{self, DirEntry},
    io::{self, AsyncSeekExt, AsyncWriteExt},
};

use super::memory::{Entry, MemorySegment};

pub struct DiskSegment {
    directory: PathBuf,
}

impl DiskSegment {
    async fn write_lookup_table(
        &self,
        prefix: &str,
        offsets: impl IntoIterator<Item = u64>,
    ) -> Result<(), io::Error> {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(format!("{prefix}.lookup.bin"))
            .await?;

        for item in offsets {
            file.write_u64(item).await?;
        }

        Ok(())
    }

    async fn write_full_table<'entry>(
        &self,
        prefix: &str,
        table: impl IntoIterator<Item = &'entry Entry, IntoIter: ExactSizeIterator>,
    ) -> Result<(), io::Error> {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.directory.join(format!("{prefix}.data.bin")))
            .await?;

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
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.directory.join("file.bin"))
            .await?;

        for (key, value) in entries {
            file.write_u32(key).await?;
            file.write_u32(value).await?;
        }

        Ok(())
    }

    pub async fn flush_memory_segment(&self, segment: &MemorySegment) -> Result<(), io::Error> {
        self.write_full_table("keys", segment.keys.iter()).await?;
        self.write_full_table("values", segment.values.iter())
            .await?;

        self.write_bloom_filter(&segment.bloom).await?;

        self.write_entries(segment.entries.iter().cloned()).await?;

        Ok(())
    }

    #[inline]
    pub async fn create_empty_segment(directory: PathBuf) -> Result<Self, io::Error> {
        Ok(Self { directory })
    }
}
