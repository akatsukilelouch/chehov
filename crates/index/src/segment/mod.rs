use std::{collections::VecDeque, path::PathBuf};

use tokio::io;

mod disk;
mod memory;

pub struct TieredSegmentMap {
    directory: PathBuf,
    counter: usize,

    memory: VecDeque<memory::MemorySegment>,

    disk: VecDeque<disk::DiskSegment>,
}

impl TieredSegmentMap {
    pub async fn move_segment(&mut self, index: usize) -> Result<(), io::Error> {
        let memory_segment = self.memory.remove(index).expect("invalid segment index");

        let path = {
            self.counter += 1;

            self.directory.join(format!("{}-segment", self.counter))
        };

        let disk_segment = disk::DiskSegment::create_empty_segment(path).await?;
        disk_segment.flush_memory_segment(&memory_segment).await?;
        Ok(())
    }
}
