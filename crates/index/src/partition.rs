use fxhash::FxHashMap;
use snafu::Snafu;
use std::path::PathBuf;
use tokio::io;
use tracing::Instrument;

use crate::segment::{self, TieredSegmentMap};

#[derive(Debug, Snafu)]
pub enum PartitionError {
    #[snafu(transparent)]
    IoError { source: io::Error },

    #[snafu(transparent)]
    ResolutionError {
        source: segment::DiskResolutionError,
    },

    #[snafu(transparent)]
    CreationError { source: segment::SegmentMapError },
}

pub struct PartitionMap {
    directory: PathBuf,
}

impl PartitionMap {
    pub async fn new(directory: PathBuf) -> Result<Self, PartitionError> {
        Ok(Self { directory })
    }

    // TODO: implement cache
    async fn load_segment_map(&self, partition: &str) -> Result<TieredSegmentMap, PartitionError> {
        let key = base32::encode(base32::Alphabet::Z, partition.as_bytes());

        Ok(TieredSegmentMap::new(self.directory.join(key)).await?)
    }

    pub async fn index<P: AsRef<str>, K: AsRef<str> + Ord, B: AsRef<str>>(
        &self,
        map: FxHashMap<P, FxHashMap<K, Vec<B>>>,
    ) -> Result<(), PartitionError> {
        for (partition, entries) in map {
            let mut segment = self.load_segment_map(partition.as_ref()).await?;

            segment
                .insert(entries)
                .instrument(tracing::trace_span!(
                    "tiered::index",
                    partition = partition.as_ref(),
                ))
                .await?;
        }

        Ok(())
    }

    pub async fn search<K: AsRef<str> + Ord, B: AsRef<str>>(
        &self,
        query: FxHashMap<K, Vec<B>>,
        mut limit: Option<usize>,
    ) -> Result<Vec<String>, PartitionError> {
        let mut result = Vec::new();

        for (partition, keys) in query {
            let segments = self.load_segment_map(partition.as_ref()).await?;

            for key in keys {
                result.extend(
                    segments
                        .find(key.as_ref(), limit)
                        .instrument(tracing::trace_span!(
                            "tiered::find",
                            partition = partition.as_ref(),
                            key = key.as_ref(),
                        ))
                        .await?,
                );

                if let Some(value) = limit {
                    let left = value.saturating_sub(result.len());

                    if left > 0 {
                        limit = Some(left);
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(result)
    }
}
