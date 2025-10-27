mod segment;
mod partition;

pub use fxhash;

pub use partition::{PartitionMap, PartitionError};
pub use segment::{SegmentMapError, DiskResolutionError};
