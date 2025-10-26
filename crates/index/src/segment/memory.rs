use bloomfilter::Bloom;
use futures_lite::stream::StopAfterFuture;
use fxhash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::{borrow::Cow, path::PathBuf, sync::WaitTimeoutResult};
use zerocopy::{ByteHash, IntoBytes};

pub struct SegmentView {
    pub directory: PathBuf,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum Entry {
    Compressed(Vec<u8>),
    Uncompressed(String),
}

impl Entry {
    pub fn new(string: &str) -> Self {
        let compressed = snappy::compress(string.as_bytes());

        if compressed.len() < string.len() {
            Self::Compressed(compressed)
        } else {
            Self::Uncompressed(string.to_string())
        }
    }

    pub fn as_uncompressed(&self) -> Cow<'_, str> {
        match self {
            Self::Compressed(buffer) => {
                String::try_from(snappy::uncompress(buffer.as_bytes()).unwrap())
                    .unwrap()
                    .into()
            }
            Self::Uncompressed(buffer) => buffer.as_str().into(),
        }
    }
}

pub struct MemorySegment {
    pub keys: Vec<Entry>,
    pub values: Vec<Entry>,
    pub entries: Vec<(u32, u32)>,
    pub bloom: Bloom<str>,
}

impl MemorySegment {
    fn to_keys_values_sets<'key, 'value, I2: IntoIterator<Item = &'value str>>(
        entries: impl IntoIterator<Item = (&'key str, I2)>,
    ) -> (Vec<Entry>, Vec<Entry>) {
        let mut keys = FxHashSet::default();
        let mut values = FxHashSet::default();

        for (key, items) in entries {
            keys.insert(Entry::new(key));
            for value in items {
                values.insert(Entry::new(value));
            }
        }

        let mut keys = keys.into_iter().collect::<Vec<_>>();
        keys.sort_unstable_by(|a, b| a.as_uncompressed().cmp(&b.as_uncompressed()));

        let mut values = values.into_iter().collect::<Vec<_>>();
        values.sort_unstable_by(|a, b| a.as_uncompressed().cmp(&b.as_uncompressed()));

        (keys, values)
    }

    fn new<'key, 'value, I2: IntoIterator<Item = &'value str>>(
        entries: impl IntoIterator<Item = (&'key str, I2)>,
    ) -> Self {
        let entries = Vec::from_iter(
            entries
                .into_iter()
                .map(|(key, values)| (key, values.into_iter().collect::<Vec<_>>())),
        );

        let (keys_linear, values_linear) = Self::to_keys_values_sets(entries.iter().cloned());

        let mut bloom =
            Bloom::new((entries.len().ilog2() * 2 + 1) as usize, entries.len()).unwrap();

        let mut entries_linear = entries
            .into_iter()
            .flat_map(|(key, values)| {
                values
                    .into_iter()
                    .map(|value| {
                        bloom.set(key);

                        let key = keys_linear
                            .binary_search_by(|entry| entry.as_uncompressed().as_ref().cmp(key))
                            .unwrap();
                        let value = values_linear
                            .binary_search_by(|entry| entry.as_uncompressed().as_ref().cmp(value))
                            .unwrap();

                        (key as u32, value as u32)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        entries_linear.sort_unstable_by_key(|&(key, ..)| key);

        entries_linear.dedup();

        Self {
            keys: keys_linear,
            values: values_linear,
            entries: entries_linear,
            bloom: bloom,
        }
    }

    pub fn resolve(&self, key: &str) -> Vec<String> {
        let Ok(key_index) = self
            .keys
            .binary_search_by(|entry| entry.as_uncompressed().as_ref().cmp(key))
        else {
            return Vec::new();
        };

        let Ok(mut index) = self
            .entries
            .binary_search_by(|&(index, ..)| index.cmp(&(key_index as u32)))
        else {
            return Vec::new();
        };

        eprintln!("lol");

        let mut items = Vec::new();

        while index as isize - 1 >= 0 && self.entries[index - 1].0 == key_index as u32 {
            index -= 1;
        }

        for index in index..self.entries.len() {
            let (item_key_index, value_index) = self.entries[index];

            if key_index as u32 != item_key_index {
                break;
            }

            items.push(
                self.values[value_index as usize]
                    .as_uncompressed()
                    .to_string(),
            );
        }

        items
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn entry_compression_behavior() {
        let short = "short"; // not compressible enough
        let long = "aaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb"; // highly compressible

        let e1 = Entry::new(short);
        let e2 = Entry::new(long);

        match e1 {
            Entry::Uncompressed(_) => {}
            _ => panic!("Short string should not be compressed"),
        }

        match e2 {
            Entry::Compressed(_) => {}
            _ => panic!("Highly compressible string should be compressed"),
        }

        assert_eq!(e1.as_uncompressed(), short);
        assert_eq!(e2.as_uncompressed(), long);
    }

    #[test]
    fn new_and_resolve_single_key_multiple_values() {
        // ("key", ["value", "value", "value2"])
        let seg = MemorySegment::new([("key", ["value", "value", "value2"])]);

        // Should contain unique key
        assert_eq!(seg.keys.len(), 1);
        // Should contain deduplicated values ("value", "value2")
        let unique_values: HashSet<_> = seg
            .values
            .iter()
            .map(|v| v.as_uncompressed().to_string())
            .collect();
        assert_eq!(unique_values.len(), 2);

        // resolve should return a valid value for "key"
        let resolved = seg.resolve("key");
        assert_eq!(resolved, ["value", "value2"]);
    }

    #[test]
    fn no_dup_keys_no_dup_values() {
        let seg = MemorySegment::new([("a", ["1"]), ("b", ["2"])]);

        assert_eq!(seg.keys.len(), 2);
        assert_eq!(seg.values.len(), 2);
        assert_eq!(seg.entries.len(), 2);

        assert_eq!(seg.resolve("a"), ["1"]);
        assert_eq!(seg.resolve("b"), ["2"]);
    }

    #[test]
    fn dup_keys_no_dup_values() {
        let seg = MemorySegment::new([("a", ["1"]), ("a", ["2"])]);

        // Deduplicated keys (1 unique key)
        assert_eq!(seg.keys.len(), 1);
        // 2 unique values
        assert_eq!(seg.values.len(), 2);
        assert_eq!(seg.entries.len(), 2);

        // Resolve returns one of the matching values (depending on insertion order)
        let resolved = seg.resolve("a");
        assert_eq!(resolved, ["1", "2"]);
    }

    #[test]
    fn no_dup_keys_dup_values() {
        let seg = MemorySegment::new([("a", ["1"]), ("b", ["1"])]);

        // 2 keys, 1 deduplicated value
        assert_eq!(seg.keys.len(), 2);
        assert_eq!(seg.values.len(), 1);
        assert_eq!(seg.entries.len(), 2);

        assert_eq!(seg.resolve("a"), ["1"]);
        assert_eq!(seg.resolve("b"), ["1"]);
    }

    #[test]
    fn dup_keys_dup_values() {
        let seg = MemorySegment::new([("a", ["1"]), ("a", ["1"])]);

        // 1 unique key, 1 unique value
        assert_eq!(seg.keys.len(), 1);
        assert_eq!(seg.values.len(), 1);
        assert_eq!(seg.entries.len(), 1);

        assert_eq!(seg.resolve("a"), ["1"]);
    }
}
