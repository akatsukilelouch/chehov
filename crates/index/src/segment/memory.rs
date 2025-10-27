use bloomfilter::Bloom;
use fxhash::{FxHashMap, FxHashSet};
use std::borrow::Cow;
use zerocopy::IntoBytes;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum Entry {
    Compressed(Vec<u8>),
    Uncompressed(String),
}

impl AsRef<[u8]> for Entry {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Compressed(buffer) => buffer.as_ref(),
            Self::Uncompressed(string) => string.as_bytes(),
        }
    }
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

    pub fn into_uncompressed(self) -> String {
        match self {
            Self::Compressed(buffer) => {
                String::try_from(snappy::uncompress(buffer.as_bytes()).unwrap()).unwrap()
            }
            Self::Uncompressed(buffer) => buffer,
        }
    }
}

pub struct CachedSegment {
    pub keys: Vec<Entry>,
    pub values: Vec<Entry>,
    pub entries: Vec<(u32, u32)>,
    pub bloom: Bloom<str>,
}

impl CachedSegment {
    fn to_keys_values_sets<K: AsRef<str> + Ord + Eq, B: AsRef<str>>(
        entries: &FxHashMap<K, Vec<B>>,
    ) -> (Vec<Entry>, Vec<Entry>) {
        let mut values_mapping = FxHashSet::default();

        let mut keys = Vec::new();

        for (key, items) in entries {
            keys.push(Entry::new(key.as_ref()));

            values_mapping.extend(items.iter().map(|item| item.as_ref()));
        }

        keys.sort_unstable_by(|a, b| a.as_uncompressed().cmp(&b.as_uncompressed()));

        tracing::trace!("created keys mapping: {:?}", keys.len());

        let mut values = values_mapping
            .into_iter()
            .map(Entry::new)
            .collect::<Vec<_>>();
        values.sort_unstable_by(|a, b| a.as_uncompressed().cmp(&b.as_uncompressed()));

        tracing::trace!("created values mapping: {:?}", values.len());

        (keys, values)
    }

    pub fn new<K: AsRef<str> + Ord + Eq, B: AsRef<str>>(entries: FxHashMap<K, Vec<B>>) -> Self {
        let (keys_linear, values_linear) = Self::to_keys_values_sets(&entries);

        let mut bloom =
            Bloom::new((entries.len().ilog2() * 2 + 1) as usize, entries.len()).unwrap();

        tracing::trace!("created new bloom of size: {:?}", bloom.len());

        let mut entries_linear = entries
            .into_iter()
            .flat_map(|(key, values)| {
                values
                    .into_iter()
                    .map(|value| {
                        bloom.set(key.as_ref());

                        let key = keys_linear
                            .binary_search_by(|entry| {
                                entry.as_uncompressed().as_ref().cmp(key.as_ref())
                            })
                            .unwrap();
                        let value = values_linear
                            .binary_search_by(|entry| {
                                entry.as_uncompressed().as_ref().cmp(value.as_ref())
                            })
                            .unwrap();

                        (key as u32, value as u32)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        entries_linear.sort_unstable_by_key(|&(key, ..)| key);

        tracing::trace!("created entries: {:?}", entries_linear.len());

        entries_linear.dedup();

        tracing::trace!("deduplicated entries: {:?}", entries_linear.len());

        Self {
            keys: keys_linear,
            values: values_linear,
            entries: entries_linear,
            bloom,
        }
    }

    pub fn find(&self, key: &str) -> Vec<String> {
        let Ok(key_index) = self
            .keys
            .binary_search_by(|entry| entry.as_uncompressed().as_ref().cmp(key))
        else {
            return Vec::new();
        };

        tracing::trace!("found key index: {:?}", key_index);

        let Ok(mut index) = self
            .entries
            .binary_search_by(|&(index, ..)| index.cmp(&(key_index as u32)))
        else {
            return Vec::new();
        };

        tracing::trace!("poked (found approximately) entries at index: {:?}", index);

        let mut items = Vec::new();

        while index as isize > 0 && self.entries[index - 1].0 == key_index as u32 {
            index -= 1;
        }

        tracing::trace!("found the start at index: {:?}", index);

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

        tracing::trace!("loaded values: {:?}", items.len());

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
        let mut map = FxHashMap::default();
        map.insert("key", vec!["value", "value", "value2"]);

        let seg = CachedSegment::new(map);

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
        let resolved = seg.find("key");
        assert_eq!(resolved, ["value", "value2"]);
    }

    #[test]
    fn no_dup_keys_no_dup_values() {
        let mut map = FxHashMap::default();
        map.insert("a", vec!["1"]);
        map.insert("b", vec!["2"]);

        let seg = CachedSegment::new(map);

        assert_eq!(seg.keys.len(), 2);
        assert_eq!(seg.values.len(), 2);
        assert_eq!(seg.entries.len(), 2);

        assert_eq!(seg.find("a"), ["1"]);
        assert_eq!(seg.find("b"), ["2"]);
    }

    #[test]
    fn dup_keys_no_dup_values() {
        // Simulate duplicate keys by merging values before calling new()
        let mut map = FxHashMap::default();
        map.insert("a", vec!["1", "2"]);

        let seg = CachedSegment::new(map);

        // Deduplicated keys (1 unique key)
        assert_eq!(seg.keys.len(), 1);
        // 2 unique values
        assert_eq!(seg.values.len(), 2);
        assert_eq!(seg.entries.len(), 2);

        // Resolve returns both values
        let resolved = seg.find("a");
        assert_eq!(resolved, ["1", "2"]);
    }

    #[test]
    fn no_dup_keys_dup_values() {
        let mut map = FxHashMap::default();
        map.insert("a", vec!["1"]);
        map.insert("b", vec!["1"]);

        let seg = CachedSegment::new(map);

        // 2 keys, 1 deduplicated value
        assert_eq!(seg.keys.len(), 2);
        assert_eq!(seg.values.len(), 1);
        assert_eq!(seg.entries.len(), 2);

        assert_eq!(seg.find("a"), ["1"]);
        assert_eq!(seg.find("b"), ["1"]);
    }

    #[test]
    fn dup_keys_dup_values() {
        // Duplicates now merge under same key
        let mut map = FxHashMap::default();
        map.insert("a", vec!["1", "1"]);

        let seg = CachedSegment::new(map);

        // 1 unique key, 1 unique value
        assert_eq!(seg.keys.len(), 1);
        assert_eq!(seg.values.len(), 1);
        assert_eq!(seg.entries.len(), 1);

        assert_eq!(seg.find("a"), ["1"]);
    }
}
