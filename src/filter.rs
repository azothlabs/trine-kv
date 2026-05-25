use std::collections::BTreeSet;

use crate::{error::Result, prefix::PrefixExtractor};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterKind {
    PointKey,
    Prefix,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixFilterDescriptor {
    pub extractor: PrefixExtractor,
    pub partitioned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrefixFilter {
    extractor: PrefixExtractor,
    prefixes: BTreeSet<Vec<u8>>,
}

impl PrefixFilter {
    #[must_use]
    pub(crate) fn from_keys<'key>(
        extractor: PrefixExtractor,
        keys: impl IntoIterator<Item = &'key [u8]>,
    ) -> Option<Self> {
        if !extractor.supports_prefix_filter() {
            return None;
        }

        let prefixes = keys
            .into_iter()
            .filter_map(|key| extractor.extract(key).map(<[u8]>::to_vec))
            .collect::<BTreeSet<_>>();

        Some(Self {
            extractor,
            prefixes,
        })
    }

    pub(crate) fn from_sorted_prefixes(
        extractor: PrefixExtractor,
        prefixes: Vec<Vec<u8>>,
    ) -> Result<Self> {
        let mut previous = None;
        for prefix in &prefixes {
            if previous
                .as_ref()
                .is_some_and(|previous: &Vec<u8>| previous >= prefix)
            {
                return Err(crate::Error::InvalidFormat {
                    message: "prefix filter entries are not sorted and unique".to_owned(),
                });
            }
            previous = Some(prefix.clone());
        }

        Ok(Self {
            extractor,
            prefixes: prefixes.into_iter().collect(),
        })
    }

    #[must_use]
    pub(crate) const fn extractor(&self) -> &PrefixExtractor {
        &self.extractor
    }

    #[must_use]
    pub(crate) fn prefixes(&self) -> &BTreeSet<Vec<u8>> {
        &self.prefixes
    }

    #[must_use]
    pub(crate) fn may_contain_query_prefix(
        &self,
        query_prefix: &[u8],
        query_extractor: &PrefixExtractor,
    ) -> bool {
        if query_extractor != &self.extractor {
            return true;
        }

        query_extractor
            .query_filter_prefix(query_prefix)
            .is_none_or(|prefix| self.prefixes.contains(prefix))
    }
}
