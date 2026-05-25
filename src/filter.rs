use crate::prefix::PrefixExtractor;

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
