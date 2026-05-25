#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum PrefixExtractor {
    FixedLen(usize),
    Separator(u8),
    Custom(String),
    #[default]
    Disabled,
}

impl PrefixExtractor {
    #[must_use]
    pub fn extract<'key>(&self, key: &'key [u8]) -> Option<&'key [u8]> {
        match self {
            Self::FixedLen(len) => key.get(..*len),
            Self::Separator(separator) => key
                .iter()
                .position(|byte| byte == separator)
                .map(|index| &key[..=index]),
            Self::Custom(_) | Self::Disabled => None,
        }
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !matches!(self, Self::Disabled)
    }

    #[must_use]
    pub(crate) fn supports_prefix_filter(&self) -> bool {
        matches!(self, Self::FixedLen(_) | Self::Separator(_))
    }

    #[must_use]
    pub(crate) fn query_filter_prefix<'prefix>(
        &self,
        query_prefix: &'prefix [u8],
    ) -> Option<&'prefix [u8]> {
        match self {
            Self::FixedLen(len) => query_prefix.get(..*len),
            Self::Separator(separator) => query_prefix
                .iter()
                .position(|byte| byte == separator)
                .map(|index| &query_prefix[..=index]),
            Self::Custom(_) | Self::Disabled => None,
        }
    }
}
