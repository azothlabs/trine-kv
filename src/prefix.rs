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
}
