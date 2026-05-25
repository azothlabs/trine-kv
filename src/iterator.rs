use crate::{error::Result, types::KeyValue};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Direction {
    #[default]
    Forward,
    Reverse,
}

#[derive(Debug, Clone)]
pub struct Iter {
    direction: Direction,
}

impl Iter {
    #[must_use]
    pub const fn empty(direction: Direction) -> Self {
        Self { direction }
    }

    #[must_use]
    pub const fn direction(&self) -> Direction {
        self.direction
    }
}

impl Iterator for Iter {
    type Item = Result<KeyValue>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
