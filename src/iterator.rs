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
    items: std::vec::IntoIter<KeyValue>,
}

impl Iter {
    #[must_use]
    pub fn empty(direction: Direction) -> Self {
        Self {
            direction,
            items: Vec::new().into_iter(),
        }
    }

    #[must_use]
    pub fn from_items(mut items: Vec<KeyValue>, direction: Direction) -> Self {
        if direction == Direction::Reverse {
            items.reverse();
        }

        Self {
            direction,
            items: items.into_iter(),
        }
    }

    #[must_use]
    pub const fn direction(&self) -> Direction {
        self.direction
    }
}

impl Iterator for Iter {
    type Item = Result<KeyValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.items.next().map(Ok)
    }
}
