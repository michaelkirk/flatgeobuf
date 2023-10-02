use core::ops::{Range, RangeFrom};

#[derive(Debug, Clone)]
pub enum HttpRange {
    Range(Range<usize>),
    RangeFrom(RangeFrom<usize>),
}

impl HttpRange {
    pub fn start(&self) -> usize {
        match self {
            Self::Range(range) => range.start,
            Self::RangeFrom(range) => range.start,
        }
    }

    pub fn end(&self) -> Option<usize> {
        match self {
            Self::Range(range) => Some(range.end),
            Self::RangeFrom(_) => None,
        }
    }

    pub fn with_end(self, end: Option<usize>) -> Self {
        match end {
            Some(end) => Self::Range(self.start()..end),
            None => Self::RangeFrom(self.start()..),
        }
    }

    pub fn split(&mut self, new_end: usize) -> Self {
        assert!(new_end > self.start());
        match self {
            Self::Range(range) => {
                assert!(new_end <= range.end);
                let old_end = range.end;
                range.end = new_end;
                Self::Range(new_end..old_end)
            }
            Self::RangeFrom(range) => {
                *self = Self::Range(range.start..new_end);
                Self::RangeFrom(new_end..)
            }
        }
    }
}
