use crate::rocks::kv::key::Key;
use std::borrow::Borrow;
use std::ops::{
    Bound, Range, RangeBounds, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundRange {
    from: Bound<Key>,
    to: Bound<Key>,
}

impl BoundRange {
    fn new(from: Bound<Key>, to: Bound<Key>) -> BoundRange {
        BoundRange { from, to }
    }

    pub fn range_from(from: Key) -> BoundRange {
        BoundRange {
            from: Bound::Included(from),
            to: Bound::Unbounded,
        }
    }

    pub fn into_keys(self) -> (Key, Option<Key>) {
        let start = match self.from {
            Bound::Included(v) => v,
            Bound::Excluded(mut v) => {
                v.push_zero();
                v
            }
            Bound::Unbounded => Key::EMPTY,
        };
        let end = match self.to {
            Bound::Included(mut v) => {
                v.push_zero();
                Some(v)
            }
            Bound::Excluded(v) => Some(v),
            Bound::Unbounded => None,
        };
        (start, end)
    }
}

impl RangeBounds<Key> for BoundRange {
    // clippy will act differently on nightly and stable, so we allow `needless_match` here.
    #[allow(clippy::needless_match)]
    fn start_bound(&self) -> Bound<&Key> {
        match &self.from {
            Bound::Included(f) => Bound::Included(f),
            Bound::Excluded(f) => Bound::Excluded(f),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&Key> {
        match &self.to {
            Bound::Included(t) => {
                if t.is_empty() {
                    Bound::Unbounded
                } else {
                    Bound::Included(t)
                }
            }
            Bound::Excluded(t) => {
                if t.is_empty() {
                    Bound::Unbounded
                } else {
                    Bound::Excluded(t)
                }
            }
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

// FIXME `==` should not `clone`
impl<T: Into<Key> + Clone> PartialEq<(Bound<T>, Bound<T>)> for BoundRange {
    fn eq(&self, other: &(Bound<T>, Bound<T>)) -> bool {
        self.from == convert_to_bound_key(other.0.clone())
            && self.to == convert_to_bound_key(other.1.clone())
    }
}

impl<T: Into<Key>> From<Range<T>> for BoundRange {
    fn from(other: Range<T>) -> BoundRange {
        BoundRange::new(
            Bound::Included(other.start.into()),
            Bound::Excluded(other.end.into()),
        )
    }
}

impl<T: Into<Key>> From<RangeFrom<T>> for BoundRange {
    fn from(other: RangeFrom<T>) -> BoundRange {
        BoundRange::new(Bound::Included(other.start.into()), Bound::Unbounded)
    }
}

impl<T: Into<Key>> From<RangeTo<T>> for BoundRange {
    fn from(other: RangeTo<T>) -> BoundRange {
        BoundRange::new(Bound::Unbounded, Bound::Excluded(other.end.into()))
    }
}

impl<T: Into<Key>> From<RangeInclusive<T>> for BoundRange {
    fn from(other: RangeInclusive<T>) -> BoundRange {
        let (start, end) = other.into_inner();
        BoundRange::new(Bound::Included(start.into()), Bound::Included(end.into()))
    }
}

impl<T: Into<Key>> From<RangeToInclusive<T>> for BoundRange {
    fn from(other: RangeToInclusive<T>) -> BoundRange {
        BoundRange::new(Bound::Unbounded, Bound::Included(other.end.into()))
    }
}

impl From<RangeFull> for BoundRange {
    fn from(_other: RangeFull) -> BoundRange {
        BoundRange::new(Bound::Unbounded, Bound::Unbounded)
    }
}

impl<T: Into<Key>> From<(T, Option<T>)> for BoundRange {
    fn from(other: (T, Option<T>)) -> BoundRange {
        let to = match other.1 {
            None => Bound::Unbounded,
            Some(to) => to.into().into_upper_bound(),
        };

        BoundRange::new(other.0.into().into_lower_bound(), to)
    }
}

impl<T: Into<Key>> From<(T, T)> for BoundRange {
    fn from(other: (T, T)) -> BoundRange {
        BoundRange::new(
            other.0.into().into_lower_bound(),
            other.1.into().into_upper_bound(),
        )
    }
}

impl<T: Into<Key> + Eq> From<(Bound<T>, Bound<T>)> for BoundRange {
    fn from(bounds: (Bound<T>, Bound<T>)) -> BoundRange {
        BoundRange::new(
            convert_to_bound_key(bounds.0),
            convert_to_bound_key(bounds.1),
        )
    }
}

pub trait IntoOwnedRange {
    /// Transform a borrowed range of some form into an owned `BoundRange`.
    fn into_owned(self) -> BoundRange;
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange for Range<&U> {
    fn into_owned(self) -> BoundRange {
        From::from(Range {
            start: self.start.to_owned(),
            end: self.end.to_owned(),
        })
    }
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange for RangeFrom<&U> {
    fn into_owned(self) -> BoundRange {
        From::from(RangeFrom {
            start: self.start.to_owned(),
        })
    }
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange for RangeTo<&U> {
    fn into_owned(self) -> BoundRange {
        From::from(RangeTo {
            end: self.end.to_owned(),
        })
    }
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange
    for RangeInclusive<&U>
{
    fn into_owned(self) -> BoundRange {
        let (from, to) = self.into_inner();
        From::from(RangeInclusive::new(from.to_owned(), to.to_owned()))
    }
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange
    for RangeToInclusive<&U>
{
    fn into_owned(self) -> BoundRange {
        From::from(RangeToInclusive {
            end: self.end.to_owned(),
        })
    }
}

impl IntoOwnedRange for RangeFull {
    fn into_owned(self) -> BoundRange {
        From::from(self)
    }
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange for (&U, Option<&U>) {
    fn into_owned(self) -> BoundRange {
        From::from((self.0.to_owned(), self.1.map(|u| u.to_owned())))
    }
}

impl<T: Into<Key> + Borrow<U>, U: ToOwned<Owned = T> + ?Sized> IntoOwnedRange for (&U, &U) {
    fn into_owned(self) -> BoundRange {
        From::from((self.0.to_owned(), self.1.to_owned()))
    }
}

fn convert_to_bound_key<K: Into<Key>>(b: Bound<K>) -> Bound<Key> {
    match b {
        Bound::Included(k) => Bound::Included(k.into()),
        Bound::Excluded(k) => Bound::Excluded(k.into()),
        Bound::Unbounded => Bound::Unbounded,
    }
}
