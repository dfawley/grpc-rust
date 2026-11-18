/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

//! Work for LB policies: payloads, and scheduling calls to a policy's `work`
//! method.

use std::any::Any;
use std::fmt::Debug;

/// A dynamic payload passed between [`WorkScheduler::schedule_work`] and its
/// associated policy's [`work`](crate::client::load_balancing::LbPolicy::work)
/// method.
///
/// Holds any `Send + Debug` value, which the receiving policy recovers with
/// [`WorkData::downcast`] and similar methods.
pub struct WorkData(Box<dyn Payload>);

/// What a WorkData can hold: `Any`, plus `Debug` so the payload can be
/// printed.
trait Payload: Any + Send + Debug {}

impl<T: Any + Send + Debug> Payload for T {}

impl WorkData {
    /// Creates a WorkData holding `data`.
    pub fn new<T: Any + Send + Debug>(data: T) -> Self {
        Self(Box::new(data))
    }

    /// Returns true if this holds a `T`.
    pub fn is<T: Any>(&self) -> bool {
        (&*self.0 as &dyn Any).is::<T>()
    }

    /// Returns the `T` this holds, or this WorkData unchanged if it holds
    /// something else.
    pub fn downcast<T: Any>(self) -> Result<T, Self> {
        if !self.is::<T>() {
            return Err(self);
        }
        let data: Box<dyn Any> = self.0;
        Ok(*data.downcast::<T>().expect("checked by is"))
    }

    /// Returns a reference to the `T` this holds, if it holds one.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        (&*self.0 as &dyn Any).downcast_ref::<T>()
    }

    /// Returns a mutable reference to the `T` this holds, if it holds one.
    pub fn downcast_mut<T: Any>(&mut self) -> Option<&mut T> {
        (&mut *self.0 as &mut dyn Any).downcast_mut::<T>()
    }
}

impl Debug for WorkData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Used to asynchronously request a call into the LbPolicy's work method if
/// the LbPolicy needs to provide an update without waiting for an update
/// from the channel first.
pub trait WorkScheduler: Send + Sync + Debug {
    /// Schedules a call into the LbPolicy's work method.  Multiple work calls
    /// carrying a `data` payload of `None` may be coalesced with one another.
    fn schedule_work(&self, data: Option<WorkData>);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_data_downcasts() {
        let mut data = WorkData::new(5u32);
        assert!(data.is::<u32>());
        assert!(!data.is::<i32>());
        assert_eq!(data.downcast_ref::<u32>(), Some(&5));
        assert_eq!(data.downcast_ref::<i32>(), None);
        *data.downcast_mut::<u32>().unwrap() = 6;
        assert_eq!(format!("{data:?}"), "6");

        // A failed downcast returns the data unchanged.
        let data = data.downcast::<i32>().unwrap_err();
        assert_eq!(data.downcast::<u32>().unwrap(), 6);
    }
}
