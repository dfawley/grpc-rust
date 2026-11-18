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

/// A trait to add `Debug` to an `Any` for [`WorkData`] to allow debugging data
/// to be printed more readily.  Blanket implemented on all types that are Any +
/// Send + Debug.  `dyn WorkDataTrait` also implements downcast methods like
/// [`Any`] for convenience.
pub trait WorkDataTrait: Any + Send + Debug {}

impl<T: Any + Send + Debug> WorkDataTrait for T {}

impl dyn WorkDataTrait {
    /// Like [`Box<dyn Any>::downcast`] but for this wrapper trait.
    pub fn downcast<T: Any>(self: Box<Self>) -> Result<Box<T>, Box<Self>> {
        // If we directly call downcast then we can't return `Self` anymore
        // (only a Box<dyn Any + Send>), so we first have to check `is` and only
        // downcast when we know it will succeed.
        if (&*self as &(dyn Any + Send)).is::<T>() {
            Ok((self as Box<dyn Any + Send>).downcast().unwrap())
        } else {
            Err(self)
        }
    }

    /// Like
    /// [`downcast_ref`](https://doc.rust-lang.org/std/any/trait.Any.html#method.downcast_ref)
    /// implemented on [`dyn Any`](Any), but for this wrapper trait.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        (self as &(dyn Any + Send)).downcast_ref::<T>()
    }

    /// Like
    /// [`downcast_mut`](https://doc.rust-lang.org/std/any/trait.Any.html#method.downcast_mut)
    /// implemented on [`dyn Any`](Any), but for this wrapper trait.
    pub fn downcast_mut<T: Any>(&mut self) -> Option<&mut T> {
        (self as &mut (dyn Any + Send)).downcast_mut::<T>()
    }
}

/// A dynamic payload passed between [`WorkScheduler::schedule_work`] and its
/// associated policy's [`work`](crate::client::load_balancing::LbPolicy::work) method.
pub type WorkData = Box<dyn WorkDataTrait>;

/// Used to asynchronously request a call into the LbPolicy's work method if
/// the LbPolicy needs to provide an update without waiting for an update
/// from the channel first.
pub trait WorkScheduler: Send + Sync + Debug {
    /// Schedules a call into the LbPolicy's work method.  Multiple work calls
    /// carrying a `data` payload of `None` may be coalesced with one another.
    fn schedule_work(&self, data: Option<WorkData>);
}
