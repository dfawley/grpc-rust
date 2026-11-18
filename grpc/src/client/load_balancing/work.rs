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

//! Addressing and queueing work for LB policies.
//!
//! The channel delivers all LB work to the root policy, and each parent passes
//! it on to the child it is for.  A [`WorkRoute`] describes one policy's
//! position in the tree: the chain of [`Hop`]s, one per parent, that wraps
//! work for that policy so each ancestor can find it on the way down.

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

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

/// One level of a [`WorkRoute`]: how a parent wraps work for one of its
/// children (or for itself) so that its own `work` method can find where it
/// goes.
///
/// This may be called on any thread, so it must only wrap the data: it must
/// not block or access policy state.
pub trait Hop: Send + Sync + Debug {
    /// Wraps `data`, which is for the policy one level below, as the policy
    /// that added this hop receives it in its `work` method.
    fn wrap(&self, data: Option<WorkData>) -> Option<WorkData>;
}

/// The path from one policy up to the root policy of a channel.
///
/// Routes are built top-down: the channel creates the root route, and every
/// other route is created with [`WorkRoute::child`] from the route of the
/// policy above it.  So the route of any policy, and any route passed up to a
/// parent through
/// [`new_subchannel`](crate::client::load_balancing::ChannelController::new_subchannel),
/// passes through every policy above it.
///
/// Clones share the same path, and are cheap.
#[derive(Clone, Debug)]
pub struct WorkRoute(Arc<Node>);

#[derive(Debug)]
enum Node {
    Root,
    Child {
        hop: Arc<dyn Hop>,
        parent: WorkRoute,
    },
}

impl WorkRoute {
    /// Creates the route to the root policy.  Only the channel (and tests)
    /// create root routes.
    pub(crate) fn root() -> Self {
        Self(Arc::new(Node::Root))
    }

    /// Returns a route one level below this one, which wraps work with `hop`
    /// before passing it to this route.
    pub fn child(&self, hop: Arc<dyn Hop>) -> Self {
        Self(Arc::new(Node::Child {
            hop,
            parent: self.clone(),
        }))
    }

    /// Wraps `data`, which is for this route's policy, as the root policy
    /// receives it in its `work` method.
    pub fn envelope(&self, data: Option<WorkData>) -> Option<WorkData> {
        self.wrap_until(None, data)
    }

    /// Wraps `data`, which is for this route's policy, as `ancestor`'s policy
    /// receives it in its own `work` method.  That policy can then route it as
    /// it routes all of its work, e.g. by passing it to its child, without
    /// queueing it.
    ///
    /// `ancestor` must be this route or on its path to the root.  That is
    /// always true of a policy's own route and any route passed up to it
    /// through `new_subchannel`.
    ///
    /// # Panics
    ///
    /// If `ancestor` is not on this route's path to the root.
    pub fn envelope_below(&self, ancestor: &WorkRoute, data: Option<WorkData>) -> Option<WorkData> {
        self.wrap_until(Some(ancestor), data)
    }

    fn wrap_until(
        &self,
        ancestor: Option<&WorkRoute>,
        mut data: Option<WorkData>,
    ) -> Option<WorkData> {
        let mut route = self;
        while !ancestor.is_some_and(|a| Arc::ptr_eq(&route.0, &a.0)) {
            match &*route.0 {
                Node::Child { hop, parent } => {
                    data = hop.wrap(data);
                    route = parent;
                }
                Node::Root if ancestor.is_none() => break,
                Node::Root => panic!("route is not below the given ancestor"),
            }
        }
        data
    }
}

/// A queue of work for the root policy, processed by the channel's work loop.
pub trait WorkQueue: Send + Sync + Debug {
    /// Queues a call into the root policy's work method with `data`, which
    /// must already be wrapped by [`WorkRoute::envelope`].
    ///
    /// This may be called on any thread, including while a subchannel holds
    /// its locks.  Implementations must only queue the work: they must not
    /// block, call a policy, or call into a subchannel.
    fn enqueue(&self, data: Option<WorkData>);
}

/// Used to asynchronously request a call into the LbPolicy's work method if
/// the LbPolicy needs to provide an update without waiting for an update
/// from the channel first.
///
/// A WorkScheduler combines the [`WorkRoute`] to its policy with the channel's
/// [`WorkQueue`].
#[derive(Clone, Debug)]
pub struct WorkScheduler {
    route: WorkRoute,
    queue: Arc<dyn WorkQueue>,
}

impl WorkScheduler {
    /// Creates a WorkScheduler for the root policy.  Only the channel (and
    /// tests) create root schedulers.
    pub(crate) fn new_root(queue: Arc<dyn WorkQueue>) -> Self {
        Self {
            route: WorkRoute::root(),
            queue,
        }
    }

    /// Returns a WorkScheduler on the same queue whose route is one level
    /// below this one; see [`WorkRoute::child`].
    pub fn child(&self, hop: Arc<dyn Hop>) -> Self {
        Self {
            route: self.route.child(hop),
            queue: self.queue.clone(),
        }
    }

    /// Returns the route to this scheduler's policy.
    pub fn route(&self) -> &WorkRoute {
        &self.route
    }

    /// Schedules a call into the LbPolicy's work method.  Multiple work calls
    /// carrying a `data` payload of `None` may be coalesced with one another.
    pub fn schedule_work(&self, data: Option<WorkData>) {
        self.queue.enqueue(self.route.envelope(data));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Wraps data in a Vec<&str>, appending `name`.
    #[derive(Debug)]
    struct NamedHop(&'static str);

    impl Hop for NamedHop {
        fn wrap(&self, data: Option<WorkData>) -> Option<WorkData> {
            let mut path = data
                .map(|d| d.downcast::<Vec<&'static str>>().unwrap())
                .unwrap_or_default();
            path.push(self.0);
            Some(WorkData::new(path))
        }
    }

    fn path(data: Option<WorkData>) -> Vec<&'static str> {
        data.map(|d| d.downcast::<Vec<&'static str>>().unwrap())
            .unwrap_or_default()
    }

    // Builds root -> a -> b -> c and returns (root, a, c).
    fn build() -> (WorkRoute, WorkRoute, WorkRoute) {
        let root = WorkRoute::root();
        let a = root.child(Arc::new(NamedHop("a")));
        let c = a
            .child(Arc::new(NamedHop("b")))
            .child(Arc::new(NamedHop("c")));
        (root, a, c)
    }

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

    #[test]
    fn envelope_applies_every_hop() {
        let (root, _, c) = build();
        assert_eq!(path(c.envelope(None)), vec!["c", "b", "a"]);
        assert_eq!(path(root.envelope(None)), Vec::<&str>::new());
    }

    #[test]
    fn envelope_below_stops_at_ancestor() {
        let (root, a, c) = build();
        assert_eq!(path(c.envelope_below(&a, None)), vec!["c", "b"]);
        assert_eq!(path(c.envelope_below(&root, None)), vec!["c", "b", "a"]);
        assert_eq!(path(c.envelope_below(&c, None)), Vec::<&str>::new());
        // Clones are the same route.
        assert_eq!(path(c.envelope_below(&a.clone(), None)), vec!["c", "b"]);
    }

    #[test]
    #[should_panic(expected = "not below")]
    fn envelope_below_panics_for_non_ancestor() {
        let (_, a, c) = build();
        a.envelope_below(&c, None);
    }

    #[test]
    #[should_panic(expected = "not below")]
    fn envelope_below_panics_for_other_tree() {
        let (_, _, c) = build();
        c.envelope_below(&WorkRoute::root(), None);
    }
}
