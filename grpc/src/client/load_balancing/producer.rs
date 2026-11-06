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

//! Subchannel producers: connection-scoped data for LB policies.
//!
//! An LB policy subscribes to a data type (a [`ProducerData`]) on a subchannel
//! with [`watch`](dyn Subchannel::watch).  While the subchannel is READY, the
//! subchannel runs a [`Producer`] for that data type on its connection; the
//! producer publishes results through its [`Publisher`], and the framework
//! delivers them to every subscriber via the subscriber's [`WorkScheduler`].
//!
//! Subscriptions are routed through a [`WatchRegistry`] attribute on the
//! subchannel, which lets a parent policy intercept its children's
//! subscriptions.
//!
//! An LB policy never receives data from a connection after that connection
//! has closed: [`ProducerData::from_work_data`] returns `Ok(None)` for data
//! whose subscription has ended or whose producer has since been dropped or
//! replaced.
//!
//! See the [Subchannels spec, §9].
//!
//! [Subchannels spec, §9]: https://github.com/dfawley/grpc/blob/master/doc/spec/04-client-channel/07-subchannels.md#9-connection-scoped-watchers-and-streams

use std::any::Any;
use std::any::TypeId;
use std::any::type_name;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;

use crate::client::DynInvoke;
use crate::client::load_balancing::WorkData;
use crate::client::load_balancing::WorkScheduler;
use crate::client::load_balancing::subchannel::Subchannel;
use crate::core::Address;
use crate::rt::GrpcRuntime;

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// A data type that LB policies can subscribe to on a subchannel, and that a
/// [`Producer`] publishes.
///
/// The type itself is the value subscribers receive; [`Params`] is what each
/// subscription supplies.  All subscribers to a data type on a subchannel
/// receive the same results; the producer combines their params.
///
/// [`Params`]: ProducerData::Params
pub trait ProducerData: Clone + Debug + Send + Sync + 'static {
    /// The parameters supplied by each subscription.
    type Params: Clone + Debug + Send + Sync + 'static;

    /// Reads a data update delivered to an LB policy's
    /// [`work`](crate::client::load_balancing::LbPolicy::work) method.
    ///
    /// Returns:
    /// - `Ok(Some((subchannel, data)))` for an update of this type, where
    ///   `subchannel` is the subchannel the subscription was made on.
    /// - `Ok(None)` for an update of this type that must be ignored, because
    ///   its subscription has since ended, or the producer that published it
    ///   has since been dropped or replaced (e.g. its connection closed).
    /// - `Err(data)`, returning the work item, if it isn't an update of this
    ///   type.
    ///
    /// Implementations should not override this method.
    #[allow(clippy::type_complexity)]
    fn from_work_data(data: WorkData) -> Result<Option<(Arc<dyn Subchannel>, Self)>, WorkData> {
        let update = data.downcast::<DataUpdate<Self>>()?;
        if !update.is_live() {
            return Ok(None);
        }
        let DataUpdate {
            subchannel, data, ..
        } = *update;
        Ok(Some((subchannel, data)))
    }
}

/// The work item delivered to a subscriber's work scheduler.  Read with
/// [`ProducerData::from_work_data`].
struct DataUpdate<D: ProducerData> {
    subchannel: Arc<dyn Subchannel>,
    data: D,
    /// The subscription this update was delivered to.
    subscription: Arc<SubscriptionState>,
    /// The producer instance that published this update, if it came from a
    /// producer (rather than from an interceptor via [`Subscriber::send`]).
    source: Option<Arc<SourceToken>>,
}

impl<D: ProducerData> DataUpdate<D> {
    fn is_live(&self) -> bool {
        self.subscription.is_live() && self.source.as_ref().is_none_or(|s| s.is_live())
    }
}

impl<D: ProducerData> Debug for DataUpdate<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataUpdate")
            .field("subchannel", &self.subchannel)
            .field("data", &self.data)
            .field("live", &self.is_live())
            .finish()
    }
}

/// Identifies a producer instance.  Invalidated when the producer is dropped
/// or replaced, so data it published but which has not yet been read is
/// discarded.
#[derive(Debug)]
struct SourceToken {
    live: AtomicBool,
}

impl SourceToken {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            live: AtomicBool::new(true),
        })
    }

    fn is_live(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }

    fn invalidate(&self) {
        self.live.store(false, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// Subscribing
// ---------------------------------------------------------------------------

impl dyn Subchannel {
    /// Subscribes to data of type `D` on this subchannel.
    ///
    /// While the subscription is active, updates are delivered to
    /// `work_scheduler`; read them in `work()` with
    /// [`ProducerData::from_work_data`].
    ///
    /// The subscription ends when the subchannel leaves READY (or immediately,
    /// if it is not READY now), or when the returned handle is dropped.  Data
    /// is never delivered from a connection after it has closed.  There is no
    /// data before the first update.
    pub fn watch<D: ProducerData>(
        self: &Arc<Self>,
        params: D::Params,
        work_scheduler: Arc<dyn WorkScheduler>,
    ) -> WatchHandle {
        let subscriber = Subscriber::<D>::new(Arc::downgrade(self), work_scheduler);
        let inner = self.register_watcher(WatchRequest::new(params, subscriber.clone()));
        WatchHandle::new(move || {
            // End the subscription even if an interceptor still holds the
            // subscriber.
            subscriber.close();
            drop(inner);
        })
    }

    /// Routes `request` to this subchannel's [`WatchRegistry`] attribute.
    ///
    /// If the subchannel has no watch registry, the request's subscription
    /// ends immediately.
    pub fn register_watcher(&self, request: WatchRequest) -> WatchHandle {
        match self.get_attribute::<Arc<dyn WatchRegistry>>() {
            Some(registry) => registry.register_watcher(request),
            None => {
                request.close();
                WatchHandle::noop()
            }
        }
    }
}

/// Routes subscriptions.
///
/// A subchannel exposes its watch registry as an attribute of type
/// `Arc<dyn WatchRegistry>`.  A parent policy may intercept the subscriptions
/// made on the subchannels it wraps by returning its own watch registry from
/// its wrapper; it forwards the requests it doesn't handle to the delegate
/// subchannel's [`register_watcher`](dyn Subchannel::register_watcher).
pub trait WatchRegistry: Send + Sync + 'static {
    /// Registers a subscription.  Dropping the returned handle cancels it.
    fn register_watcher(&self, request: WatchRequest) -> WatchHandle;
}

/// Unsubscribes when dropped.
pub struct WatchHandle {
    on_drop: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl WatchHandle {
    /// Creates a handle that calls `on_drop` when it is dropped.
    pub fn new(on_drop: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            on_drop: Some(Box::new(on_drop)),
        }
    }

    /// Creates a handle that does nothing when dropped.
    pub fn noop() -> Self {
        Self { on_drop: None }
    }
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        if let Some(f) = self.on_drop.take() {
            f();
        }
    }
}

impl Debug for WatchHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatchHandle").finish_non_exhaustive()
    }
}

/// A type-erased request to subscribe to some [`ProducerData`] type.
pub struct WatchRequest {
    type_id: TypeId,
    type_name: &'static str,
    subscription: Arc<SubscriptionState>,
    /// A `(D::Params, Subscriber<D>)`.
    inner: Box<dyn Any + Send + Sync>,
}

impl WatchRequest {
    fn new<D: ProducerData>(params: D::Params, subscriber: Subscriber<D>) -> Self {
        Self {
            type_id: TypeId::of::<D>(),
            type_name: type_name::<D>(),
            subscription: subscriber.state.clone(),
            inner: Box::new((params, subscriber)),
        }
    }

    /// Returns the request's params and subscriber if it is a subscription to
    /// `D`, or the request back if not.
    pub fn downcast<D: ProducerData>(self) -> Result<(D::Params, Subscriber<D>), WatchRequest> {
        if self.type_id != TypeId::of::<D>() {
            return Err(self);
        }
        let inner = self
            .inner
            .downcast::<(D::Params, Subscriber<D>)>()
            .expect("WatchRequest payload matches its type id");
        Ok(*inner)
    }

    /// The `TypeId` of the requested data type.
    pub(crate) fn data_type_id(&self) -> TypeId {
        self.type_id
    }

    /// Ends the request's subscription.
    pub(crate) fn close(self) {
        self.subscription.close();
    }
}

impl Debug for WatchRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatchRequest")
            .field("data_type", &self.type_name)
            .field("live", &self.subscription.is_live())
            .finish()
    }
}

/// The shared state of a subscription.
struct SubscriptionState {
    /// The subchannel the subscription was made on, which is reported with
    /// each update.  Weak, as producers are hosted by the subchannel.
    subchannel: Weak<dyn Subchannel>,
    work_scheduler: Arc<dyn WorkScheduler>,
    live: AtomicBool,
}

impl SubscriptionState {
    fn is_live(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }

    fn close(&self) {
        self.live.store(false, Ordering::Release);
    }
}

/// The receiving end of a subscription to `D`.
///
/// Clones refer to the same subscription.  Dropping a subscriber does not end
/// the subscription; [`close`](Subscriber::close) does.
pub struct Subscriber<D: ProducerData> {
    state: Arc<SubscriptionState>,
    _phantom: PhantomData<fn(D)>,
}

impl<D: ProducerData> Clone for Subscriber<D> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<D: ProducerData> Debug for Subscriber<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscriber")
            .field("data_type", &type_name::<D>())
            .field("live", &self.is_live())
            .finish()
    }
}

impl<D: ProducerData> Subscriber<D> {
    fn new(subchannel: Weak<dyn Subchannel>, work_scheduler: Arc<dyn WorkScheduler>) -> Self {
        Self {
            state: Arc::new(SubscriptionState {
                subchannel,
                work_scheduler,
                live: AtomicBool::new(true),
            }),
            _phantom: PhantomData,
        }
    }

    /// Delivers `data` to the subscriber.  Does nothing if the subscription
    /// has ended.
    pub fn send(&self, data: D) {
        self.deliver(data, None);
    }

    /// Ends the subscription.  Updates not yet read by the subscriber are
    /// discarded, and later ones are not delivered.
    pub fn close(&self) {
        self.state.close();
    }

    /// Returns whether the subscription is still active.
    pub fn is_live(&self) -> bool {
        self.state.is_live()
    }

    fn deliver(&self, data: D, source: Option<&Arc<SourceToken>>) {
        if !self.is_live() {
            return;
        }
        // If the subchannel is gone, nobody can be interested in its data.
        let Some(subchannel) = self.state.subchannel.upgrade() else {
            return;
        };
        self.state
            .work_scheduler
            .schedule_work(Some(Box::new(DataUpdate {
                subchannel,
                data,
                subscription: self.state.clone(),
                source: source.cloned(),
            })));
    }
}

// ---------------------------------------------------------------------------
// Producers
// ---------------------------------------------------------------------------

/// Builds producers of a [`ProducerData`] type.
pub trait ProducerBuilder: Send + Sync + Debug + 'static {
    /// The data type produced.
    type Data: ProducerData;

    /// Builds a producer on `options.connection`.  The producer starts its
    /// work here, and stops it when dropped.
    ///
    /// This, and the [`Producer`] methods, are called while the subchannel's
    /// producer state is locked: they must not subscribe to data on the same
    /// subchannel.  Publishing from `build` is fine (see
    /// [`Producer::add_subscription`] for its restrictions).
    fn build(&self, options: ProducerOptions<Self::Data>) -> Box<dyn Producer<Self::Data>>;
}

/// The inputs to [`ProducerBuilder::build`].
#[non_exhaustive]
pub struct ProducerOptions<D: ProducerData> {
    /// The READY connection the producer runs on.
    pub connection: Arc<dyn DynInvoke>,
    /// The current subscriptions.
    pub subscriptions: Vec<(SubscriptionId, D::Params)>,
    /// Publishes the producer's results to the subscribers.
    pub publisher: Publisher<D>,
    /// The address of the subchannel.
    pub address: Address,
    /// The runtime to use for the producer's work.
    pub runtime: GrpcRuntime,
}

impl<D: ProducerData> Debug for ProducerOptions<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerOptions")
            .field("subscriptions", &self.subscriptions)
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

/// A producer of `D` running on one connection.
///
/// It is built when the first subscription to `D` is made while the
/// subchannel is READY; dropped when the subchannel leaves READY or the last
/// subscription is cancelled; and replaced with a new one, built with all
/// current subscriptions, when the subchannel switches connections.
pub trait Producer<D: ProducerData>: Send + 'static {
    /// Called when a subscription is added to a running producer.
    ///
    /// Returns a value to deliver to the new subscriber only, e.g. the
    /// current health state, or `None` to deliver nothing; the subscriber
    /// receives the producer's later publishes either way.  (Subscriptions
    /// given to [`build`](ProducerBuilder::build) receive nothing until the
    /// producer publishes.)
    ///
    /// Publishing is paused for this data type during the call, so the
    /// returned value is delivered before any later publish.  So this must
    /// not publish, nor wait on a lock held by a thread that is publishing.
    fn add_subscription(&mut self, id: SubscriptionId, params: D::Params) -> Option<D>;
    /// Called when a subscription, other than the last, is removed from a
    /// running producer.  (When the last one is removed, the producer is
    /// dropped instead.)
    fn remove_subscription(&mut self, id: SubscriptionId);
}

/// Identifies a subscription to a producer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriptionId(u64);

impl SubscriptionId {
    fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// Publishes a producer's results to its subscribers.
///
/// Once the producer is dropped or replaced, its publisher does nothing.
pub struct Publisher<D: ProducerData> {
    fanout: Arc<Mutex<FanOut<D>>>,
    token: Arc<SourceToken>,
}

impl<D: ProducerData> Clone for Publisher<D> {
    fn clone(&self) -> Self {
        Self {
            fanout: self.fanout.clone(),
            token: self.token.clone(),
        }
    }
}

impl<D: ProducerData> Debug for Publisher<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher")
            .field("data_type", &type_name::<D>())
            .field("live", &self.token.is_live())
            .finish()
    }
}

impl<D: ProducerData> Publisher<D> {
    /// Delivers `update` to the current subscribers.  Nothing is stored:
    /// subscribers added later don't receive it (but see
    /// [`Producer::add_subscription`]).
    pub fn publish(&self, update: D) {
        let fanout = self.fanout.lock();
        if fanout.is_current(&self.token) {
            fanout.deliver(update);
        }
    }
}

/// The subscribers of the running producer of a data type, shared with its
/// publisher.
struct FanOut<D: ProducerData> {
    /// The running producer, or None if none is running.
    token: Option<Arc<SourceToken>>,
    subscribers: BTreeMap<SubscriptionId, Subscriber<D>>,
}

impl<D: ProducerData> FanOut<D> {
    fn is_current(&self, token: &Arc<SourceToken>) -> bool {
        self.token.as_ref().is_some_and(|t| Arc::ptr_eq(t, token))
    }

    fn deliver(&self, data: D) {
        for subscriber in self.subscribers.values() {
            subscriber.deliver(data.clone(), self.token.as_ref());
        }
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Chooses which producer serves each data type.
///
/// Each channel has its own registry, which falls back to
/// [`GLOBAL_PRODUCER_REGISTRY`].
pub struct ProducerRegistry {
    builders: Mutex<HashMap<TypeId, Arc<dyn DynProducerBuilder>>>,
    fallback: Option<&'static ProducerRegistry>,
}

impl ProducerRegistry {
    /// Creates an empty registry with no fallback.
    pub fn new() -> Self {
        Self {
            builders: Mutex::default(),
            fallback: None,
        }
    }

    /// Creates an empty registry that falls back to `fallback` for data types
    /// it has no builder for.
    pub fn with_fallback(fallback: &'static ProducerRegistry) -> Self {
        Self {
            builders: Mutex::default(),
            fallback: Some(fallback),
        }
    }

    /// Registers `builder` for its data type, replacing any builder already
    /// registered for it in this registry.
    pub fn add_builder<B: ProducerBuilder>(&self, builder: B) {
        self.builders
            .lock()
            .insert(TypeId::of::<B::Data>(), Arc::new(BuilderAdapter(builder)));
    }

    /// Returns the builder for the data type `id`, checking the fallback if
    /// this registry has none.
    fn get_builder(&self, id: TypeId) -> Option<Arc<dyn DynProducerBuilder>> {
        if let Some(b) = self.builders.lock().get(&id) {
            return Some(b.clone());
        }
        self.fallback.and_then(|f| f.get_builder(id))
    }
}

impl Default for ProducerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for ProducerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerRegistry")
            .field("builders", &self.builders.lock().values())
            .field("has_fallback", &self.fallback.is_some())
            .finish()
    }
}

/// The global producer registry, used by every channel's registry as its
/// fallback.  Crates outside `grpc` register their producers here.
pub static GLOBAL_PRODUCER_REGISTRY: LazyLock<ProducerRegistry> =
    LazyLock::new(ProducerRegistry::new);

/// A type-erased [`ProducerBuilder`].
trait DynProducerBuilder: Send + Sync + Debug {
    fn new_entry(self: Arc<Self>) -> Box<dyn DynEntry>;
}

#[derive(Debug)]
struct BuilderAdapter<B>(B);

impl<B: ProducerBuilder> DynProducerBuilder for BuilderAdapter<B> {
    fn new_entry(self: Arc<Self>) -> Box<dyn DynEntry> {
        Box::new(Entry::<B> {
            builder: self,
            subscriptions: BTreeMap::new(),
            fanout: Arc::new(Mutex::new(FanOut {
                token: None,
                subscribers: BTreeMap::new(),
            })),
            producer: None,
        })
    }
}

// ---------------------------------------------------------------------------
// ProducerSet: hosting producers in a subchannel
// ---------------------------------------------------------------------------

/// Hosts the producers of a subchannel.
///
/// Tracks the subscriptions for each data type; builds, replaces and drops
/// producers as the subchannel's connection changes; and fans their results
/// out to the subscribers.  The subchannel exposes it as its
/// [`WatchRegistry`] attribute, and calls [`ready`](ProducerSet::ready) when
/// it becomes READY or switches connections, and
/// [`not_ready`](ProducerSet::not_ready) when it leaves READY (before
/// scheduling the state update).
pub(crate) struct ProducerSet {
    weak_self: Weak<ProducerSet>,
    registry: Arc<ProducerRegistry>,
    address: Address,
    runtime: GrpcRuntime,
    state: Mutex<SetState>,
}

struct SetState {
    /// The READY connection, or None if not READY.
    connection: Option<Arc<dyn DynInvoke>>,
    /// The subscriptions and producer for each data type with at least one
    /// subscription.
    entries: HashMap<TypeId, Box<dyn DynEntry>>,
}

/// What a data type's producer is built with, apart from its subscriptions
/// and publisher.
struct BuildContext<'a> {
    connection: &'a Arc<dyn DynInvoke>,
    address: &'a Address,
    runtime: &'a GrpcRuntime,
}

/// Producers that were stopped, to be dropped after the set's lock is
/// released (so a producer's `Drop` can't deadlock with the set).
type Garbage = Vec<Box<dyn Send>>;

impl ProducerSet {
    pub(crate) fn new(
        registry: Arc<ProducerRegistry>,
        address: Address,
        runtime: GrpcRuntime,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak_self| Self {
            weak_self: weak_self.clone(),
            registry,
            address,
            runtime,
            state: Mutex::new(SetState {
                connection: None,
                entries: HashMap::new(),
            }),
        })
    }

    /// Called when the subchannel becomes READY on `connection`, or switches
    /// to `connection` while staying READY.  Replaces any running producers
    /// with ones built on `connection` with all current subscriptions.
    pub(crate) fn ready(&self, connection: Arc<dyn DynInvoke>) {
        let mut garbage = Garbage::new();
        {
            let mut state = self.state.lock();
            let state = &mut *state;
            let connection = &*state.connection.insert(connection);
            let ctx = BuildContext {
                connection,
                address: &self.address,
                runtime: &self.runtime,
            };
            for entry in state.entries.values_mut() {
                entry.restart(&ctx, &mut garbage);
            }
        }
        drop(garbage);
    }

    /// Called when the subchannel leaves READY.  Ends all subscriptions and
    /// drops all producers.
    pub(crate) fn not_ready(&self) {
        let mut garbage = Garbage::new();
        {
            let mut state = self.state.lock();
            state.connection = None;
            for (_, mut entry) in state.entries.drain() {
                entry.end_all(&mut garbage);
            }
        }
        drop(garbage);
    }

    fn remove(&self, type_id: TypeId, id: SubscriptionId) {
        let mut garbage = Garbage::new();
        {
            let mut state = self.state.lock();
            if let Some(entry) = state.entries.get_mut(&type_id) {
                entry.remove(id, &mut garbage);
                if entry.is_empty() {
                    state.entries.remove(&type_id);
                }
            }
        }
        drop(garbage);
    }
}

impl WatchRegistry for ProducerSet {
    fn register_watcher(&self, request: WatchRequest) -> WatchHandle {
        let type_id = request.data_type_id();
        let mut state = self.state.lock();
        let state = &mut *state;
        let Some(connection) = &state.connection else {
            // Not READY: the subscription has already ended.
            request.close();
            return WatchHandle::noop();
        };
        let entry = match state.entries.entry(type_id) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let Some(builder) = self.registry.get_builder(type_id) else {
                    eprintln!(
                        "no producer registered for data type {}; ending subscription",
                        request.type_name
                    );
                    request.close();
                    return WatchHandle::noop();
                };
                e.insert(builder.new_entry())
            }
        };
        let id = SubscriptionId::next();
        let ctx = BuildContext {
            connection,
            address: &self.address,
            runtime: &self.runtime,
        };
        entry.add(id, request, &ctx);
        let weak_self = self.weak_self.clone();
        WatchHandle::new(move || {
            if let Some(set) = weak_self.upgrade() {
                set.remove(type_id, id);
            }
        })
    }
}

impl Debug for ProducerSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        f.debug_struct("ProducerSet")
            .field("address", &self.address)
            .field("ready", &state.connection.is_some())
            .field("data_types", &state.entries.len())
            .finish()
    }
}

/// The subscriptions and producer of one data type in a [`ProducerSet`].
trait DynEntry: Send {
    /// Adds a subscription while READY, building the producer if needed.
    fn add(&mut self, id: SubscriptionId, request: WatchRequest, ctx: &BuildContext<'_>);
    /// Removes a subscription, ending it, and drops the producer if it was the
    /// last.
    fn remove(&mut self, id: SubscriptionId, garbage: &mut Garbage);
    /// Replaces the producer (if any) with one built on `ctx.connection`.
    fn restart(&mut self, ctx: &BuildContext<'_>, garbage: &mut Garbage);
    /// Ends all subscriptions and drops the producer.
    fn end_all(&mut self, garbage: &mut Garbage);
    fn is_empty(&self) -> bool;
}

/// A subscription's params and subscriber.
type SubscriptionEntry<D> = (<D as ProducerData>::Params, Subscriber<D>);

struct Entry<B: ProducerBuilder> {
    builder: Arc<BuilderAdapter<B>>,
    subscriptions: BTreeMap<SubscriptionId, SubscriptionEntry<B::Data>>,
    fanout: Arc<Mutex<FanOut<B::Data>>>,
    producer: Option<Box<dyn Producer<B::Data>>>,
}

impl<B: ProducerBuilder> Entry<B> {
    /// Builds a producer with all current subscriptions.  No producer may be
    /// running.
    fn start(&mut self, ctx: &BuildContext<'_>) {
        debug_assert!(self.producer.is_none());
        let token = SourceToken::new();
        {
            // Install the subscribers before building, so data the producer
            // publishes from `build` is delivered.
            let mut fanout = self.fanout.lock();
            fanout.token = Some(token.clone());
            fanout.subscribers = self
                .subscriptions
                .iter()
                .map(|(id, (_, s))| (*id, s.clone()))
                .collect();
        }
        let options = ProducerOptions {
            connection: ctx.connection.clone(),
            subscriptions: self
                .subscriptions
                .iter()
                .map(|(id, (p, _))| (*id, p.clone()))
                .collect(),
            publisher: Publisher {
                fanout: self.fanout.clone(),
                token,
            },
            address: ctx.address.clone(),
            runtime: ctx.runtime.clone(),
        };
        self.producer = Some(self.builder.0.build(options));
    }

    /// Stops the running producer, if any.  Data it published but which
    /// hasn't been read yet is discarded.
    fn stop(&mut self, garbage: &mut Garbage) {
        {
            let mut fanout = self.fanout.lock();
            if let Some(token) = fanout.token.take() {
                token.invalidate();
            }
            fanout.subscribers.clear();
        }
        if let Some(producer) = self.producer.take() {
            garbage.push(Box::new(producer));
        }
    }
}

impl<B: ProducerBuilder> DynEntry for Entry<B> {
    fn add(&mut self, id: SubscriptionId, request: WatchRequest, ctx: &BuildContext<'_>) {
        let (params, subscriber) = request
            .downcast::<B::Data>()
            .unwrap_or_else(|_| panic!("WatchRequest routed to the wrong producer"));
        self.subscriptions
            .insert(id, (params.clone(), subscriber.clone()));
        match &mut self.producer {
            None => self.start(ctx),
            Some(producer) => {
                // Hold the fanout lock across the call, so no publish can
                // come between the returned value and the subscriber joining
                // the fanout.
                let mut fanout = self.fanout.lock();
                if let Some(initial) = producer.add_subscription(id, params) {
                    subscriber.deliver(initial, fanout.token.as_ref());
                }
                fanout.subscribers.insert(id, subscriber);
            }
        }
    }

    fn remove(&mut self, id: SubscriptionId, garbage: &mut Garbage) {
        let Some((_, subscriber)) = self.subscriptions.remove(&id) else {
            return;
        };
        subscriber.close();
        if self.subscriptions.is_empty() {
            self.stop(garbage);
            return;
        }
        self.fanout.lock().subscribers.remove(&id);
        if let Some(producer) = &mut self.producer {
            producer.remove_subscription(id);
        }
    }

    fn restart(&mut self, ctx: &BuildContext<'_>, garbage: &mut Garbage) {
        self.stop(garbage);
        if !self.subscriptions.is_empty() {
            self.start(ctx);
        }
    }

    fn end_all(&mut self, garbage: &mut Garbage) {
        for (_, (_, subscriber)) in std::mem::take(&mut self.subscriptions) {
            subscriber.close();
        }
        self.stop(garbage);
    }

    fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::any::TypeId;
    use std::hash::Hash;
    use std::sync::Arc;
    use std::sync::mpsc;

    use parking_lot::Mutex;

    use super::*;
    use crate::client::DynInvoke;
    use crate::client::load_balancing::subchannel::private::Sealed;
    use crate::client::load_balancing::test_utils::TestWorkScheduler;
    use crate::client::test_util::NopInvoker;

    // ---- Test data type and producer ----

    #[derive(Clone, Debug, PartialEq)]
    struct TestData(String);

    impl ProducerData for TestData {
        type Params = u32;
    }

    #[derive(Debug, PartialEq)]
    enum Event {
        Build {
            subscriptions: Vec<(SubscriptionId, u32)>,
            connection: usize,
        },
        Add(SubscriptionId, u32),
        Remove(SubscriptionId),
        Drop(usize),
    }

    fn conn_id(c: &Arc<dyn DynInvoke>) -> usize {
        Arc::as_ptr(c) as *const () as usize
    }

    /// Records producer events, and keeps each built producer's publisher.
    #[derive(Debug, Default)]
    struct Recorder {
        events: Mutex<Vec<Event>>,
        publishers: Mutex<Vec<Publisher<TestData>>>,
        /// What producers return from `add_subscription`.
        initial: Mutex<Option<TestData>>,
    }

    impl Recorder {
        fn take_events(&self) -> Vec<Event> {
            std::mem::take(&mut *self.events.lock())
        }
        fn publisher(&self, i: usize) -> Publisher<TestData> {
            self.publishers.lock()[i].clone()
        }
    }

    #[derive(Debug)]
    struct TestBuilder {
        recorder: Arc<Recorder>,
        /// If set, the producer publishes this from `build`.
        publish_on_build: Option<TestData>,
    }

    struct TestProducer {
        recorder: Arc<Recorder>,
        connection: usize,
    }

    impl ProducerBuilder for TestBuilder {
        type Data = TestData;
        fn build(&self, options: ProducerOptions<TestData>) -> Box<dyn Producer<TestData>> {
            let connection = conn_id(&options.connection);
            self.recorder.events.lock().push(Event::Build {
                subscriptions: options.subscriptions,
                connection,
            });
            if let Some(d) = &self.publish_on_build {
                options.publisher.publish(d.clone());
            }
            self.recorder.publishers.lock().push(options.publisher);
            Box::new(TestProducer {
                recorder: self.recorder.clone(),
                connection,
            })
        }
    }

    impl Producer<TestData> for TestProducer {
        fn add_subscription(&mut self, id: SubscriptionId, params: u32) -> Option<TestData> {
            self.recorder.events.lock().push(Event::Add(id, params));
            self.recorder.initial.lock().clone()
        }
        fn remove_subscription(&mut self, id: SubscriptionId) {
            self.recorder.events.lock().push(Event::Remove(id));
        }
    }

    impl Drop for TestProducer {
        fn drop(&mut self) {
            self.recorder
                .events
                .lock()
                .push(Event::Drop(self.connection));
        }
    }

    // ---- Test subchannel exposing a watch registry attribute ----

    struct TestSubchannel {
        registry: Option<Arc<dyn WatchRegistry>>,
    }

    impl Sealed for TestSubchannel {}

    impl Subchannel for TestSubchannel {
        fn address(&self) -> Address {
            Address::default()
        }
        fn get_attribute_dyn(&self, id: TypeId) -> Option<&dyn Any> {
            if id == TypeId::of::<Arc<dyn WatchRegistry>>() {
                return self.registry.as_ref().map(|r| r as &dyn Any);
            }
            None
        }
        fn connect(&self) {}
    }

    impl Hash for TestSubchannel {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            (self as *const Self as usize).hash(state);
        }
    }
    impl PartialEq for TestSubchannel {
        fn eq(&self, other: &Self) -> bool {
            std::ptr::eq(self, other)
        }
    }
    impl Eq for TestSubchannel {}

    // ---- Fixture ----

    struct Fixture {
        recorder: Arc<Recorder>,
        set: Arc<ProducerSet>,
        subchannel: Arc<dyn Subchannel>,
    }

    fn fixture_with(publish_on_build: Option<TestData>) -> Fixture {
        let recorder = Arc::new(Recorder::default());
        let registry = Arc::new(ProducerRegistry::new());
        registry.add_builder(TestBuilder {
            recorder: recorder.clone(),
            publish_on_build,
        });
        let set = ProducerSet::new(registry, Address::default(), GrpcRuntime::default());
        let subchannel: Arc<dyn Subchannel> = Arc::new(TestSubchannel {
            registry: Some(set.clone()),
        });
        Fixture {
            recorder,
            set,
            subchannel,
        }
    }

    fn fixture() -> Fixture {
        fixture_with(None)
    }

    fn new_conn() -> Arc<dyn DynInvoke> {
        Arc::new(NopInvoker)
    }

    fn work_scheduler() -> (Arc<dyn WorkScheduler>, mpsc::Receiver<Option<WorkData>>) {
        let (tx_work, rx) = mpsc::channel();
        (Arc::new(TestWorkScheduler { tx_work }), rx)
    }

    /// Reads all pending updates from `rx` as `TestData`, with `None` for
    /// updates that must be ignored.
    fn drain(rx: &mpsc::Receiver<Option<WorkData>>) -> Vec<Option<TestData>> {
        rx.try_iter()
            .map(|w| {
                TestData::from_work_data(w.expect("work data"))
                    .expect("TestData update")
                    .map(|(_, d)| d)
            })
            .collect()
    }

    fn data(s: &str) -> TestData {
        TestData(s.to_string())
    }

    fn sub_ids(events: &[Event]) -> Vec<SubscriptionId> {
        match &events[0] {
            Event::Build { subscriptions, .. } => subscriptions.iter().map(|(id, _)| *id).collect(),
            e => panic!("expected Build, got {e:?}"),
        }
    }

    // ---- Tests ----

    #[test]
    fn builds_on_first_subscription_and_fans_out() {
        let f = fixture();
        let conn = new_conn();
        f.set.ready(conn.clone());
        let (ws1, rx1) = work_scheduler();
        let (ws2, rx2) = work_scheduler();

        let h1 = f.subchannel.watch::<TestData>(5, ws1);
        let events = f.recorder.take_events();
        assert_eq!(events.len(), 1);
        let id1 = sub_ids(&events)[0];
        assert_eq!(
            events[0],
            Event::Build {
                subscriptions: vec![(id1, 5)],
                connection: conn_id(&conn)
            }
        );

        let h2 = f.subchannel.watch::<TestData>(7, ws2);
        let events = f.recorder.take_events();
        let [Event::Add(id2, 7)] = events[..] else {
            panic!("unexpected events {events:?}");
        };
        assert_ne!(id1, id2);

        f.recorder.publisher(0).publish(data("a"));
        assert_eq!(drain(&rx1), vec![Some(data("a"))]);
        assert_eq!(drain(&rx2), vec![Some(data("a"))]);

        // Removing a non-last subscription.
        drop(h1);
        assert_eq!(f.recorder.take_events(), vec![Event::Remove(id1)]);
        f.recorder.publisher(0).publish(data("b"));
        assert!(drain(&rx1).is_empty());
        assert_eq!(drain(&rx2), vec![Some(data("b"))]);

        // Removing the last subscription drops the producer.
        drop(h2);
        assert_eq!(f.recorder.take_events(), vec![Event::Drop(conn_id(&conn))]);
        f.recorder.publisher(0).publish(data("c"));
        assert!(drain(&rx2).is_empty());
    }

    #[test]
    fn update_reports_watched_subchannel() {
        let f = fixture();
        f.set.ready(new_conn());
        let (ws, rx) = work_scheduler();
        let _h = f.subchannel.watch::<TestData>(1, ws);
        f.recorder.publisher(0).publish(data("a"));
        let (sc, d) = TestData::from_work_data(rx.try_recv().unwrap().unwrap())
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&sc, &f.subchannel));
        assert_eq!(d, data("a"));
    }

    #[test]
    fn from_work_data_returns_other_work() {
        #[derive(Debug)]
        struct Other;
        let w: WorkData = Box::new(Other);
        let w = TestData::from_work_data(w).unwrap_err();
        assert!(w.downcast_ref::<Other>().is_some());

        // An update of another data type is also returned.
        let f = fixture();
        f.set.ready(new_conn());
        let (ws, rx) = work_scheduler();
        let _h = f.subchannel.watch::<TestData>(1, ws);
        f.recorder.publisher(0).publish(data("a"));
        #[derive(Clone, Debug)]
        struct OtherData;
        impl ProducerData for OtherData {
            type Params = ();
        }
        let w = rx.try_recv().unwrap().unwrap();
        let w = OtherData::from_work_data(w).unwrap_err();
        assert!(TestData::from_work_data(w).unwrap().is_some());
    }

    #[test]
    fn not_ready_ends_subscriptions_and_discards_pending_data() {
        let f = fixture();
        let conn = new_conn();
        f.set.ready(conn.clone());
        let (ws, rx) = work_scheduler();
        let h = f.subchannel.watch::<TestData>(1, ws);
        f.recorder.take_events();

        // Published, but not yet read when the connection closes.
        f.recorder.publisher(0).publish(data("a"));
        f.set.not_ready();
        assert_eq!(f.recorder.take_events(), vec![Event::Drop(conn_id(&conn))]);
        assert_eq!(drain(&rx), vec![None]);

        // The old publisher does nothing.
        f.recorder.publisher(0).publish(data("b"));
        assert!(drain(&rx).is_empty());

        // Becoming READY again doesn't resume the ended subscription.
        f.set.ready(new_conn());
        assert!(f.recorder.take_events().is_empty());
        drop(h);
        assert!(f.recorder.take_events().is_empty());
    }

    #[test]
    fn connection_switch_replaces_producer() {
        let f = fixture();
        let conn1 = new_conn();
        let conn2 = new_conn();
        f.set.ready(conn1.clone());
        let (ws1, rx1) = work_scheduler();
        let (ws2, rx2) = work_scheduler();
        let _h1 = f.subchannel.watch::<TestData>(1, ws1);
        let _h2 = f.subchannel.watch::<TestData>(2, ws2);
        let events = f.recorder.take_events();
        let id1 = sub_ids(&events)[0];
        let Event::Add(id2, 2) = events[1] else {
            panic!("unexpected events {events:?}");
        };

        f.recorder.publisher(0).publish(data("old"));
        *f.recorder.initial.lock() = Some(data("old"));
        f.set.ready(conn2.clone());
        // The old producer is dropped after its replacement is built (once the
        // set's lock is released), but it is invalidated first.
        assert_eq!(
            f.recorder.take_events(),
            vec![
                Event::Build {
                    subscriptions: vec![(id1, 1), (id2, 2)],
                    connection: conn_id(&conn2)
                },
                Event::Drop(conn_id(&conn1)),
            ]
        );
        // Data queued from the old connection is discarded.
        assert_eq!(drain(&rx1), vec![None]);
        assert_eq!(drain(&rx2), vec![None]);

        // The old publisher does nothing; the new one delivers.
        f.recorder.publisher(0).publish(data("stale"));
        f.recorder.publisher(1).publish(data("new"));
        assert_eq!(drain(&rx1), vec![Some(data("new"))]);
        assert_eq!(drain(&rx2), vec![Some(data("new"))]);

        // Later subscribers get what the new producer returns from
        // add_subscription.
        *f.recorder.initial.lock() = Some(data("current"));
        let (ws3, rx3) = work_scheduler();
        let _h3 = f.subchannel.watch::<TestData>(3, ws3);
        assert_eq!(drain(&rx3), vec![Some(data("current"))]);
    }

    #[test]
    fn add_subscription_value_goes_to_new_subscriber_only() {
        let f = fixture();
        f.set.ready(new_conn());
        let (ws1, rx1) = work_scheduler();
        let _h1 = f.subchannel.watch::<TestData>(1, ws1);
        let p = f.recorder.publisher(0);

        // Published values aren't stored for later subscribers.
        p.publish(data("p"));
        assert_eq!(drain(&rx1), vec![Some(data("p"))]);
        let (ws2, rx2) = work_scheduler();
        let _h2 = f.subchannel.watch::<TestData>(2, ws2);
        assert!(drain(&rx2).is_empty());

        // The value returned by add_subscription goes only to the new
        // subscriber, before anything published later.
        *f.recorder.initial.lock() = Some(data("initial"));
        let (ws3, rx3) = work_scheduler();
        let _h3 = f.subchannel.watch::<TestData>(3, ws3);
        p.publish(data("next"));
        assert_eq!(drain(&rx3), vec![Some(data("initial")), Some(data("next"))]);
        assert_eq!(drain(&rx1), vec![Some(data("next"))]);
        assert_eq!(drain(&rx2), vec![Some(data("next"))]);
    }

    #[test]
    fn publish_during_build_is_delivered() {
        let f = fixture_with(Some(data("initial")));
        f.set.ready(new_conn());
        let (ws, rx) = work_scheduler();
        let _h = f.subchannel.watch::<TestData>(1, ws);
        assert_eq!(drain(&rx), vec![Some(data("initial"))]);
    }

    #[test]
    fn dropping_handle_discards_pending_data() {
        let f = fixture();
        f.set.ready(new_conn());
        let (ws, rx) = work_scheduler();
        let (ws2, _rx2) = work_scheduler();
        let h = f.subchannel.watch::<TestData>(1, ws);
        let _h2 = f.subchannel.watch::<TestData>(2, ws2);
        f.recorder.publisher(0).publish(data("a"));
        drop(h);
        assert_eq!(drain(&rx), vec![None]);
    }

    #[test]
    fn subscribe_while_not_ready_ends_immediately() {
        let f = fixture();
        let (ws, rx) = work_scheduler();
        let _h = f.subchannel.watch::<TestData>(1, ws);
        f.set.ready(new_conn());
        assert!(f.recorder.take_events().is_empty());
        assert!(drain(&rx).is_empty());
    }

    #[test]
    fn unregistered_data_type_ends_subscription() {
        let f = fixture();
        f.set.ready(new_conn());
        #[derive(Clone, Debug)]
        struct Unregistered;
        impl ProducerData for Unregistered {
            type Params = ();
        }
        let (ws, _rx) = work_scheduler();
        let _h = f.subchannel.watch::<Unregistered>((), ws);
        assert!(f.recorder.take_events().is_empty());
    }

    #[test]
    fn no_watch_registry_ends_subscription() {
        let subchannel: Arc<dyn Subchannel> = Arc::new(TestSubchannel { registry: None });
        let (ws, rx) = work_scheduler();
        let subscriber = Subscriber::<TestData>::new(Arc::downgrade(&subchannel), ws);
        let _h = subchannel.register_watcher(WatchRequest::new(1, subscriber.clone()));
        assert!(!subscriber.is_live());
        subscriber.send(data("a"));
        assert!(drain(&rx).is_empty());
    }

    #[test]
    fn registry_fallback() {
        static GLOBAL: LazyLock<ProducerRegistry> = LazyLock::new(ProducerRegistry::new);
        let recorder = Arc::new(Recorder::default());
        GLOBAL.add_builder(TestBuilder {
            recorder: recorder.clone(),
            publish_on_build: None,
        });
        let local = ProducerRegistry::with_fallback(&GLOBAL);
        assert!(local.get_builder(TypeId::of::<TestData>()).is_some());
        assert!(
            ProducerRegistry::new()
                .get_builder(TypeId::of::<TestData>())
                .is_none()
        );

        // A local builder takes precedence.
        let local_recorder = Arc::new(Recorder::default());
        local.add_builder(TestBuilder {
            recorder: local_recorder.clone(),
            publish_on_build: None,
        });
        let set = ProducerSet::new(Arc::new(local), Address::default(), GrpcRuntime::default());
        let subchannel: Arc<dyn Subchannel> = Arc::new(TestSubchannel {
            registry: Some(set.clone()),
        });
        set.ready(new_conn());
        let (ws, _rx) = work_scheduler();
        let _h = subchannel.watch::<TestData>(1, ws);
        assert_eq!(local_recorder.take_events().len(), 1);
        assert!(recorder.take_events().is_empty());
    }

    /// An interceptor that substitutes the data delivered to its children,
    /// like outlier detection does for health.
    struct Interceptor {
        delegate: Arc<dyn Subchannel>,
        children: Arc<Mutex<Vec<Subscriber<TestData>>>>,
        work_scheduler: Arc<dyn WorkScheduler>,
    }

    impl WatchRegistry for Interceptor {
        fn register_watcher(&self, request: WatchRequest) -> WatchHandle {
            let (params, child) = match request.downcast::<TestData>() {
                Ok(x) => x,
                Err(other) => return self.delegate.register_watcher(other),
            };
            let inner = self
                .delegate
                .watch::<TestData>(params, self.work_scheduler.clone());
            self.children.lock().push(child);
            let children = self.children.clone();
            WatchHandle::new(move || {
                drop(inner);
                children.lock().clear();
            })
        }
    }

    #[test]
    fn interception() {
        let f = fixture();
        f.set.ready(new_conn());
        let (interceptor_ws, interceptor_rx) = work_scheduler();
        let children = Arc::new(Mutex::new(Vec::new()));
        let wrapper: Arc<dyn Subchannel> = Arc::new(TestSubchannel {
            registry: Some(Arc::new(Interceptor {
                delegate: f.subchannel.clone(),
                children: children.clone(),
                work_scheduler: interceptor_ws,
            })),
        });

        let (ws, rx) = work_scheduler();
        let h = wrapper.watch::<TestData>(1, ws);
        f.recorder.publisher(0).publish(data("real"));
        // The interceptor receives the real data and substitutes it.
        let (sc, real) = TestData::from_work_data(interceptor_rx.try_recv().unwrap().unwrap())
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&sc, &f.subchannel));
        assert_eq!(real, data("real"));
        children.lock()[0].send(data("substituted"));
        let (sc, d) = TestData::from_work_data(rx.try_recv().unwrap().unwrap())
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&sc, &wrapper));
        assert_eq!(d, data("substituted"));

        // Dropping the handle ends the subscription, even though the
        // interceptor still holds (a clone of) the subscriber.
        let child = children.lock()[0].clone();
        child.send(data("pending"));
        drop(h);
        assert!(!child.is_live());
        assert_eq!(drain(&rx), vec![None]);
        child.send(data("late"));
        assert!(drain(&rx).is_empty());
        // The interceptor's own subscription was dropped too.
        assert!(matches!(
            f.recorder.take_events().last(),
            Some(Event::Drop(_))
        ));
    }

    #[test]
    fn subscriber_close() {
        let f = fixture();
        let (ws, rx) = work_scheduler();
        let s = Subscriber::<TestData>::new(Arc::downgrade(&f.subchannel), ws);
        let s2 = s.clone();
        s.send(data("a"));
        drop(s2); // Dropping a clone doesn't close.
        assert!(s.is_live());
        assert_eq!(drain(&rx), vec![Some(data("a"))]);
        s.send(data("b"));
        s.close();
        assert_eq!(drain(&rx), vec![None]);
    }
}
