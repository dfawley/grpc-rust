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

use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use crate::StatusCodeError;
use crate::StatusError;
use crate::client::RequestHeaders;
use crate::client::load_balancing::ChannelController;
use crate::client::load_balancing::LbPolicy;
use crate::client::load_balancing::LbState;
use crate::client::load_balancing::PickResult;
use crate::client::load_balancing::Picker;
use crate::client::load_balancing::SubchannelUpdate;
use crate::client::load_balancing::WorkData;
use crate::client::load_balancing::WorkScheduler;
use crate::client::load_balancing::subchannel::ForwardingSubchannel;
use crate::client::load_balancing::subchannel::Subchannel;
use crate::client::load_balancing::subchannel::SubchannelState;
use crate::client::load_balancing::subchannel::WeakSubchannel;
use crate::client::name_resolution::ResolverUpdate;
use crate::core::Address;

/// Implements subchannel sharing for T.  Whenever T creates a subchannel, this
#[derive(Debug)]
struct InternalSubchannelUpdate(SubchannelUpdate);

#[derive(Debug)]
struct SubchannelReleased(Arc<dyn Subchannel>);

/// Implements subchannel sharing for T.  Whenever T creates a subchannel, this
/// policy wraps what the channel returns, and if another subchannel is created
/// for the same address, the first subchannel will be reused to back the second
/// subchannel.
#[derive(Debug)]
pub(crate) struct SubchannelSharing<T> {
    delegate: T,
    work_scheduler: Arc<dyn WorkScheduler>,
    inner: Inner,
}

impl<T> SubchannelSharing<T> {
    pub(crate) fn new(delegate: T, work_scheduler: Arc<dyn WorkScheduler>) -> Self {
        Self {
            delegate,
            work_scheduler,
            inner: Inner {
                subchannels_by_address: HashMap::new(),
                subchannels_int_to_ext: HashMap::new(),
            },
        }
    }
}

type ExtSubchannelMap = HashMap<WeakSubchannel, Arc<dyn WorkScheduler>>;

#[derive(Debug)]
struct Inner {
    subchannels_by_address: HashMap<Address, Arc<dyn Subchannel>>,
    subchannels_int_to_ext: HashMap<Arc<dyn Subchannel>, (SubchannelState, ExtSubchannelMap)>,
}

#[derive(Debug)]
struct SubchannelUpdateForwarder {
    parent: Arc<dyn WorkScheduler>,
}

impl WorkScheduler for SubchannelUpdateForwarder {
    fn schedule_work(&self, data: Option<WorkData>) {
        let Some(data) = data else {
            self.parent.schedule_work(None);
            return;
        };
        match data.downcast::<SubchannelUpdate>() {
            Ok(update) => {
                self.parent
                    .schedule_work(Some(Box::new(InternalSubchannelUpdate(*update))));
            }
            Err(data) => {
                self.parent.schedule_work(Some(data));
            }
        }
    }
}

impl<T: LbPolicy> LbPolicy for SubchannelSharing<T> {
    type LbConfig = T::LbConfig;

    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&T::LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), String> {
        let mut channel_controller = SharingChannelController {
            inner: &mut self.inner,
            work_scheduler: &self.work_scheduler,
            delegate: channel_controller,
        };
        self.delegate
            .resolver_update(update, config, &mut channel_controller)
    }

    fn work(&mut self, data: Option<WorkData>, channel_controller: &mut dyn ChannelController) {
        if let Some(data) = data {
            let data = match data.downcast::<InternalSubchannelUpdate>() {
                Ok(internal_update) => {
                    let update = internal_update.0;
                    if let Some((state, ext_map)) =
                        self.inner.subchannels_int_to_ext.get_mut(&update.subchannel)
                    {
                        *state = update.state.clone();
                        let mut targets = Vec::new();
                        ext_map.retain(|weak, ws| {
                            if let Some(sc) = weak.upgrade() {
                                targets.push((sc, ws.clone()));
                                true
                            } else {
                                false
                            }
                        });
                        for (sc, ws) in targets {
                            ws.schedule_work(Some(Box::new(SubchannelUpdate {
                                subchannel: sc,
                                state: update.state.clone(),
                            })));
                        }
                    }
                    return;
                }
                Err(data) => data,
            };

            let data = match data.downcast::<SubchannelReleased>() {
                Ok(released) => {
                    let int_sc = released.0;
                    if let Some(entry) = self.inner.subchannels_int_to_ext.get_mut(&int_sc) {
                        entry.1.retain(|weak, _| weak.strong_count() != 0);
                        if entry.1.is_empty() {
                            self.inner.subchannels_int_to_ext.remove(&int_sc);
                            self.inner.subchannels_by_address.remove(&int_sc.address());
                        }
                    }
                    return;
                }
                Err(data) => data,
            };

            let mut channel_controller = SharingChannelController {
                inner: &mut self.inner,
                work_scheduler: &self.work_scheduler,
                delegate: channel_controller,
            };
            self.delegate.work(Some(data), &mut channel_controller);
            return;
        }

        let mut channel_controller = SharingChannelController {
            inner: &mut self.inner,
            work_scheduler: &self.work_scheduler,
            delegate: channel_controller,
        };
        self.delegate.work(None, &mut channel_controller);
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        let mut channel_controller = SharingChannelController {
            inner: &mut self.inner,
            work_scheduler: &self.work_scheduler,
            delegate: channel_controller,
        };
        self.delegate.exit_idle(&mut channel_controller);
    }
}

#[derive(Debug)]
struct SharedSubchannel {
    delegate: Arc<dyn Subchannel>,
    work_scheduler: Arc<dyn WorkScheduler>,
}

impl PartialEq for SharedSubchannel {
    fn eq(&self, other: &Self) -> bool {
        // self.real_subchannel == other.real_subchannel hits
        // https://github.com/rust-lang/rust/issues/31740 bug and
        // &self.real_subchannel == &other.real_subchannel makes clippy
        // complain.
        PartialEq::eq(&self.delegate, &other.delegate)
    }
}

impl Eq for SharedSubchannel {}

impl Hash for SharedSubchannel {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.delegate.hash(state);
    }
}

impl ForwardingSubchannel for SharedSubchannel {
    fn delegate(&self) -> &Arc<dyn Subchannel> {
        &self.delegate
    }
}

impl Drop for SharedSubchannel {
    fn drop(&mut self) {
        self.work_scheduler
            .schedule_work(Some(Box::new(SubchannelReleased(self.delegate.clone()))));
    }
}

struct SharingChannelController<'a> {
    inner: &'a mut Inner,
    work_scheduler: &'a Arc<dyn WorkScheduler>,
    delegate: &'a mut dyn ChannelController,
}

impl ChannelController for SharingChannelController<'_> {
    fn new_subchannel(
        &mut self,
        address: &Address,
        work_scheduler: Arc<dyn WorkScheduler>,
    ) -> (Arc<dyn Subchannel>, SubchannelState) {
        let (int_subchannel, initial_state) = match self.inner.subchannels_by_address.get(address) {
            Some(sc) => {
                let sc = sc.clone();
                let state = self
                    .inner
                    .subchannels_int_to_ext
                    .get(&sc)
                    .map(|(state, _)| state.clone())
                    .unwrap_or_else(SubchannelState::idle);
                (sc, state)
            }
            None => {
                let forwarder = Arc::new(SubchannelUpdateForwarder {
                    parent: self.work_scheduler.clone(),
                });
                let (new_sc, state) = self.delegate.new_subchannel(address, forwarder);
                self.inner
                    .subchannels_by_address
                    .insert(address.clone(), new_sc.clone());
                (new_sc, state)
            }
        };

        let ext_subchannel: Arc<dyn Subchannel> = Arc::new(SharedSubchannel {
            delegate: int_subchannel.clone(),
            work_scheduler: self.work_scheduler.clone(),
        });

        let entry = self
            .inner
            .subchannels_int_to_ext
            .entry(int_subchannel)
            .or_insert_with(|| (initial_state, HashMap::new()));

        entry.1.insert((&ext_subchannel).into(), work_scheduler);

        (ext_subchannel, entry.0.clone())
    }

    fn update_picker(&mut self, mut update: LbState) {
        update.picker = UnwrapPicker::new_arc(update.picker);
        self.delegate.update_picker(update);
    }

    fn request_resolution(&mut self) {
        self.delegate.request_resolution();
    }
}

#[derive(Debug)]
struct UnwrapPicker {
    delegate: Arc<dyn Picker>,
}

impl UnwrapPicker {
    fn new_arc(delegate: Arc<dyn Picker>) -> Arc<Self> {
        Arc::new(Self { delegate })
    }
}

impl Picker for UnwrapPicker {
    fn pick(&self, request: &RequestHeaders) -> PickResult {
        let result = self.delegate.pick(request);
        match result {
            PickResult::Pick(mut pick) => {
                let Some(subchannel) = pick.subchannel.downcast_ref::<SharedSubchannel>() else {
                    return PickResult::Fail(StatusError::new(
                        StatusCodeError::Internal,
                        format!(
                            "received unexpected subchannel type: {:?}",
                            pick.subchannel.type_id()
                        ),
                    ));
                };
                pick.subchannel = subchannel.delegate.clone();
                PickResult::Pick(pick)
            }
            _ => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::mpsc;

    use super::*;
    use crate::client::ConnectivityState;
    use crate::client::load_balancing::LbPolicy;
    use crate::client::load_balancing::LbPolicyOptions;
    use crate::client::load_balancing::Pick;
    use crate::client::load_balancing::PickResult;
    use crate::client::load_balancing::Picker;
    use crate::client::load_balancing::subchannel::SubchannelState;
    use crate::client::load_balancing::test_utils::NopWorkScheduler;
    use crate::client::load_balancing::test_utils::StubPolicy;
    use crate::client::load_balancing::test_utils::StubPolicyFuncs;
    use crate::client::load_balancing::test_utils::TestChannelController;
    use crate::client::load_balancing::test_utils::TestEvent;
    use crate::client::load_balancing::test_utils::TestSubchannel;
    use crate::client::load_balancing::test_utils::TestWorkScheduler;
    use crate::client::load_balancing::test_utils::new_request_headers;
    use crate::client::name_resolution::ResolverUpdate;
    use crate::core::Address;
    use crate::metadata::MetadataMap;
    use crate::rt::default_runtime;

    fn test_lb_policy_options(tx_events: mpsc::Sender<TestEvent>) -> LbPolicyOptions {
        LbPolicyOptions {
            work_scheduler: Arc::new(TestWorkScheduler { tx_events }),
            runtime: default_runtime(),
        }
    }

    // Tests that a single subchannel creation is properly forwarded to the
    // underlying channel controller and the created shared subchannel seen by
    // the delegate policy contains the real one.
    #[test]
    fn test_single_subchannel() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let sc_out = Arc::new(Mutex::new(None));
        let sc_out_clone = sc_out.clone();

        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |data, _workitem, cc| {
                    let addr = Address {
                        address: "127.0.0.1:80".to_string().into(),
                        ..Default::default()
                    };
                    let sc = cc
                        .new_subchannel(&addr, data.lb_policy_options.work_scheduler.clone())
                        .0;
                    *sc_out_clone.lock().unwrap() = Some(sc);
                })),
                ..Default::default()
            },
            test_lb_policy_options(tx_events.clone()),
        );

        let mut sharing = SubchannelSharing::new(
            mock,
            Arc::new(TestWorkScheduler {
                tx_events: tx_events.clone(),
            }),
        );

        sharing.work(None, &mut cc);

        let event = rx_events.recv().unwrap();
        let TestEvent::NewSubchannel(internal_sc) = event else {
            panic!("expected NewSubchannel")
        };

        let external_sc = sc_out.lock().unwrap().take().unwrap();
        let shared = external_sc.downcast_ref::<SharedSubchannel>().unwrap();
        assert!(Arc::ptr_eq(&shared.delegate, &internal_sc));
    }

    // Tests that when a delegate policy creates multiple subchannels with the
    // same address, they share the same delegate subchannel from the underlying
    // channel controller.
    #[test]
    fn test_multiple_subchannels_same_address() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let sc_out1 = Arc::new(Mutex::new(None));
        let sc_out1_clone = sc_out1.clone();
        let sc_out2 = Arc::new(Mutex::new(None));
        let sc_out2_clone = sc_out2.clone();

        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |data, _workitem, cc| {
                    let addr = Address {
                        address: "127.0.0.1:80".to_string().into(),
                        ..Default::default()
                    };
                    *sc_out1_clone.lock().unwrap() = Some(
                        cc.new_subchannel(&addr, data.lb_policy_options.work_scheduler.clone())
                            .0,
                    );
                    *sc_out2_clone.lock().unwrap() = Some(
                        cc.new_subchannel(&addr, data.lb_policy_options.work_scheduler.clone())
                            .0,
                    );
                })),
                ..Default::default()
            },
            test_lb_policy_options(tx_events.clone()),
        );

        let mut sharing = SubchannelSharing::new(
            mock,
            Arc::new(TestWorkScheduler {
                tx_events: tx_events.clone(),
            }),
        );

        sharing.work(None, &mut cc);

        // Confirm that only one new_subchannel was seen by the underlying
        // channel controller.
        let event = rx_events.recv().unwrap();
        let TestEvent::NewSubchannel(internal_sc) = event else {
            panic!("expected NewSubchannel")
        };
        assert!(rx_events.try_recv().is_err());

        // Confirm that both SharedSubchannels seen by the delegate are unique
        // but share the same underlying subchannel.
        let external_sc1 = sc_out1.lock().unwrap().take().unwrap();
        let external_sc2 = sc_out2.lock().unwrap().take().unwrap();

        let shared1 = external_sc1.downcast_ref::<SharedSubchannel>().unwrap();
        let shared2 = external_sc2.downcast_ref::<SharedSubchannel>().unwrap();

        assert!(Arc::ptr_eq(&shared1.delegate, &internal_sc));
        assert!(Arc::ptr_eq(&shared2.delegate, &internal_sc));
        assert!(!Arc::ptr_eq(&external_sc1, &external_sc2));
    }

    // Tests that when the delegate creates subchannels with different
    // addresses, they get different internal subchannels.
    #[test]
    fn test_multiple_subchannels_different_addresses() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let sc_out1 = Arc::new(Mutex::new(None));
        let sc_out1_clone = sc_out1.clone();
        let sc_out2 = Arc::new(Mutex::new(None));
        let sc_out2_clone = sc_out2.clone();

        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |data, _workitem, cc| {
                    let addr1 = Address {
                        address: "127.0.0.1:80".to_string().into(),
                        ..Default::default()
                    };
                    let addr2 = Address {
                        address: "127.0.0.2:80".to_string().into(),
                        ..Default::default()
                    };
                    *sc_out1_clone.lock().unwrap() = Some(
                        cc.new_subchannel(&addr1, data.lb_policy_options.work_scheduler.clone())
                            .0,
                    );
                    *sc_out2_clone.lock().unwrap() = Some(
                        cc.new_subchannel(&addr2, data.lb_policy_options.work_scheduler.clone())
                            .0,
                    );
                })),
                ..Default::default()
            },
            test_lb_policy_options(tx_events.clone()),
        );

        let mut sharing = SubchannelSharing::new(
            mock,
            Arc::new(TestWorkScheduler {
                tx_events: tx_events.clone(),
            }),
        );

        sharing.work(None, &mut cc);

        // Verify that two new_subchannel calls occurred.
        let event1 = rx_events.recv().unwrap();
        let event2 = rx_events.recv().unwrap();
        assert!(matches!(event1, TestEvent::NewSubchannel(_)));
        assert!(matches!(event2, TestEvent::NewSubchannel(_)));

        assert!(rx_events.try_recv().is_err());

        // Verify that the two subchannels contain different delegates.
        let external_sc1 = sc_out1.lock().unwrap().take().unwrap();
        let external_sc2 = sc_out2.lock().unwrap().take().unwrap();

        let shared1 = external_sc1.downcast_ref::<SharedSubchannel>().unwrap();
        let shared2 = external_sc2.downcast_ref::<SharedSubchannel>().unwrap();

        assert!(!Arc::ptr_eq(&shared1.delegate, &shared2.delegate));
    }

    // Tests that when subchannels are dropped, they are removed from the
    // sharing map.
    #[test]
    fn test_subchannel_cleanup_on_drop() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let update_calls = Arc::new(Mutex::new(0));
        let update_calls_clone = update_calls.clone();

        let sc_out1 = Arc::new(Mutex::new(None));
        let sc_out1_clone = sc_out1.clone();
        let sc_out2 = Arc::new(Mutex::new(None));
        let sc_out2_clone = sc_out2.clone();
        let sc_out3 = Arc::new(Mutex::new(None));
        let sc_out3_clone = sc_out3.clone();

        let work_calls = Arc::new(Mutex::new(0));
        let work_calls_clone = work_calls.clone();

        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |data, workitem, cc| {
                    if let Some(workitem) = workitem {
                        if let Ok(_update) = workitem.downcast::<SubchannelUpdate>() {
                            *update_calls_clone.lock().unwrap() += 1;
                            return;
                        }
                    }
                    let addr = Address {
                        address: "127.0.0.1:80".to_string().into(),
                        ..Default::default()
                    };
                    let mut num_calls = work_calls_clone.lock().unwrap();
                    if *num_calls == 0 {
                        *sc_out1_clone.lock().unwrap() = Some(
                            cc.new_subchannel(&addr, data.lb_policy_options.work_scheduler.clone())
                                .0,
                        );
                        *sc_out2_clone.lock().unwrap() = Some(
                            cc.new_subchannel(&addr, data.lb_policy_options.work_scheduler.clone())
                                .0,
                        );
                    } else if *num_calls == 1 {
                        *sc_out3_clone.lock().unwrap() = Some(
                            cc.new_subchannel(&addr, data.lb_policy_options.work_scheduler.clone())
                                .0,
                        );
                    }
                    *num_calls += 1;
                })),
                ..Default::default()
            },
            test_lb_policy_options(tx_events.clone()),
        );

        let mut sharing = SubchannelSharing::new(
            mock,
            Arc::new(TestWorkScheduler {
                tx_events: tx_events.clone(),
            }),
        );

        // The first call to work should create sc1 and sc2.
        sharing.work(None, &mut cc);
        let _ = rx_events.recv().unwrap();

        let external_sc1 = sc_out1.lock().unwrap().take().unwrap();
        let external_sc2 = sc_out2.lock().unwrap().take().unwrap();

        let internal_sc = external_sc1
            .downcast_ref::<SharedSubchannel>()
            .unwrap()
            .delegate
            .clone();
        let state = SubchannelState::idle();

        // Perform a subchannel update and confirm that two calls are made to
        // the delegate.
        let test_sc = internal_sc.downcast_ref::<TestSubchannel>().unwrap();
        test_sc.work_scheduler().schedule_work(Some(Box::new(SubchannelUpdate {
            subchannel: internal_sc.clone(),
            state: state.clone(),
        })));
        while let Ok(TestEvent::ScheduleWork(data)) = rx_events.try_recv() {
            sharing.work(data, &mut cc);
        }
        assert_eq!(*update_calls.lock().unwrap(), 2);

        // Drop one external subchannel.
        drop(external_sc1);

        // Perform a subchannel update and confirm that only one call is made.
        *update_calls.lock().unwrap() = 0;
        test_sc.work_scheduler().schedule_work(Some(Box::new(SubchannelUpdate {
            subchannel: internal_sc.clone(),
            state: state.clone(),
        })));
        while let Ok(TestEvent::ScheduleWork(data)) = rx_events.try_recv() {
            sharing.work(data, &mut cc);
        }
        assert_eq!(*update_calls.lock().unwrap(), 1);

        // We should have 4 strong references to the internal subchannel: ours,
        // external_sc2, and the maps.
        assert_eq!(Arc::strong_count(&internal_sc), 4);

        // Drop the other subchannel.
        drop(external_sc2);
        while let Ok(TestEvent::ScheduleWork(data)) = rx_events.try_recv() {
            sharing.work(data, &mut cc);
        }

        // Now there should be only our reference left to the internal
        // subchannel: ours.
        assert_eq!(Arc::strong_count(&internal_sc), 1);

        // Perform a subchannel update and confirm zero calls are made.
        *update_calls.lock().unwrap() = 0;
        test_sc.work_scheduler().schedule_work(Some(Box::new(SubchannelUpdate {
            subchannel: internal_sc.clone(),
            state: state.clone(),
        })));
        while let Ok(TestEvent::ScheduleWork(data)) = rx_events.try_recv() {
            sharing.work(data, &mut cc);
        }
        assert_eq!(*update_calls.lock().unwrap(), 0);

        // Create a subchannel with the same address again and confirm that a
        // new underlying subchannel is created.
        sharing.work(None, &mut cc);
        let event = rx_events.recv().unwrap();
        assert!(matches!(event, TestEvent::NewSubchannel(_)));

        let external_sc3 = sc_out3.lock().unwrap().take().unwrap();
        let shared_sc3 = external_sc3.downcast_ref::<SharedSubchannel>().unwrap();

        // Confirm a new subchannel was created.
        assert!(!Arc::ptr_eq(&shared_sc3.delegate, &internal_sc));
    }

    // Tests that single subchannel updates are sent to the delegate for every
    // duplicated shared subchannel.
    #[test]
    fn test_subchannel_update_broadcasts() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let update_calls = Arc::new(Mutex::new(0));
        let update_calls_clone = update_calls.clone();

        let sc_out1 = Arc::new(Mutex::new(None));
        let sc_out1_clone = sc_out1.clone();
        let sc_out2 = Arc::new(Mutex::new(None));
        let sc_out2_clone = sc_out2.clone();

        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |data, workitem, cc| {
                    if let Some(workitem) = workitem {
                        if let Ok(_update) = workitem.downcast::<SubchannelUpdate>() {
                            *update_calls_clone.lock().unwrap() += 1;
                            return;
                        }
                    }
                    let addr = Address {
                        address: "127.0.0.1:80".to_string().into(),
                        ..Default::default()
                    };
                    *sc_out1_clone.lock().unwrap() = Some(
                        cc.new_subchannel(&addr, data.lb_policy_options.work_scheduler.clone())
                            .0,
                    );
                    *sc_out2_clone.lock().unwrap() = Some(
                        cc.new_subchannel(&addr, data.lb_policy_options.work_scheduler.clone())
                            .0,
                    );
                })),
                ..Default::default()
            },
            test_lb_policy_options(tx_events.clone()),
        );

        let mut sharing = SubchannelSharing::new(
            mock,
            Arc::new(TestWorkScheduler {
                tx_events: tx_events.clone(),
            }),
        );

        sharing.work(None, &mut cc);
        let _ = rx_events.recv().unwrap();

        let external_sc1 = sc_out1.lock().unwrap().take().unwrap();
        let external_sc2 = sc_out2.lock().unwrap().take().unwrap();

        let internal_sc = external_sc1
            .downcast_ref::<SharedSubchannel>()
            .unwrap()
            .delegate
            .clone();
        let state = SubchannelState::idle();

        // Verify that two delegated update calls are made.
        let test_sc = internal_sc.downcast_ref::<TestSubchannel>().unwrap();
        test_sc.work_scheduler().schedule_work(Some(Box::new(SubchannelUpdate {
            subchannel: internal_sc.clone(),
            state: state.clone(),
        })));
        while let Ok(TestEvent::ScheduleWork(data)) = rx_events.try_recv() {
            sharing.work(data, &mut cc);
        }
        assert_eq!(*update_calls.lock().unwrap(), 2);

        // Drop one and verify that one delegated update call is made.
        drop(external_sc1);
        *update_calls.lock().unwrap() = 0;
        test_sc.work_scheduler().schedule_work(Some(Box::new(SubchannelUpdate {
            subchannel: internal_sc.clone(),
            state,
        })));
        while let Ok(TestEvent::ScheduleWork(data)) = rx_events.try_recv() {
            sharing.work(data, &mut cc);
        }
        assert_eq!(*update_calls.lock().unwrap(), 1);
    }

    // Tests that the picker properly unwraps the shared subchannel into the
    // underlying subchannel.
    #[test]
    fn test_picker_unwraps_shared_subchannel() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let sc_out = Arc::new(Mutex::new(None));
        let sc_out_clone = sc_out.clone();

        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |data, _workitem, cc| {
                    let addr = Address {
                        address: "127.0.0.1:80".to_string().into(),
                        ..Default::default()
                    };
                    let sc = cc
                        .new_subchannel(&addr, data.lb_policy_options.work_scheduler.clone())
                        .0;
                    *sc_out_clone.lock().unwrap() = Some(sc.clone());

                    #[derive(Debug)]
                    struct MockPicker {
                        sc: Arc<dyn Subchannel>,
                    }
                    impl Picker for MockPicker {
                        fn pick(&self, _req: &RequestHeaders) -> PickResult {
                            PickResult::Pick(Pick {
                                subchannel: self.sc.clone(),
                                metadata: MetadataMap::new(),
                                on_complete: None,
                            })
                        }
                    }

                    cc.update_picker(LbState {
                        connectivity_state: ConnectivityState::Ready,
                        picker: Arc::new(MockPicker { sc }),
                    });
                })),
                ..Default::default()
            },
            test_lb_policy_options(tx_events.clone()),
        );

        let mut sharing = SubchannelSharing::new(
            mock,
            Arc::new(TestWorkScheduler {
                tx_events: tx_events.clone(),
            }),
        );

        sharing.work(None, &mut cc);
        let _ = rx_events.recv().unwrap();

        let event = rx_events.recv().unwrap();
        let TestEvent::UpdatePicker(state) = event else {
            panic!("expected UpdatePicker")
        };

        let req = new_request_headers();
        let result = state.picker.pick(&req);
        let PickResult::Pick(pick) = result else {
            panic!("expected Pick")
        };

        let external_sc = sc_out.lock().unwrap().take().unwrap();
        let shared = external_sc.downcast_ref::<SharedSubchannel>().unwrap();

        assert!(Arc::ptr_eq(&pick.subchannel, &shared.delegate));
    }

    // Tests that update/work/exit_idle methods are delegated appropriately and
    // resolve_now is delegated back to the channel.
    #[test]
    fn test_delegates_other_methods() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let called = Arc::new(Mutex::new(vec![]));

        let mock = StubPolicy::new(
            StubPolicyFuncs {
                resolver_update: Some(Arc::new({
                    let called_clone = called.clone();
                    move |_data, _update, _config, _cc| {
                        called_clone.lock().unwrap().push("resolver_update");
                        Ok(())
                    }
                })),
                work: Some(Arc::new({
                    let called_clone = called.clone();
                    move |_data, _workitem, cc| {
                        called_clone.lock().unwrap().push("work");
                        cc.request_resolution();
                    }
                })),
                exit_idle: Some(Arc::new({
                    let called_clone = called.clone();
                    move |_data, _cc| called_clone.lock().unwrap().push("exit_idle")
                })),
                ..Default::default()
            },
            test_lb_policy_options(tx_events.clone()),
        );

        let mut sharing = SubchannelSharing::new(
            mock,
            Arc::new(TestWorkScheduler {
                tx_events: tx_events.clone(),
            }),
        );

        let update = ResolverUpdate::default();
        sharing.resolver_update(update, None, &mut cc).unwrap();
        sharing.work(None, &mut cc);
        sharing.exit_idle(&mut cc);

        assert_eq!(
            *called.lock().unwrap(),
            vec!["resolver_update", "work", "exit_idle"]
        );

        let event = rx_events.recv().unwrap();
        assert!(matches!(event, TestEvent::RequestResolution));
    }


    // Tests that a shared subchannel's correct state is returned by
    // new_subchannel.
    #[test]
    fn test_new_subchannel_state() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };
        let (tx_work, rx_work) =
            mpsc::channel::<Box<dyn FnOnce(&mut dyn ChannelController) + Send>>();
        // Wrap rx_work in a mutex to allow the stub work Fn() closure to access
        // it mutably.
        let rx_work = Mutex::new(rx_work);

        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |_data, _workitem, cc| {
                    (rx_work.lock().unwrap().recv().unwrap())(cc);
                })),
                ..Default::default()
            },
            test_lb_policy_options(tx_events.clone()),
        );

        let mut sharing = SubchannelSharing::new(
            mock,
            Arc::new(TestWorkScheduler {
                tx_events: tx_events.clone(),
            }),
        );

        let addr = Address {
            address: "127.0.0.2:80".to_string().into(),
            ..Default::default()
        };

        let sc1 = Arc::new(Mutex::new(None));

        // Create the first subchannel
        let sc1_clone = sc1.clone();
        let addr_clone = addr.clone();
        tx_work
            .send(Box::new(move |cc| {
                let (sc, state) = cc.new_subchannel(&addr_clone, Arc::new(NopWorkScheduler));
                assert_eq!(state.connectivity_state, ConnectivityState::Idle);
                *sc1_clone.lock().unwrap() = Some(sc);
            }))
            .unwrap();
        sharing.work(None, &mut cc);

        let event = rx_events.recv().unwrap();
        let TestEvent::NewSubchannel(int_sc) = event else {
            panic!("expected NewSubchannel")
        };

        let test_sc = int_sc.downcast_ref::<TestSubchannel>().unwrap();

        // Update the state to Connecting.
        test_sc.work_scheduler().schedule_work(Some(Box::new(SubchannelUpdate {
            subchannel: int_sc.clone(),
            state: SubchannelState::connecting(),
        })));
        while let Ok(TestEvent::ScheduleWork(data)) = rx_events.try_recv() {
            sharing.work(data, &mut cc);
        }

        // Create a second subchannel for the address and verify that the state
        // is also Connecting.
        let addr_clone = addr.clone();
        tx_work
            .send(Box::new(move |cc| {
                let (sc, state) = cc.new_subchannel(&addr_clone, Arc::new(NopWorkScheduler));
                assert_eq!(state.connectivity_state, ConnectivityState::Connecting);
            }))
            .unwrap();
        sharing.work(None, &mut cc);

        // Update the state to Ready.
        test_sc.work_scheduler().schedule_work(Some(Box::new(SubchannelUpdate {
            subchannel: int_sc.clone(),
            state: SubchannelState::ready(),
        })));
        while let Ok(TestEvent::ScheduleWork(data)) = rx_events.try_recv() {
            sharing.work(data, &mut cc);
        }

        // Create another subchannel for the address and verify that the state
        // is now Ready.
        let addr_clone = addr.clone();
        tx_work
            .send(Box::new(move |cc| {
                let (sc, state) = cc.new_subchannel(&addr_clone, Arc::new(NopWorkScheduler));
                assert_eq!(state.connectivity_state, ConnectivityState::Ready);
            }))
            .unwrap();
        sharing.work(None, &mut cc);
    }
}
