// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use super::Shared;

/// Hands a connection's calls to Python in the order the client started them.
#[derive(Default)]
pub(super) struct Sequencer {
    state: Mutex<SequencerState>,
}

#[derive(Default)]
struct SequencerState {
    issued: u64,
    next: u64,
    // Turns that ended before the ones ahead of them.
    ended: HashSet<u64>,
    waiting: HashMap<u64, oneshot::Sender<()>>,
}

impl Sequencer {
    pub(super) fn issue(self: &Arc<Self>) -> Turn {
        let mut state = self.state.lock().unwrap();
        let number = state.issued;
        state.issued += 1;
        Turn(Arc::new(TurnInner {
            sequencer: self.clone(),
            number,
        }))
    }
}

/// A call's place in its connection's order, given up when the last clone drops.
#[derive(Clone)]
pub(super) struct Turn(Arc<TurnInner>);

struct TurnInner {
    sequencer: Arc<Sequencer>,
    number: u64,
}

impl Turn {
    pub(super) async fn wait(&self) {
        let ready = {
            let mut state = self.0.sequencer.state.lock().unwrap();
            if state.next == self.0.number {
                return;
            }
            let (sender, ready) = oneshot::channel();
            state.waiting.insert(self.0.number, sender);
            ready
        };
        let _ = ready.await;
    }
}

impl Drop for TurnInner {
    fn drop(&mut self) {
        let mut state = self.sequencer.state.lock().unwrap();
        state.waiting.remove(&self.number);
        if state.next != self.number {
            state.ended.insert(self.number);
            return;
        }
        let mut next = self.number + 1;
        while state.ended.remove(&next) {
            next += 1;
        }
        state.next = next;
        if let Some(sender) = state.waiting.remove(&next) {
            let _ = sender.send(());
        }
    }
}

pub(super) fn turn<T>(shared: &Shared, request: &tonic::Request<T>) -> Option<Turn> {
    if !shared.ordered {
        return None;
    }
    request.extensions().get::<Turn>().cloned()
}
