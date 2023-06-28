use std::{fmt, sync::Arc};

use anyhow::anyhow;
use futures::future::BoxFuture;
use serde_json::{Map, Value};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StateMachineError<'a> {
    #[error("event `{}` inappropriate in current state `{}`", op, state)]
    InvalidEventError { op: &'a str, state: State },
    #[error("event {0} does not exist")]
    UnknownEventError(&'a str),
    #[error("transition canceled with error: {0}")]
    CanceledError(&'a str),
    #[error("state machine state already stopped: {0}")]
    AlreadyStopped(&'a str),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum CallbackType {
    BeforeEvent,
    LeaveState,
    EnterState,
    AfterEvent,
}

// cKey is a struct key used for keeping the callbacks mapped to a target.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CKey {
    // target is either the name of a state or an event depending on which
    // callback type the key refers to. It can also be "" for a non-targeted
    // callback like before_event.
    target: State,
    // callbackType is the situation when the callback will be run.
    callback_type: CallbackType,
}

// eKey is a struct key used for storing the transition map.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct EKey {
    // event is the name of the event that the keys refers to.
    event: Op,
    // src is the source from where the event can transition.
    src: State,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, serde::Deserialize, serde::Serialize)]
pub enum State {
    Ready,          // 准备
    Canceled,       // 取消
    Custom(String), // 用户定义
    Pending,        // 状态挂起
    Done,           // 完成
}

impl From<String> for State {
    fn from(value: String) -> Self {
        match value.as_str() {
            "Ready" => Self::Ready,
            "Canceled" => Self::Canceled,
            "Pending" => Self::Pending,
            "Done" => Self::Done,
            _ => Self::Custom(value),
        }
    }
}
impl From<&str> for State {
    fn from(value: &str) -> Self {
        match value {
            "Ready" => Self::Ready,
            "Canceled" => Self::Canceled,
            "Done" => Self::Done,
            "Pending" => Self::Pending,
            _ => Self::Custom(value.to_string()),
        }
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            State::Ready => write!(f, "{}", "Ready"),
            State::Canceled => write!(f, "{}", "Canceled"),
            State::Pending => write!(f, "{}", "Pending"),
            State::Custom(c) => write!(f, "{}", c),
            State::Done => write!(f, "{}", "Done"),
        }
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum Op {
    Start,          // 初始 准备完成
    Custom(String), // 用户定义
    Cancel,         // 取消
    Continue,       // 重新激活流程
    Stop,           // 完成 停止到Done
}

impl From<String> for Op {
    fn from(value: String) -> Op {
        match value.as_str() {
            "Start" => Self::Start,
            "Stop" => Self::Stop,
            _ => Self::Custom(value),
        }
    }
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Op::Cancel => write!(f, "{}", "Cancel"),
            Op::Start => write!(f, "{}", "Start"),
            Op::Custom(c) => write!(f, "{}", c),
            Op::Stop => write!(f, "{}", "Stop"),
            Op::Continue => write!(f, "{}", "Continue"),
        }
    }
}

#[derive(Clone)]
pub struct Event {
    pub state_machine: Arc<StateMachine>,
    // accept op
    pub op: Op,
    // Src is the receive args
    pub src: State,
    // Dst is the state after the transition.
    pub dst: State,
    // Args is a optinal list of arguments passed to the callback.
    pub args: Option<Map<String, Value>>,
    // canceled is an internal flag set if the transition is canceled.
    canceled: bool,
    // event err
    pub err: Option<Arc<dyn std::error::Error>>,
}

unsafe impl Send for Event {}
unsafe impl Sync for Event {}

impl Event {
    pub fn cancel(&mut self) {
        self.canceled = true;
    }
}

pub type Callback = for<'a> fn(&'a mut Event) -> BoxFuture<'a, anyhow::Result<()>>;

pub trait StateMachineEvtCb: Send + Sync {
    fn callback<'a>(&'a self, e: &'a mut Event) -> BoxFuture<'a, anyhow::Result<()>>;
}

#[derive(Clone)]
struct Empty;

#[derive(Clone)]
pub struct StateMachine {
    pub name: String,
    // curr is the state that the FSM is currently in.
    curr: State,
    // last is the last state
    last: State,

    // transitions maps events and source states to destination states.
    transitions: HashMap<EKey, State>,
    // callbacks maps events and targers to callback functions.
    callbacks: HashMap<CKey, Callback>,
    // icallbacks maps events and targers to callback object.
    icallbacks: HashMap<CKey, Arc<dyn StateMachineEvtCb + 'static>>,

    // accept finite event
    accept_events: HashMap<Op, Empty>,
    // accept finite state
    accept_states: HashMap<State, Empty>,
    // history
    pub history: Vec<State>,
}

impl StateMachine {
    pub fn new<T: ToString>(name: T) -> Self {
        Self {
            name: name.to_string(),
            curr: State::Ready,
            last: State::Ready,
            callbacks: HashMap::new(),
            icallbacks: HashMap::new(),
            transitions: HashMap::new(),
            accept_events: HashMap::new(),
            accept_states: HashMap::new(),
            history: vec![],
        }
    }

    pub async fn current(&self) -> &State {
        &self.curr
    }

    // Is returns true if state is the current state.
    pub async fn is(&self, state: &State) -> bool {
        self.curr.eq(state)
    }
    // state allows the user to move to the given state from current state.
    // The call does not trigger any callbacks, if defined.
    pub async fn state(&mut self, state: &State) {
        self.curr = state.clone()
    }

    // can returns true if event can occur in the current state.
    pub async fn can(&mut self, op: &Op) -> bool {
        if self.curr == State::Done || self.curr == State::Canceled {
            return false;
        }
        self.transitions
            .get(&EKey {
                event: op.clone(),
                src: self.curr.clone(),
            })
            .is_some()
    }

    pub fn add2<'a>(
        &'a mut self,
        event: &'a Op,
        src: &'a [State],
        dst: &'a State,
        icallbacks: &'a [(CallbackType, Arc<dyn StateMachineEvtCb>)],
    ) -> BoxFuture<'a, &'a mut StateMachine> {
        let block = Box::pin(async move {
            let s = self.add(event, src, dst, &[]).await;

            for (k, v) in icallbacks {
                s.icallbacks.insert(
                    CKey {
                        target: dst.clone(),
                        callback_type: k.clone(),
                    },
                    v.clone(),
                );
            }

            self
        });

        block
    }

    fn add<'a>(
        &'a mut self,
        event: &'a Op,
        src: &'a [State],
        dst: &'a State,
        callbacks: &'a [(CallbackType, Callback)],
    ) -> BoxFuture<'a, &'a mut StateMachine> {
        let block = Box::pin(async move {
            self.accept_states.insert(dst.clone(), Empty {});
            // add transitions
            for state in src {
                self.transitions.extend(HashMap::from([
                    (
                        EKey {
                            event: event.clone(),
                            src: state.clone(),
                        },
                        dst.clone(),
                    ),
                    (
                        EKey {
                            event: Op::Cancel,
                            src: state.clone(),
                        },
                        State::Canceled,
                    ),
                ]));

                self.accept_states.insert(state.clone(), Empty {});
            }

            // add events
            self.accept_events.extend(HashMap::from([
                (event.clone(), Empty {}),
                (Op::Cancel, Empty {}),
            ]));

            // add callback
            for (k, v) in callbacks {
                self.callbacks.insert(
                    CKey {
                        target: dst.clone(),
                        callback_type: k.clone(),
                    },
                    v.clone(),
                );
            }

            self
        });

        block
    }

    pub async fn event(
        &mut self,
        op: Op,
        args: Option<Map<String, Value>>,
    ) -> anyhow::Result<&mut StateMachine> {
        if !self.can(&op).await {
            return Err(anyhow!(
                "{}",
                StateMachineError::InvalidEventError {
                    op: &op.to_string(),
                    state: self.curr.clone()
                }
            ));
        }

        if op == Op::Cancel {
            match self.curr {
                State::Canceled => {
                    return Err(anyhow!(
                        "{}",
                        StateMachineError::CanceledError("already in this state")
                    ))
                }
                State::Done => {
                    return Err(anyhow!(
                        "{}",
                        StateMachineError::CanceledError("current state is done")
                    ))
                }
                _ => {}
            }
        }

        let src = self.curr.clone();
        let dst = match self
            .transitions
            .get(&EKey {
                event: op.clone(),
                src: src.clone(),
            })
            .cloned()
        {
            Some(v) => v,
            None => {
                return Err(anyhow!(
                    "{}",
                    StateMachineError::InvalidEventError {
                        op: &op.to_string(),
                        state: self.curr.clone()
                    }
                ));
            }
        };

        let mut evt = Event {
            state_machine: Arc::new(self.clone()),
            op,
            src: src.clone(),
            dst: dst.clone(),
            args,
            canceled: false,
            err: None,
        };

        for t in vec![
            CallbackType::BeforeEvent,
            CallbackType::EnterState,
            CallbackType::AfterEvent,
            CallbackType::LeaveState,
        ] {
            self.cb(&dst, &mut evt, t).await?;
        }

        self.do_transition(&dst);

        Ok(self)
    }

    async fn cb(
        &mut self,
        dst: &State,
        evt: &mut Event,
        callback_type: CallbackType,
    ) -> anyhow::Result<()> {
        if self.curr == State::Done || self.curr == State::Canceled {
            return Err(anyhow!(
                "{}",
                StateMachineError::AlreadyStopped(&self.curr.to_string())
            ));
        }

        if self.callbacks.len() > 0 {
            if let Some(cb) = self.callbacks.get(&CKey {
                target: dst.clone(),
                callback_type: callback_type.clone(),
            }) {
                cb(evt).await?;
                if evt.canceled {
                    self.do_transition(&State::Canceled);
                }
            }
        } else {
            if let Some(cb) = self.icallbacks.get(&CKey {
                target: dst.clone(),
                callback_type,
            }) {
                cb.callback(evt).await?;
                if evt.canceled {
                    self.do_transition(&State::Canceled);
                }
            }
        }

        Ok(())
    }

    fn do_transition(&mut self, desired: &State) {
        self.history.push(self.curr.clone());
        self.last = self.curr.clone();
        self.curr = desired.clone();
        match desired {
            State::Ready | State::Canceled | State::Done => self.history.push(desired.clone()),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StateMachine;
    use crate::state_machine::{CallbackType, Event, Op, State};
    use futures::future::BoxFuture;
    use serde_json::{json, Map};

    #[tokio::test]
    async fn test_fsm_cancel() {
        let mut args = Map::new();
        args.insert("abc".to_string(), json!(r#"{"a":1}"#));

        let cancelcb = for<'a> |e: &'a mut Event| -> BoxFuture<'a, anyhow::Result<()>> {
            let block = Box::pin(async move {
                println!("execute cancelcb.....");

                if let Some(args) = e.args.clone() {
                    for (k, v) in args {
                        println!("k:{},v:{}", k, v);
                    }
                }

                e.cancel();

                Ok(())
            });

            block
        };

        match StateMachine::new("")
            .to_owned()
            .add(
                &Op::Custom("Open".to_string()),
                &[State::Ready],
                &State::Done,
                &[
                    (CallbackType::BeforeEvent, cancelcb),
                    (CallbackType::AfterEvent, cancelcb),
                ],
            )
            .await
            .event(Op::Custom("Open".to_string()), Some(args.clone()))
            .await
        {
            Ok(sm) => {
                panic!("{}", "Inconsistent expected state")
            }
            Err(e) => println!("{}", e),
        }
    }

    #[tokio::test]
    async fn test_fsm() {
        let mut args = Map::new();
        args.insert("abc".to_string(), json!(r#"{"a":1}"#));

        let cb = for<'a> |e: &'a mut Event| -> BoxFuture<'a, anyhow::Result<()>> {
            let block = Box::pin(async move {
                println!("execute cb.....");

                if let Some(args) = e.args.clone() {
                    for (k, v) in args {
                        println!("k:{},v:{}", k, v);
                    }
                }

                Ok(())
            });

            block
        };
        match StateMachine::new("")
            .to_owned()
            .add(
                &Op::Custom("Open".to_string()),
                &[State::Ready],
                &State::Custom("Running".to_string()),
                &[(CallbackType::AfterEvent, cb)],
            )
            .await
            .add(
                &Op::Custom("Close".to_string()),
                &[State::Custom("Running".to_string())],
                &State::Done,
                &[(CallbackType::AfterEvent, cb)],
            )
            .await
            .event(Op::Custom("Open".to_string()), Some(args.clone()))
            .await
        {
            Ok(sm) => {
                println!("current state {}", sm.current().await);

                if !sm.is(&State::Custom("Running".to_string())).await {
                    panic!("{}", "Inconsistent expected state")
                }

                // test can cancel
                assert_eq!(sm.can(&Op::Cancel).await, true);

                match sm.event(Op::Custom("Close".to_string()), None).await {
                    Ok(sm) => {
                        println!("{}", sm.current().await);
                        if !sm.is(&State::Done).await {
                            panic!("{}", "Inconsistent expected state")
                        }
                        println!("sm history {:#?}", sm.history);
                    }
                    Err(e) => panic!("{}", e),
                };
            }
            Err(e) => panic!("{}", e),
        }
    }
}
