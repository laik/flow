#![feature(async_closure)]
#![feature(closure_lifetime_binder)]
#![allow(clippy::unnecessary_wraps)]
#![feature(type_alias_impl_trait)]

use crossgate::{
    parse,
    store::{
        new_mongo_condition, Condition,
        Event::{Added, Updated},
        MongoFilter, MongoStorageExtends, MongoStore,
    },
    utils::{dict::from_value_to_unstructed, from_str, matchs::matchs, Unstructed},
};
use futures::Future;
use model::{Flow, FlowEvent};
pub use state_machine::State;
use std::{
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_context::context::{Context, RefContext};

use crate::model::StepEvent;
pub mod common;
pub mod controller;
pub mod model;
pub mod state_machine;

// use lrlex::lrlex_mod;
// use lrpar::lrpar_mod;

// lrlex_mod!("flow.l");
// lrpar_mod!("flow.y");

pub struct Location {
    file: &'static str,
    line: u32,
    column: u32,
}

pub trait ErrorLocation<T, E> {
    fn location(self, loc: &'static Location) -> anyhow::Result<T>;
}

impl<T, E> ErrorLocation<T, E> for Result<T, E>
where
    E: Display,
    Result<T, E>: anyhow::Context<T, E>,
{
    fn location(self, loc: &'static Location) -> anyhow::Result<T> {
        let msg = self.as_ref().err().map(ToString::to_string);
        anyhow::Context::with_context(self, || {
            format!(
                "{} at {} line {} column {}",
                msg.unwrap(),
                loc.file,
                loc.line,
                loc.column,
            )
        })
    }
}

#[macro_export]
macro_rules! here {
    () => {
        &crate::Location {
            file: file!(),
            line: line!(),
            column: column!(),
        }
    };
}

pub(crate) static FLOW_DB: &str = "flow";

pub(crate) static FLOW_TABLE: &str = "flow";
pub(crate) static FLOW_EVENT_TABLE: &str = "fevent";

pub(crate) static STEP_TABLE: &str = "step";
pub(crate) static STEP_EVENT_TABLE: &str = "sevent";

pub(crate) static STATE_TABLE: &str = "state";

pub(crate) static RESOURCE_TABLE: &str = "resource";
pub(crate) static CONDITION_TABLE: &str = "condition";

pub fn unix_timestamp_string() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string()
}

pub fn unix_timestamp_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub(crate) fn wheres<T: ToString>(s: T) -> anyhow::Result<Condition<MongoFilter>> {
    Ok(new_mongo_condition()
        .to_owned()
        .wheres(&s.to_string())?
        .to_owned())
}

pub async fn continue_flow_step(
    store: MongoStore,
    flow_event_id: &str,
    step: &str,
) -> anyhow::Result<()> {
    let q = new_mongo_condition()
        .to_owned()
        .with_db(FLOW_DB)
        .with_table(STEP_EVENT_TABLE)
        .with_fields(&["return", "is_pending"])
        .wheres(&format!(
            "step = '{}' && flow_event = '{}'",
            step, flow_event_id,
        ))?
        .to_owned();

    let _ = store
        .clone()
        .update_any_type::<Unstructed>(from_str(r#"{"return":"","is_pending":false}"#)?, q)
        .await?;
    Ok(())
}

// 调用流程
pub async fn call_flow_with_state(
    store: MongoStore,
    flow_name: &str,
    flow_id: &str,
    step: &str,
    state: &str,
) -> anyhow::Result<()> {
    // check current status can be accepted
    let flow: Flow = store
        .clone()
        .get_any_type::<Flow>(
            new_mongo_condition()
                .to_owned()
                .with_db(FLOW_DB)
                .with_table(FLOW_TABLE)
                .wheres(&format!("_id = '{}'", flow_name))
                .location(here!())?
                .to_owned(),
        )
        .await
        .location(here!())?;

    let q = new_mongo_condition()
        .to_owned()
        .with_db(FLOW_DB)
        .with_table(FLOW_EVENT_TABLE)
        .wheres(&format!("flow = '{}' && _id = '{}'", flow_name, flow_id))
        .location(here!())?
        .to_owned();

    let mut flow_event: FlowEvent = store
        .clone()
        .get_any_type::<FlowEvent>(q.clone())
        .await
        .location(here!())?;

    if &flow_event.step != step {
        // 检查当前步骤是否正确
        return Ok(());
    }
    // 检查当前状态是否可以接受
    if !flow.check_accept_op(&flow_event.step, state) {
        let err = format!(
            "flow {} current step {} can't accept {}",
            flow_name, flow_event.step, state
        );
        return Err(anyhow::anyhow!(err));
    }

    let q = new_mongo_condition()
        .to_owned()
        .with_db(FLOW_DB)
        .with_table(STEP_EVENT_TABLE)
        .wheres(&format!(
            "flow_event = '{}' && step = '{}'",
            flow_event.uid, flow_event.step
        ))
        .location(here!())?
        .to_owned();

    let mut step_event: StepEvent = store
        .clone()
        .get_any_type::<StepEvent>(q.clone())
        .await
        .location(here!())?;

    let op = format!("{}-{}", step_event.step, state);
    step_event.r#return = Some(op.clone());
    step_event.is_pending = false;
    step_event.active = false;

    let q = q
        .to_owned()
        .with_fields(&["return", "is_pending", "active"])
        .to_owned();

    store
        .clone()
        .update_any_type::<StepEvent>(step_event.to_owned(), q)
        .await
        .location(here!())?;

    let q = new_mongo_condition()
        .to_owned()
        .with_db(FLOW_DB)
        .with_table(FLOW_EVENT_TABLE)
        .with_fields(&["pending", "accept_op"])
        .wheres(&format!("flow = '{}' && _id = '{}'", flow_name, flow_id))
        .location(here!())?
        .to_owned();

    flow_event.pending = false;
    flow_event.accept_op = Some(op);
    let _ = store
        .clone()
        .update_any_type::<FlowEvent>(flow_event.clone(), q)
        .await?;

    Ok(())
}

pub trait EventHandler {
    type EventFuture<'a>: Future<Output = anyhow::Result<State>>
    where
        Self: 'a;
    fn handle<'a>(
        &self,
        ctx: &mut Context,
        item: Unstructed,
        flow_name: &str,
        flow_evt_id: &str,
        step: &str,
    ) -> Self::EventFuture<'a>;
}

pub struct EventDispatch {}

impl EventDispatch {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn run(
        &self,
        ctx: Context,
        store: MongoStore,
        flow_name: &str,
        step: &str,
        event_handler: impl EventHandler,
        predicate: Option<&str>,
    ) -> anyhow::Result<()> {
        log::info!(
            "start event dispatch step '{}' predicate '{:?}'",
            step,
            predicate
        );

        let (_, mut h) = Context::with_parent(&RefContext::from(ctx), None);

        if let Some(predicate) = predicate {
            let _ = parse(predicate)?;
        }

        // 检查 flow_name 是否存在？
        let q = new_mongo_condition()
            .to_owned()
            .with_db(FLOW_DB)
            .with_table(STEP_EVENT_TABLE)
            .wheres(&format!("step = '{}'", step))?
            .to_owned();

        let mut rv = store
            .clone()
            .watch_any_type::<StepEvent>(h.spawn_ctx(), q)
            .await?;

        while let Some(evt) = rv.recv().await {
            let mut step_evt = match evt {
                Added(item) | Updated(item) => item,
                _ => continue,
            };

            // 中断处理
            if step_evt.is_pending {
                continue;
            }

            if let Some(predicate) = predicate {
                if matchs(
                    &mut vec![from_value_to_unstructed(&step_evt)?],
                    parse(predicate)?,
                )?
                .len()
                .eq(&0)
                {
                    continue;
                }
            }

            // 检查当前的流程是否完成(Done)或者(取消)Canceld
            let q = new_mongo_condition()
                .to_owned()
                .with_db(FLOW_DB)
                .with_table(FLOW_EVENT_TABLE)
                .wheres(&format!(
                    "flow = '{}' && _id = '{}'",
                    &flow_name, &step_evt.flow_event
                ))
                .location(here!())?
                .to_owned();

            let flow_event: FlowEvent = match store
                .clone()
                .get_any_type::<FlowEvent>(q)
                .await
                .location(here!())
            {
                Ok(evt) => evt,
                Err(e) => {
                    log::error!(
                        "flow {} step {} get flow event error: {}",
                        flow_name,
                        step,
                        e.to_string()
                    );
                    continue;
                }
            };

            if flow_event.step == State::Canceled.to_string().as_str()
                || flow_event.step == State::Done.to_string().as_str()
            {
                continue;
            }

            // 接收来自step action 处理结果, 如果错误，那么跳过处理这个订单？
            let state = event_handler
                .handle(
                    &mut h.spawn_ctx(),
                    from_value_to_unstructed(&step_evt.args)?,
                    flow_name,
                    &step_evt.flow_event,
                    &step_evt.step,
                )
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "flow {},step {},handle error:{}",
                        flow_name,
                        &step_evt.step,
                        e.to_string()
                    )
                })
                .location(here!())?;

            // 如果接收的到是action 的Pending,流程挂起
            if State::Pending == state {
                let q = new_mongo_condition()
                    .to_owned()
                    .with_db(FLOW_DB)
                    .with_table(STEP_EVENT_TABLE)
                    .with_fields(&["is_pending"])
                    .wheres(&format!(
                        "step = '{}' && flow_event = '{}'",
                        step_evt.step, step_evt.flow_event,
                    ))
                    .location(here!())?
                    .with_update_version(false)
                    .to_owned();

                step_evt.is_pending = true;

                let _ = store
                    .clone()
                    .update_any_type::<StepEvent>(step_evt, q)
                    .await
                    .location(here!())?;

                continue;
            }

            // 业务如果返回的状态不能被accept,那么直接返回到业务代码调用中
            if !self
                .check_accept_op(store.clone(), &step_evt, state.to_string().as_str())
                .await
                .location(here!())?
            {
                let err = format!(
                    "flow {} step {} can not accept {},check your step implement",
                    flow_name, step_evt.step, state
                );
                return Err(anyhow::anyhow!(err));
            }

            let q = new_mongo_condition()
                .to_owned()
                .with_db(FLOW_DB)
                .with_table(STEP_EVENT_TABLE)
                .with_fields(&["return"])
                .wheres(&format!(
                    "step = '{}' && flow_event = '{}'",
                    &step_evt.step, &step_evt.flow_event,
                ))
                .location(here!())?
                .to_owned();

            step_evt.r#return = Some(state.to_string());

            let _ = store
                .clone()
                .update_any_type::<StepEvent>(step_evt, q)
                .await
                .location(here!())?;
        }

        Ok(())
    }

    async fn check_accept_op(
        &self,
        store: MongoStore,
        step_evt: &StepEvent,
        state: &str,
    ) -> anyhow::Result<bool> {
        // 找出相应的fevent
        let q = new_mongo_condition()
            .to_owned()
            .with_db(FLOW_DB)
            .with_table(FLOW_EVENT_TABLE)
            .wheres(&format!("_id = '{}' ", step_evt.flow_event))?
            .to_owned();

        let flow_evt: FlowEvent = store
            .clone()
            .get_any_type::<FlowEvent>(q)
            .await
            .location(here!())?;

        let q = new_mongo_condition()
            .to_owned()
            .with_db(FLOW_DB)
            .with_table(FLOW_TABLE)
            .wheres(&format!("_id = '{}' ", flow_evt.flow))?
            .to_owned();

        let flow: Flow = store
            .clone()
            .get_any_type::<Flow>(q)
            .await
            .location(here!())?;

        Ok(flow.check_accept_op(&flow_evt.step, state))
    }
}
