use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

use anyhow::anyhow;

use crossgate::object::Object;
use crossgate::parse;
use crossgate::service::MongoStoreService;
use crossgate::utils::dict::value_to_map;
use crossgate::utils::Unstructed;
use futures::future::BoxFuture;
use mongodb::bson::oid::ObjectId;
use serde_json::Value;
use tokio_context::context::{Context, Handle, RefContext};

use crossgate::store::{new_mongo_condition as condition, Event, MongoStorageExtends, MongoStore};

use crate::common::get_mongo_store;
use crate::model::{Condition, Flow, FlowEvent, Resource, StateDesc, Step, StepEvent};

use crate::state_machine::State;
use crate::state_machine::{
    CallbackType::EnterState, Event as StateMachineEvent, Op, StateMachine, StateMachineEvtCb,
};
use crate::{
    call_flow_with_state, here, unix_timestamp_u64, wheres, ErrorLocation, CONDITION_TABLE,
    FLOW_DB, FLOW_EVENT_TABLE, FLOW_TABLE, RESOURCE_TABLE, STATE_TABLE, STEP_EVENT_TABLE,
    STEP_TABLE,
};

fn db<'a>(_: &'a str, db: &'a str) -> String {
    format!("{}", db)
}
#[derive(Clone)]
pub struct StateDescService {
    db: String,
    store: MongoStoreService<StateDesc>,
}

impl StateDescService {
    pub fn new(tenant: &str, store: &MongoStore) -> Self {
        let db = db(tenant, FLOW_DB).to_owned();
        let store = MongoStoreService::<StateDesc>::new(&db, STATE_TABLE, store.clone());
        Self { db, store }
    }

    async fn get<'a, T: ToString>(&self, id: &T) -> anyhow::Result<Option<StateDesc>> {
        self.store
            .0
            .get(wheres(format!("_id='{}'", id.to_string()))?)
            .await
    }
}
#[derive(Clone)]
pub struct FlowService {
    db: String,
    store: MongoStoreService<Flow>,
}

impl FlowService {
    pub fn new(tenant: &str, store: &MongoStore) -> Self {
        let db = db(tenant, FLOW_DB).to_owned();
        let store = MongoStoreService::<Flow>::new(&db, FLOW_TABLE, store.clone());
        Self { db, store }
    }

    async fn from_event<'a>(&self, event: &'a FlowEvent) -> anyhow::Result<Option<Flow>> {
        self.store
            .0
            .get(wheres(&format!(" _id ='{}' ", event.flow))?)
            .await
    }

    async fn get_step_retry_sess(&self, step: &str, flow: &str) -> anyhow::Result<u32> {
        if let Some(f) = self
            .store
            .0
            .get(wheres(&format!("_id='{}'", flow))?)
            .await?
        {
            if let Some(step) = f.steps.iter().find(|item| item.uid == step) {
                return Ok(step.retry_delay_sess.unwrap_or(3));
            }
        }
        return Ok(3);
    }
}
#[derive(Clone)]
pub struct StepService {
    db: String,
    store: MongoStoreService<Step>,
}

impl StepService {
    pub fn new(tenant: &str, store: &MongoStore) -> Self {
        let db = db(tenant, FLOW_DB).to_owned();
        let store = MongoStoreService::<Step>::new(&db, STEP_TABLE, store.clone());
        Self { db, store }
    }

    async fn _list(&self, ids: &[String]) -> anyhow::Result<Vec<Step>> {
        self.store
            .0
            .list(wheres(format!(
                "_id ~ ({})",
                ids.iter()
                    .map(|item| return format!(r#""{}""#, item))
                    .collect::<Vec<String>>()
                    .join(",")
            ))?)
            .await
            .location(here! {})
    }

    async fn _get<S: ToString>(&self, id: &S) -> anyhow::Result<Option<Step>> {
        self.store
            .0
            .get(wheres(format!("_id='{}'", id.to_string()))?)
            .await
            .location(here! {})
    }

    async fn _from_event<'a>(&self, event: &'a StepEvent) -> anyhow::Result<Option<Step>> {
        self.store
            .0
            .get(wheres(format!("_id='{}'", event.step))?)
            .await
            .location(here! {})
    }
}
#[derive(Clone)]
pub struct StepEventService {
    db: String,
    store: MongoStoreService<StepEvent>,
}

impl StepEventService {
    pub fn new(tenant: &str, store: &MongoStore) -> Self {
        let db = db(tenant, FLOW_DB).to_owned();
        let store = MongoStoreService::<StepEvent>::new(&db, STEP_EVENT_TABLE, store.clone());
        Self { db, store }
    }

    async fn exist<S: ToString>(&self, step: &S, flow_event: String) -> anyhow::Result<bool> {
        Ok(self
            .store
            .0
            .count(wheres(format!(
                "step='{}' && flow_event='{}'",
                step.to_string(),
                flow_event,
            ))?)
            .await
            .location(here! {})?
            > 0)
    }

    async fn get<S: ToString>(
        &self,
        step: &S,
        flow_event_id: &S,
    ) -> anyhow::Result<Option<StepEvent>> {
        self.store
            .0
            .get(wheres(format!(
                "step='{}' && flow_event='{}'",
                step.to_string(),
                flow_event_id.to_string()
            ))?)
            .await
            .location(here! {})
    }

    async fn update<S: ToString>(
        &self,
        evt: StepEvent,
        id: &S,
        fields: &[&str],
    ) -> anyhow::Result<()> {
        self.store
            .0
            .update(
                evt,
                condition()
                    .to_owned()
                    .with_fields(fields)
                    .wheres(&format!("_id='{}'", id.to_string()))?
                    .to_owned(),
            )
            .await
            .location(here! {})?;

        Ok(())
    }
}
#[derive(Clone)]
pub struct FlowEventService {
    db: String,
    pub store: MongoStoreService<FlowEvent>,
}

impl FlowEventService {
    pub fn new(tenant: &str, store: &MongoStore) -> Self {
        let db = db(tenant, FLOW_DB).to_owned();
        let store = MongoStoreService::<FlowEvent>::new(&db, FLOW_EVENT_TABLE, store.clone());
        Self { db, store }
    }

    pub async fn get<S: ToString>(&self, id: &S) -> anyhow::Result<Option<FlowEvent>> {
        self.store
            .0
            .get(wheres(format!(r#"_id="{}""#, id.to_string()))?)
            .await
            .location(here! {})
    }

    pub async fn add(&self, fe: FlowEvent) -> anyhow::Result<Option<FlowEvent>> {
        let q = condition()
            .to_owned()
            .wheres(&format!("_id='{}'", fe.uid()))?
            .to_owned();
        if self.store.0.count(q).await.location(here! {})? > 0 {
            return Ok(None);
        }
        self.store.0.save(fe, condition()).await.location(here! {})
    }

    pub async fn update<S: ToString>(
        &self,
        evt: FlowEvent,
        id: &S,
        fields: &[&str],
    ) -> anyhow::Result<()> {
        self.store
            .0
            .update(
                evt,
                condition()
                    .to_owned()
                    .with_fields(fields)
                    .wheres(&format!("_id='{}'", id.to_string()))?
                    .to_owned(),
            )
            .await
            .location(here! {})?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct ConditionService {
    db: String,
    store: MongoStoreService<Condition>,
}

impl ConditionService {
    pub fn new(tenant: &str, store: &MongoStore) -> Self {
        let db = db(tenant, FLOW_DB).to_owned();
        let store = MongoStoreService::<Condition>::new(&db, CONDITION_TABLE, store.clone());
        Self { db, store }
    }
}

#[derive(Clone)]
pub struct ResourceService {
    db: String,
    store: MongoStoreService<Resource>,
}

impl ResourceService {
    pub fn new(tenant: &str, store: &MongoStore) -> Self {
        let db = db(tenant, FLOW_DB).to_owned();
        let store = MongoStoreService::<Resource>::new(&db, RESOURCE_TABLE, store.clone());
        Self { db, store }
    }

    async fn get_rsource(&self, id: &str) -> anyhow::Result<Option<Resource>> {
        let q = condition()
            .to_owned()
            .wheres(&format!("_id='{}'", id))?
            .to_owned();
        self.store.0.clone().get(q).await.location(here! {})
    }
}

#[derive(Clone)]
pub struct Controller {
    store: MongoStore,

    _step: StepService,
    step_evt: StepEventService,

    flow: FlowService,
    flow_evt: FlowEventService,

    _state: StateDescService,
    cond: ConditionService,
    resource: ResourceService,
}

impl Controller {
    pub fn new(store: &MongoStore) -> Self {
        Self {
            store: store.clone(),
            _step: StepService::new("", &store),
            step_evt: StepEventService::new("", &store),
            flow: FlowService::new("", &store),
            flow_evt: FlowEventService::new("", &store),
            _state: StateDescService::new("", &store),
            cond: ConditionService::new("", &store),
            resource: ResourceService::new("", &store),
        }
    }

    pub fn start(ctx: Context) -> BoxFuture<'static, anyhow::Result<()>> {
        log::info!("start flow controller");
        Box::pin(async move { Self::new(get_mongo_store().await).run(ctx).await })
    }

    async fn run(&self, ctx: Context) -> anyhow::Result<()> {
        let (_, mut h) = Context::with_parent(&RefContext::from(ctx), None);
        tokio::select! {
            Err(e) = self.condition(h.spawn_ctx()) => return Err(e),
            Err(e) = self.flow(h.spawn_ctx()) => return Err(e),
        }
    }

    async fn condition(&self, ctx: Context) -> anyhow::Result<()> {
        let (_, mut h) = Context::with_parent(&RefContext::from(ctx), None);

        let condition_ctx = h.spawn_ctx();
        let mut tasks = HashMap::<String, (Handle, JoinHandle<()>)>::new();

        let condition_stream = async move || -> anyhow::Result<()> {
            let mut stream = self
                .cond
                .store
                .0
                .watch(condition_ctx, condition())
                .await
                .location(here! {})?;

            while let Some(evt) = stream.recv().await {
                log::debug!("condition stream recv {}", evt);

                match evt {
                    Event::Added(condition_item) | Event::Updated(condition_item) => {
                        let task_id = condition_item.uid.clone();
                        if let Some(task) = tasks.remove(&task_id) {
                            log::debug!("condition {} task cancel", task_id);
                            task.0.cancel();
                            task.1.abort();
                        }

                        if let Err(e) = parse(&condition_item.predicate) {
                            log::warn!(
                                r#"condition "{}" try parse predicate "{}" error: {}"#,
                                condition_item.uid(),
                                condition_item.predicate,
                                e,
                            );
                            continue;
                        }

                        let res = match self
                            .resource
                            .get_rsource(&condition_item.resource)
                            .await
                            .location(here! {})
                        {
                            Ok(res) => {
                                if res.is_none() {
                                    continue;
                                }
                                res.unwrap()
                            }
                            Err(e) => {
                                log::warn!(
                                    r#"condition "{}" depended resource "{}" error: {}"#,
                                    condition_item.uid(),
                                    condition_item.resource,
                                    e
                                );
                                continue;
                            }
                        };

                        let (execute_ctx, h) = Context::new();
                        let controller = self.clone();

                        let join = tokio::spawn(async move {
                            let q = match condition()
                                .to_owned()
                                .with_db(&res.db)
                                .with_table(&res.table)
                                .wheres(&condition_item.predicate)
                                .cloned()
                            {
                                Ok(q) => q,
                                Err(e) => {
                                    log::warn!(
                                        r#"error constructing predicate "{}" error: {}"#,
                                        &condition_item.predicate,
                                        e
                                    );
                                    return;
                                }
                            };

                            let mut stream = match controller
                                .store
                                .clone()
                                .watch_any_type::<Unstructed>(execute_ctx, q)
                                .await
                                .location(here! {})
                            {
                                Ok(stream) => stream,
                                Err(e) => {
                                    log::warn!(
                                        r#"error constructing predicate "{}" error: {}"#,
                                        &condition_item.predicate,
                                        e
                                    );
                                    return;
                                }
                            };

                            log::debug!(
                                r#"process condition "{}" start recv {}.{}"#,
                                condition_item.uid(),
                                res.db,
                                res.table
                            );

                            while let Some(evt) = stream.recv().await {
                                // TODO: 事件的ID需要唯一，流程会引用来作为唯一

                                // 每次被更改新增需要新增
                                if let Event::Added(mut item) | Event::Updated(mut item) = evt {
                                    let args = match value_to_map(&item) {
                                        Ok(r) => r,
                                        Err(e) => {
                                            log::warn!(
                                                r#"error generate args to flow event condition "{}" predicate "{}" dest "{}" error: {}"#,
                                                &condition_item.uid,
                                                &condition_item.predicate,
                                                &condition_item.dest,
                                                e
                                            );
                                            continue;
                                        }
                                    };

                                    let uid = item.get_by_type::<String>("_id", "".into());
                                    if uid == "" {
                                        log::warn!(
                                            r#"the watched data must have an "_id" field, error by condition "{}" predicate "{}" dest "{}""#,
                                            &condition_item.uid,
                                            &condition_item.predicate,
                                            &condition_item.dest,
                                        );
                                        continue;
                                    };

                                    let fe = FlowEvent {
                                        uid,
                                        flow: condition_item.dest.clone(),
                                        args,
                                        ..Default::default()
                                    };
                                    if let Err(e) =
                                        controller.flow_evt.clone().add(fe).await.location(here! {})
                                    {
                                        log::warn!(
                                            r#"error generate flow event condition "{}" predicate "{}" dest "{}" error: {}"#,
                                            &condition_item.uid,
                                            &condition_item.predicate,
                                            &condition_item.dest,
                                            e
                                        );
                                    }
                                }
                            }
                        });

                        log::debug!("process condition add task {}", &task_id);

                        tasks.insert(task_id, (h, join));
                    }
                    Event::Deleted(item) => {
                        let task_id = item.get("_id").to_string();
                        if let Some(task) = tasks.remove(&task_id) {
                            task.0.cancel();
                            task.1.abort();
                        }
                    }
                    _ => {}
                }
            }
            Ok(())
        };

        log::debug!("process condition controller starts");

        condition_stream().await
    }

    async fn flow(&self, ctx: Context) -> anyhow::Result<()> {
        let (_, mut h) = Context::with_parent(&RefContext::from(ctx), None);

        let flow_evt_ctx = h.spawn_ctx();
        let flow_stream = async move || -> anyhow::Result<()> {
            // where pending=false and state not in ("Done","Canceled")
            let mut stream = self
                .flow_evt
                .store
                .0
                .watch(
                    flow_evt_ctx,
                    wheres(r#"pending=false && step ~~ ("Done","Canceled")"#)?,
                )
                .await
                .location(here! {})?;

            while let Some(item) = stream.recv().await {
                log::debug!("flow stream handle item: {}", item);
                match item {
                    Event::Added(mut evt) | Event::Updated(mut evt) => {
                        // 接收到事件，然后转换成状态机，
                        let flow = match self.flow.from_event(&evt).await.location(here! {})? {
                            Some(item) => item,
                            None => {
                                log::warn!(
                                    "recv flow stream evt can not covert to flow struct flow:{:?} evt_id: {:?} !!!",
                                    evt.flow,
                                    evt.uid,
                                );
                                continue;
                            }
                        };

                        // 检查step是否已经完成，step.active==false
                        if let Some(step_event) = self
                            .step_evt
                            .get(&evt.step, &evt.uid)
                            .await
                            .location(here! {})?
                        {
                            if step_event.active {
                                log::debug!(
                                    "flow: {} stream handle id: {} step: {} is active, skip",
                                    evt.flow,
                                    evt.uid,
                                    evt.step
                                );
                                continue;
                            }
                        }

                        // 初始化状态机
                        /*
                        flow Default
                            step Ready => Start->A,Cancel->Canceled;

                            // user add
                            step A => succ->B,fail->A,Cancel->Canceled;
                            step B => succ->Done,fail->B,Cancel->Canceled;

                            // default state
                            step Canceled;
                            step Done;

                        转换成的流程表
                         op      src      state    return
                        Start   Ready      A      [succ,fail,Cancel,Pending]
                        A—succ    A        B      [succ,fail,Cancel,Pending]
                        A-fail    A        A      [succ,fial,Cancel,Pending]
                        B—succ    B       Done    [succ,fail,Cancel,Pending]
                        B-fail    B        B      [succ,fial,Cancel,Pending]
                        */
                        let mut state_machine = StateMachine::new(&evt.uid);

                        for (index, step) in flow.steps.into_iter().enumerate() {
                            if index == 0 {
                                state_machine
                                    .add2(
                                        &Op::Start,
                                        &[State::Ready],
                                        &State::Custom(step.uid.clone()),
                                        &[(EnterState, Arc::new(self.clone()))],
                                    )
                                    .await;
                            }

                            for (rtn_state, dst) in step.r#return {
                                state_machine
                                    .add2(
                                        &Op::Custom(format!("{}-{}", step.uid, rtn_state)),
                                        &[State::Custom(step.uid.clone())],
                                        &State::from(dst),
                                        &[(EnterState, Arc::new(self.clone()))],
                                    )
                                    .await;
                            }
                        }

                        // set current step state
                        state_machine.state(&State::from(evt.step.clone())).await;

                        if state_machine.current().await.eq(&State::Done) {
                            continue;
                        }

                        let mut errors = vec![];

                        if let State::Ready = state_machine.current().await {
                            if let Err(e) =
                                state_machine.event(Op::Start, Some(evt.args.clone())).await
                            {
                                errors.push(format!(
                                    "state machine exec Start error: {}",
                                    e.to_string()
                                ));
                            }
                        } else if let Some(op) = &evt.accept_op {
                            if let Err(e) = state_machine
                                .event(Op::Custom(op.to_string()), Some(evt.args.clone()))
                                .await
                            {
                                errors.push(format!(
                                    "state machine exec '{}' error: {}",
                                    op,
                                    e.to_string()
                                ));
                            }
                        }

                        // update evt state
                        evt.step = state_machine.current().await.to_string();

                        let state_machine_history = state_machine
                            .history
                            .iter()
                            .map(|item| item.to_string())
                            .collect::<Vec<String>>();

                        if let Some(mut history) = evt.history {
                            history.extend(state_machine_history);
                            history.dedup(); // 清除重复的历史记录
                            evt.history = Some(history);
                        } else {
                            evt.history = Some(state_machine_history);
                        }

                        if errors.len() > 0 {
                            evt.err = Some(errors.join(",").to_string());
                        }

                        log::debug!(
                            "update flow evt step={},history={:?},err={:?} uid {}",
                            &evt.step,
                            &evt.history,
                            &evt.err,
                            &evt.uid
                        );

                        // update flow_event set step=?,err=?,history=?,pending=true,accept_op=null where _id=?;
                        evt.pending = true;
                        evt.accept_op = None;
                        self.flow_evt
                            .update(
                                evt.clone(),
                                &evt.uid,
                                &["step", "history", "err", "pending", "accept_op"],
                            ) // 多个fields会有问题
                            .await?;
                    }
                    Event::Error(e) => return Err(anyhow!("flow event receive error: {}", e)),
                    _ => {} // ignore other
                }
            }

            Ok(())
        };

        let step_event_ctx = h.spawn_ctx();
        let step_stream = async move || -> anyhow::Result<()> {
            // where active is true
            let mut stream = self
                .step_evt
                .store
                .0
                .watch(step_event_ctx, wheres(format!("active={}", true))?)
                .await?;

            while let Some(item) = stream.recv().await {
                log::debug!("step stream handle item: {}", item);
                match item {
                    Event::Added(mut evt) | Event::Updated(mut evt) => {
                        // 如果是挂起状态或者更新step的值为空,例如step的op指向自己时return会update为空
                        if evt.r#return.is_none()
                            || evt.r#return.clone().unwrap().eq("")
                            || evt.is_pending
                        // 用户在操作时挂起
                        {
                            continue;
                        }

                        // 检查当前step evt对应的flow evt是否还在
                        let mut flow_evt = match self.flow_evt.get(&evt.flow_event).await {
                            Ok(r) => {
                                if r.is_none() {
                                    continue;
                                }
                                r.unwrap()
                            }
                            Err(e) => {
                                evt.err = Some(format!(
                                    "flow {} process lost error: {}",
                                    &evt.flow_event,
                                    e.to_string()
                                ));
                                evt.active = false;
                                self.step_evt
                                    .update(evt.clone(), &evt.uid, &["active", "err"])
                                    .await?;
                                continue;
                            }
                        };

                        if State::from(flow_evt.step.as_str()) == State::Done {
                            evt.active = false;
                            self.step_evt
                                .update(evt.clone(), &evt.uid, &["active"])
                                .await?;
                            continue;
                        }

                        // 如果已经流程已经canceled将结束
                        if State::from(flow_evt.step.as_str()) == State::Canceled {
                            evt.active = false;
                            evt.err = Some(format!("flow {} already canceled", &evt.flow_event));
                            self.step_evt
                                .update(evt.clone(), &evt.uid, &["active", "err"])
                                .await?;
                            continue;
                        }

                        if let Err(e) = call_flow_with_state(
                            self.store.clone(),
                            &flow_evt.flow,
                            &flow_evt.uid,
                            &flow_evt.step,
                            &String::from(evt.r#return.clone().unwrap()),
                        )
                        .await
                        {
                            evt.err = Some(format!("call flow error: {}", e.to_string()));
                            self.step_evt
                                .update(evt.clone(), &evt.uid, &["err"])
                                .await?;
                        }
                    }
                    Event::Error(e) => return Err(anyhow!("step event receive error: {}", e)),
                    _ => {}
                }
            }
            Ok(())
        };

        log::debug!("process flow controller starts");

        tokio::select! {
            Err(e) = step_stream() => {
                h.cancel();
                log::error!("process flow controller step steam error");
                return Err(anyhow::anyhow!("{}",e.to_string()))
            },
            Err(e) = flow_stream() => {
                h.cancel();
                log::error!("process flow controller flow steam error");
                return Err(anyhow::anyhow!("{}",e.to_string()))
            },
        }
    }
}

impl StateMachineEvtCb for Controller {
    fn callback<'a>(&'a self, e: &'a mut StateMachineEvent) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            match State::from(e.dst.clone()) {
                State::Ready | State::Canceled | State::Done | State::Pending => return Ok(()),
                State::Custom(_) => {}
            }

            let step = e.dst.to_string();
            let flow_name = e.state_machine.name.clone();
            let retry_delay_sess = self.flow.get_step_retry_sess(&step, &flow_name).await?;

            if e.src == e.dst {
                if let Some(step_evt) = match self.step_evt.get(&step, &flow_name).await {
                    Ok(item) => item,
                    Err(e) => {
                        log::warn!("retry loop step error: {}", e.to_string());
                        return Err(e);
                    }
                } {
                    if (unix_timestamp_u64() - step_evt.version()) < retry_delay_sess as u64 {
                        tokio::time::sleep(Duration::from_secs(
                            retry_delay_sess as u64 - (unix_timestamp_u64() - step_evt.version()),
                        ))
                        .await;
                    }

                    let q = condition()
                        .to_owned()
                        .with_fields(&["version", "flow_event", "step", "args", "active", "return"])
                        .wheres(&format!("_id = '{}'", &step_evt.uid))?
                        .to_owned();

                    let _ = self
                        .step_evt
                        .store
                        .0
                        .update(
                            StepEvent {
                                version: unix_timestamp_u64(),
                                active: true,
                                r#return: Some("".into()),
                                ..step_evt
                            },
                            q,
                        )
                        .await?;

                    return Ok(());
                }
            }

            match self
                .step_evt
                .exist(&e.dst, e.state_machine.name.clone())
                .await
            {
                Ok(has) => {
                    if has {
                        log::debug!(
                            "found step '{}' flow_event '{}' then create it",
                            &e.dst,
                            &e.state_machine.name
                        );
                        return Ok(()); // break
                    } else {
                        log::debug!(
                            "not found step '{}' flow_event '{}' then create it",
                            &e.dst,
                            &e.state_machine.name
                        );
                    }
                }
                Err(e) => {
                    log::warn!("find step event error: {}", e.to_string());
                    return Err(anyhow::anyhow!("find step event error: {}", e));
                }
            };

            let _ = self
                .step_evt
                .store
                .0
                .save(
                    StepEvent {
                        uid: ObjectId::new().to_string(),
                        flow_event: e.state_machine.name.clone(),
                        step: e.dst.to_string(),
                        args: e.args.clone(),
                        active: true,
                        retry_delay_sess: Some(retry_delay_sess),
                        ..Default::default()
                    },
                    condition(),
                )
                .await?;

            Ok(())
        })
    }
}
