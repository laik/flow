use std::collections::HashMap;

use crossgate::{
    object::{decorate, Object},
    store::MongoDbModel,
};
use serde_json::{Map, Value};

use crate::{
    state_machine::{Op, State},
    unix_timestamp_u64,
};

// 所有的资源uid==name

#[decorate]
struct Resource {
    pub db: String,
    pub table: String,
}
#[decorate]
struct Condition {
    pub(crate) resource: String,  // order resource
    pub(crate) predicate: String, // a=123 && b=1
    pub(crate) dest: String,      // order_flow
}

#[decorate]
struct StateDesc {
    pub desc: Map<String, Value>, // 声明状态，例如 {"succ":"成功","faild":"失败"}
}

#[decorate]
struct Step {
    pub state_desc: String, // ref state_desc id
    pub retry_delay_sess: Option<u32>,
    pub r#return: HashMap<String, String>, // succ -> B, faild -> Done
}

#[decorate]
struct Flow {
    pub steps: Vec<Step>,
}

impl Flow {
    pub fn get_step(&self, step: &str) -> Option<&Step> {
        self.steps.iter().find(|s| s.uid == step)
    }

    pub fn check_accept_op(&self, step: &str, state: &str) -> bool {
        if step == "Done" {
            return true;
        }
        match self.steps.iter().find(|s| s.uid == step) {
            Some(step) => return step.r#return.contains_key(state),
            None => false,
        }
    }
}

impl MongoDbModel for Flow {}

fn _default_step() -> String {
    State::Ready.to_string()
}

// 字段 需要有默认值，watch的资源只会返回ID
#[decorate]
struct FlowEvent {
    pub flow: String,
    pub args: Map<String, Value>,                    // 参数
    pub golbal_variable: Option<Map<String, Value>>, // 全局变量
    pub accept_op: Option<String>,
    pub pending: bool, // 挂起状态，防止抖动
    #[serde(default = "_default_step")]
    pub step: String, //初始步骤
    pub history: Option<Vec<String>>, //历史状态
    pub err: Option<String>,
}

impl MongoDbModel for FlowEvent {}

impl Default for FlowEvent {
    fn default() -> Self {
        Self {
            flow: Default::default(),
            args: Map::new(),
            pending: false,
            golbal_variable: Default::default(),
            accept_op: Some(Op::Start.to_string()),
            step: State::Ready.to_string(),
            history: Default::default(),
            err: Default::default(),
            uid: Default::default(),
            version: unix_timestamp_u64(),
            kind: "FlowEvent".into(),
        }
    }
}

// 事件ID需要生成为一标识
#[decorate]
struct StepEvent {
    pub flow_event: String,               // 创建者的flow event ID
    pub step: String,                     // 引用step的名字
    pub args: Option<Map<String, Value>>, // 参数
    pub r#return: Option<String>,
    pub retry_delay_sess: Option<u32>, // 当一个步骤指向自己的时候，例如： Step A , succ -> B, fail ->A, 当错误的时候会重新触发时间
    pub is_pending: bool,              // 是否挂起状态
    pub active: bool,
    pub err: Option<String>,
}

impl MongoDbModel for StepEvent {}

impl Default for StepEvent {
    fn default() -> Self {
        Self {
            kind: "StepEvent".into(),
            flow_event: Default::default(),
            step: Default::default(),
            args: Default::default(),
            uid: Default::default(),
            version: unix_timestamp_u64(),
            retry_delay_sess: None,
            is_pending: false,
            r#return: None,
            active: true,
            err: None,
        }
    }
}
