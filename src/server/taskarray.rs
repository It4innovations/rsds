

use crate::common::Map;
use crate::protocol::key::DaskKey;
use crate::protocol::protocol::{SerializedMemory, SerializedTransport};
use crate::protocol::workermsg::TaskArgument;
use crate::scheduler::TaskId;
use crate::server::core::Core;
use crate::server::task::{ClientTaskHolder, TaskRef};

type Int = i32;

pub enum IntExpr {
    Index,
    Const(Int),
    Add(Box<(IntExpr, IntExpr)>),
    Mul(Box<(IntExpr, IntExpr)>),
}

pub enum RangeExpr {
    Get(IntExpr),
    // [x]
    Slice(IntExpr, IntExpr, IntExpr),
    // [start:end:step]
    All, // [:]
}

pub enum ArgumentExpr {
    Int(IntExpr),
    Serialized(Vec<u8>),
    Task(DaskKey),
    TaskArray(DaskKey, RangeExpr),
    //ObjectList(Vec<Vec<u8>>, RangeExpr),
}

pub struct TaskArrayPart {
    size: Int,
    function: Vec<u8>,
    args: Vec<ArgumentExpr>,
}

pub struct TaskArray {
    key: DaskKey,
    parts: Vec<TaskArrayPart>,
}

pub struct CompactGraph {
    array: Vec<TaskArray>,
}

pub enum MaterializedArgumentExpr<'a> {
    Int(&'a IntExpr),
    Serialized(&'a Vec<u8>),
    Task(TaskRef),
    TaskArray(&'a MaterializedTaskArray, &'a RangeExpr),
}

impl<'a> MaterializedArgumentExpr<'a> {
    pub fn from<'b : 'a, 'c : 'a>(expr: &'b ArgumentExpr, core: &Core, arrays: &'c Map<DaskKey, MaterializedTaskArray>) -> Self {
        match expr {
            ArgumentExpr::Int(v) => MaterializedArgumentExpr::Int(v),
            ArgumentExpr::Serialized(v) => MaterializedArgumentExpr::Serialized(v),
            ArgumentExpr::Task(key) => MaterializedArgumentExpr::Task(core.get_task_by_key_or_panic(key).clone()),
            ArgumentExpr::TaskArray(key, range) => MaterializedArgumentExpr::TaskArray(arrays.get(key).unwrap(), &range)
        }
    }
}

pub struct MaterializedTaskArray {
    key: DaskKey,
    tasks: Vec<TaskRef>,
}

pub fn materialize_compact_graph(core: &mut Core, graph: &CompactGraph, id_counter: TaskId) {
    let mut m_arrays = Map::new();
    for array in &graph.array {
        let m_array = materialize_task_array(core, &array, &m_arrays, id_counter);
        m_arrays.insert(array.key.clone(), m_array);
    }
}

pub fn materialize_task_array(core: &mut Core, array: &TaskArray, m_arrays: &Map<DaskKey, MaterializedTaskArray>, mut id_counter: TaskId) -> MaterializedTaskArray {
    let tasks = Vec::new();
    let mut index = 0;
    for part in &array.parts {
        let m_args: Vec<MaterializedArgumentExpr> = part.args.iter().map(|a| MaterializedArgumentExpr::from(a, core, m_arrays)).collect();
        for _ in 0..part.size {
            let context = EvalContext::new(index);
            let mut deps: Vec<TaskId> = Vec::new();
            let args: Vec<_> = m_args.iter().map(|a| context.eval_arg(a, &mut deps)).collect();
            deps.sort();
            deps.dedup();

            let task_spec = Some(ClientTaskHolder::Custom {
                function: SerializedMemory::Inline(rmpv::Value::Binary(part.function.clone())),
                args: TaskArgument::List(args), // arguments has to be a list
            });

            let unfinished_deps = deps.len() as u32; // TODO: Compute real unfinished deps

            let task_ref = TaskRef::new(
                id_counter,
                format!("{}-{}", array.key, index).into(),
                task_spec,
                deps,
                unfinished_deps, // FIXME
                0, // FIXME
                0, // FIXME
            );

            core.add_task(task_ref.clone());

            // context.eval_arg();
            id_counter += 1;
            index += 1;
        }
    }
    MaterializedTaskArray {
        key: array.key.clone(),
        tasks,
    }
}

/*
impl CompactGraph {

    pub fn materialize(&self, mut id_counter: TaskId) {
        for array in &self.array {
            self.materialize_array(array, id_counter);
            id_counter += array.size as u64;
        }
    }

    pub fn materialize_array(&self, array: &TaskArray, mut id_counter: TaskId) {
        /*let index = 0;
        for part in &array.parts {
            for i in 0..part.size {
                let context = EvalContext::new(index, self);
                // context.eval_arg();
                id_counter += 1;
                index += 1;
            }
        }*/
    }
}*/

/*pub enum Argument {
    Int(i32),
    TaskKey(String),
    Serialized(Vec<u8>),
    List(Vec<Argument>),
}*/

pub struct EvalContext {
    index: Int,
}

impl EvalContext {
    pub fn new(index: Int) -> Self {
        EvalContext { index }
    }

    pub fn eval_int(&self, expr: &IntExpr) -> Int {
        match expr {
            IntExpr::Index => self.index,
            IntExpr::Const(v) => *v,
            IntExpr::Add(pair) => self.eval_int(&pair.0) + self.eval_int(&pair.1),
            IntExpr::Mul(pair) => self.eval_int(&pair.0) * self.eval_int(&pair.1),
        }
    }

    pub fn eval_arg(&self, expr: &MaterializedArgumentExpr, deps: &mut Vec<TaskId>) -> TaskArgument {
        match expr {
            MaterializedArgumentExpr::Int(e) => TaskArgument::Int(self.eval_int(e) as i64),
            MaterializedArgumentExpr::Serialized(data) => TaskArgument::Serialized(SerializedTransport::Inline(rmpv::Value::Binary((*data).clone()))),
            MaterializedArgumentExpr::Task(task_ref) => {
                let task = task_ref.get();
                deps.push(task.id);
                TaskArgument::TaskKey(task.key().into())
            }
            MaterializedArgumentExpr::TaskArray(mta, RangeExpr::Get(e)) => {
                let index = self.eval_int(e);
                let task = mta.tasks[index as usize].get();
                deps.push(task.id);
                TaskArgument::TaskKey(task.key().into())
            }
            MaterializedArgumentExpr::TaskArray(_key, _) => {
                todo!()
            }
        }
    }
}
