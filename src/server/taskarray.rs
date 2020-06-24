use crate::comm::Notifications;
use crate::common::Map;
use crate::protocol::clientmsg::{ArgumentExpr, Int, IntExpr, RangeExpr, TaskArray, PickledData};
use crate::protocol::key::{DaskKey, dask_key_ref_to_str, to_dask_key};
use crate::protocol::protocol::{SerializedMemory, SerializedTransport};
use crate::protocol::workermsg::TaskArgument;
use crate::scheduler::protocol::FromSchedulerMessage::TaskAssignments;
use crate::scheduler::TaskId;
use crate::server::core::Core;
use crate::server::task::{ClientTaskHolder, TaskRef};
use crate::trace::trace_task_new;

#[derive(Debug)]
pub enum MaterializedArgumentExpr<'a> {
    Int(&'a IntExpr),
    ContextRef(&'a IntExpr),
    Bool(bool),
    Serialized(&'a Vec<u8>),
    List(Vec<MaterializedArgumentExpr<'a>>),
    Task(TaskRef),
    TaskArray(&'a MaterializedTaskArray, &'a RangeExpr),
}

impl<'a> MaterializedArgumentExpr<'a> {
    pub fn from<'b : 'a, 'c : 'a>(expr: &'b ArgumentExpr, core: &Core, arrays: &'c Map<DaskKey, MaterializedTaskArray>) -> Self {
        match expr {
            ArgumentExpr::Int(v) => MaterializedArgumentExpr::Int(v),
            ArgumentExpr::Bool(v) => MaterializedArgumentExpr::Bool(*v),
            ArgumentExpr::CtxRef(v) => MaterializedArgumentExpr::ContextRef(v),
            ArgumentExpr::Serialized(v) => MaterializedArgumentExpr::Serialized(v),
            ArgumentExpr::List(list) => MaterializedArgumentExpr::List(list
                .into_iter()
                .map(|v| MaterializedArgumentExpr::from(v, core, arrays))
                .collect()
            ),
            ArgumentExpr::Task(key) => MaterializedArgumentExpr::Task(core.get_task_by_key_or_panic(key).clone()),
            ArgumentExpr::TaskArray(key, range) => {
                let array = arrays.get(key).unwrap();
                MaterializedArgumentExpr::TaskArray(array, &range)
            }
        }
    }
}

#[derive(Debug)]
pub struct MaterializedTaskArray {
    key: DaskKey,
    tasks: Vec<TaskRef>,
}

pub fn materialize_task_arrays(core: &mut Core, arrays: &Vec<TaskArray>) -> Vec<TaskRef> {
    let mut m_arrays = Map::new();
    let mut result = Vec::new();
    for array in arrays {
        let m_array = materialize_task_array(core, &array, &m_arrays);
        result.extend(m_array.tasks.iter().cloned());
        m_arrays.insert(array.key.clone(), m_array);
    }
    result
}

pub fn materialize_task_array(core: &mut Core, array: &TaskArray, m_arrays: &Map<DaskKey, MaterializedTaskArray>) -> MaterializedTaskArray {
    let mut tasks = Vec::new();
    let mut index = 0;
    for part in &array.parts {
        let m_args: Vec<MaterializedArgumentExpr> = part.args
            .iter()
            .map(|a| MaterializedArgumentExpr::from(a, core, m_arrays))
            .collect();
        for _ in 0..part.size {
            let context = EvalContext::new(index, &part.context);
            let mut deps: Vec<TaskId> = Vec::new();
            let args: Vec<_> = m_args
                .iter()
                .map(|a| context.eval_arg(a, &mut deps))
                .collect();
            deps.sort();
            deps.dedup();
            let task_spec = Some(ClientTaskHolder::Custom {
                function: SerializedMemory::Inline(rmpv::Value::Binary(part.function.clone())),
                args: TaskArgument::List(args), // arguments has to be a list
                kwargs: part.kwargs.as_ref().map(|v| SerializedMemory::Inline(rmpv::Value::Binary(v.0.clone())))
            });

            let unfinished_deps = deps.len() as u32; // TODO: Compute real unfinished deps

            let task_id = core.new_task_id();
            let task_key = to_dask_key(&format!("{}-{}", array.key, index));
            trace_task_new(task_id, dask_key_ref_to_str(&task_key), &deps);

            let task_ref = TaskRef::new(
                task_id,
                task_key,
                task_spec,
                deps,
                unfinished_deps, // FIXME
                0, // FIXME
                0, // FIXME
            );
            tasks.push(task_ref);
            index += 1;
        }
    }
    MaterializedTaskArray {
        key: array.key.clone(),
        tasks,
    }
}

pub struct EvalContext<'a> {
    index: Int,
    context: &'a Vec<PickledData>
}

impl<'a> EvalContext<'a> {
    pub fn new(index: Int, context: &'a Vec<PickledData>) -> Self {
        EvalContext { index, context }
    }

    pub fn eval_int(&self, expr: &IntExpr) -> Int {
        match expr {
            IntExpr::Index => self.index,
            IntExpr::Const(v) => *v,
            IntExpr::Add(pair) => self.eval_int(&pair.0) + self.eval_int(&pair.1),
            IntExpr::Mul(pair) => self.eval_int(&pair.0) * self.eval_int(&pair.1),
            IntExpr::Div(pair) => self.eval_int(&pair.0) / self.eval_int(&pair.1),
            IntExpr::Mod(pair) => self.eval_int(&pair.0) % self.eval_int(&pair.1),
        }
    }

    pub fn eval_arg(&self, expr: &MaterializedArgumentExpr, deps: &mut Vec<TaskId>) -> TaskArgument {
        match expr {
            MaterializedArgumentExpr::Int(e) => TaskArgument::Int(self.eval_int(e) as i64),
            MaterializedArgumentExpr::Bool(v) => TaskArgument::Bool(*v),
            MaterializedArgumentExpr::ContextRef(e) => {
                let index = self.eval_int(e) as usize;
                let data = self.context[index].0.clone();  // TODO: take?
                TaskArgument::Serialized(SerializedTransport::Inline(rmpv::Value::Binary(data)))
            },
            MaterializedArgumentExpr::Serialized(data) => TaskArgument::Serialized(
                SerializedTransport::Inline(rmpv::Value::Binary((*data).clone()))
            ),
            MaterializedArgumentExpr::List(items) => TaskArgument::List(items
                .iter()
                .map(|v| self.eval_arg(v, deps))
                .collect()
            ),
            MaterializedArgumentExpr::Task(task_ref) => {
                let task = task_ref.get();
                deps.push(task.id);
                TaskArgument::TaskKey(task.key().into())
            }
            MaterializedArgumentExpr::TaskArray(mta, RangeExpr::GetItem(e)) => {
                let index = self.eval_int(e);
                let task = mta.tasks[index as usize].get();
                deps.push(task.id);
                TaskArgument::TaskKey(task.key().into())
            }
            MaterializedArgumentExpr::TaskArray(mta, RangeExpr::All) => {
                let mut result = Vec::new();
                for task_ref in &mta.tasks {
                    let task = task_ref.get();
                    deps.push(task.id);
                    result.push(TaskArgument::TaskKey(task.key().into()))
                }
                TaskArgument::List(result)
            }
            MaterializedArgumentExpr::TaskArray(mta, RangeExpr::Slice(start, stop, step)) => {
                let start = self.eval_int(&start);
                let stop = self.eval_int(&stop);
                let step = self.eval_int(&step);
                assert!(step >= 1);

                let mut result = Vec::new();
                for i in 0..((stop - start + step - 1) / step) {
                    let task = mta.tasks[(start + i * step) as usize].get();
                    deps.push(task.id);
                    result.push(TaskArgument::TaskKey(task.key().into()))
                }
                TaskArgument::List(result)
            }
        }
    }
}
