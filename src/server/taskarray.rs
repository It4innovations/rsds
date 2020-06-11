use crate::scheduler::TaskId;

type Int = i32;

#[derive(Debug)]
pub enum IntExpr {
    Index,
    Const(Int),
    Add(Box<(IntExpr, IntExpr)>),
    Mul(Box<(IntExpr, IntExpr)>),
}

pub enum RangeExpr {
    Get(IntExpr), // [x]
    Slice(IntExpr, IntExpr, IntExpr), // [start:end:step]
    All, // [:]
}

pub enum ArgumentExpr {
    Int(IntExpr),
    Object(Vec<u8>),
    Task(String),
    TaskArray(String, RangeExpr),
    //ObjectList(Vec<Vec<u8>>, RangeExpr),
}

pub struct TaskArrayPart {
    size: Int,
    function: Vec<u8>,
    arguments: Vec<ArgumentExpr>
}

pub struct TaskArray {
    key: String,
    parts: Vec<TaskArrayPart>,
    size: Int,
}

pub struct CompactGraph {
    array: Vec<TaskArray>,
}

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
            for i in &part.size {
                let context = EvalContext::new(index);
                context.eval_arg()
                id_counter += 1;
                index += 1;
            }
        }*/
    }
}

pub enum Argument {
    Int(i32),
    TaskKey(String),
    Serialized(Vec<u8>),
    List(Vec<Argument>),
}

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

    pub fn eval_arg(&self, expr: &ArgumentExpr) -> Argument {
        todo!()
        /*match expr {
            ArgumentExpr::Int(e) => Argument::Int(self.eval_int(e)),
            ArgumentExpr::Object(data) => Argument::Serialized(data.clone()),
            ArgumentExpr::Task(key) => Argument::TaskKey(key.clone()),
            ArgumentExpr::TaskArray(key, RangeExpr::Get(e)) => {
                let index = self.eval_int(e);
                Argument::TaskKey(format!("{}-{}", key, index))
            }
            ArgumentExpr::TaskArray(key, _) => {
                todo!()
            }
        }*/
    }
}
