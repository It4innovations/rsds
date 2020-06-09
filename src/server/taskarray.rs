
#[derive(Debug)]
pub enum IntExpr {
    Index,
    Const(i64),
    Add(Box<IntExpr>, Box<IntExpr>),
    Mul(Box<IntExpr>, Box<IntExpr>),
}

pub enum RangeExpr {
    Get(IntExpr),
    Slice(IntExpr, IntExpr, IntExpr),
    All,
}

pub enum ArgumentExpr {
    Int(IntExpr),
    Object(Vec<u8>),
    Task(String),
    TaskArray(String, RangeExpr),
    //ObjectList(Vec<Vec<u8>>, RangeExpr),
}

pub enum Argument {
    Int(i64),
    TaskKey(String),
    Object(Vec<u8>),
    List(Vec<Argument>),
}

pub struct EvalContext {
    index: i64,
}

impl EvalContext {

    pub fn new(index: i64) -> Self {
        EvalContext { index }
    }

    pub fn eval_int(&self, expr: &IntExpr) -> i64 {
        match expr {
            IntExpr::Index => self.index,
            IntExpr::Const(v) => v,
            IntExpr::Add(e1, e2) => self.eval_int(e1) + self.eval_int(e2),
            IntExpr::Mul(e1, e2) => self.eval_int(e1) * self.eval_int(e2),
        }
    }

    pub fn eval_arg(&self, expr: &ArgumentExpr) -> Argument {
        match expr {
            ArgumentExpr::Int(e) => Argument::Int(self.eval_int(e)),
            ArgumentExpr::Object(data) => Argument::Object(data.clone()),
            Argument::Task(key) => Argument::TaskKey(key.clone()),
            Argument::TaskArray(key, RangeExpr::Single(e)) => {
                let index = self.eval_int(e);
                Argument::TaskKey(format!("{}-{}", key, index))
            }
        }
    }
}