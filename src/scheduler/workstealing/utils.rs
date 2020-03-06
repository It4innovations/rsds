use crate::common::Map;
use crate::scheduler::protocol::TaskId;
use crate::scheduler::task::{OwningTaskRef, TaskRef};

pub fn compute_b_level(tasks: &Map<TaskId, OwningTaskRef>) {
    let mut n_consumers: Map<TaskRef, u32> = Map::with_capacity(tasks.len());
    let mut stack: Vec<TaskRef> = Vec::new();
    for (_, tref) in tasks.iter() {
        let len = tref.get().consumers.len() as u32;
        if len == 0 {
            //tref.get_mut().b_level = 0.0;
            stack.push(tref.clone());
        } else {
            n_consumers.insert(tref.clone(), len);
        }
    }
    while let Some(tref) = stack.pop() {
        let mut task = tref.get_mut();

        let mut b_level = 0;
        for tr in &task.consumers {
            b_level = b_level.max(tr.get().b_level);
        }
        task.b_level = b_level + 1;

        for inp in &task.inputs {
            let v: &mut u32 = n_consumers.get_mut(&inp).unwrap();
            if *v <= 1 {
                assert_eq!(*v, 1);
                stack.push(inp.clone());
            } else {
                *v -= 1;
            }
        }
    }
}
