use std::collections::HashMap;
use crate::scheduler::schedproto::TaskId;
use crate::scheduler::implementation::task::TaskRef;

pub fn compute_b_level(tasks: &HashMap<TaskId, TaskRef>) {
    let mut n_consumers : HashMap<TaskRef, u32> = HashMap::new();
    n_consumers.reserve(tasks.len());
    let mut stack: Vec<TaskRef> = Vec::new();
    for (id, tref) in tasks.iter() {
        let len = tref.get().consumers.len() as u32;
        if len == 0 {
            //tref.get_mut().b_level = 0.0;
            stack.push(tref.clone());
        } else {
            n_consumers.insert(tref.clone(), len);
        }
    }
    loop {
        let tref = match stack.pop() {
            Some(tref) => tref,
            None => break,
        };
        let mut task = tref.get_mut();

        let mut b_level = 0.0f32;
        for tr in &task.consumers {
            b_level = b_level.max(tr.get().b_level);
        }
        task.b_level = b_level + 1.0f32;

        for inp in &task.inputs {
            let v : &mut u32 = n_consumers.get_mut(&inp).unwrap();
            if *v <= 1 {
                assert_eq!(*v, 1);
                stack.push(inp.clone());
            } else {
                *v -= 1;
            }
        }
    }
}