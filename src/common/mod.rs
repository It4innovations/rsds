pub type Map<K, V> = hashbrown::HashMap<K, V>;
pub type Set<T> = hashbrown::HashSet<T>;

pub use wrapped::{RcEqWrapper, WrappedRcRefCell};

pub use id_counter::IdCounter;
pub use key_id_map::{Identifiable, KeyIdMap};
pub use cycle_wrapped::{HasCycle, CycleOwner};

pub type Priority = i32;

mod cycle_wrapped;
mod id_counter;
mod key_id_map;
mod wrapped;
