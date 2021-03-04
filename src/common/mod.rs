pub use cycle_wrapped::{CycleOwner, HasCycle};
pub use id_counter::IdCounter;
//pub use key_id_map::{Identifiable, KeyIdMap};
pub use wrapped::WrappedRcRefCell;

pub type Map<K, V> = hashbrown::HashMap<K, V>;
pub type Set<T> = hashbrown::HashSet<T>;

mod cycle_wrapped;
pub mod data;
mod id_counter;
mod key_id_map;
pub mod rpc;
mod wrapped;
