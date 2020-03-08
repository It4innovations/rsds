pub type Map<K, V> = hashbrown::HashMap<K, V>;
pub type Set<T> = hashbrown::HashSet<T>;

pub use wrapped::WrappedRcRefCell;

pub use cycle_wrapped::{CycleOwner, HasCycle};
pub use id_counter::IdCounter;
pub use key_id_map::{Identifiable, KeyIdMap};

mod cycle_wrapped;
mod id_counter;
mod key_id_map;
mod wrapped;
