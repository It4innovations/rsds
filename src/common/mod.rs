pub type Map<K, V> = hashbrown::HashMap<K, V>;
pub type Set<T> = hashbrown::HashSet<T>;

pub use wrapped::{RcEqWrapper, WrappedRcRefCell};

pub use key_id_map::{Identifiable, KeyIdMap};
pub use id_counter::IdCounter;

mod key_id_map;
mod wrapped;
mod id_counter;
