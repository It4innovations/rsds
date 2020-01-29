pub type Map<K, V> = hashbrown::HashMap<K, V>;
pub type Set<T> = hashbrown::HashSet<T>;

pub use wrapped::{RcEqWrapper, WrappedRcRefCell};

mod wrapped;
