use std::borrow::Borrow;
use std::hash::Hash;

use hashbrown::hash_map::Values;

use crate::common::Map;

#[derive(Debug, Default)]
pub struct KeyIdMap<Id, Type, Key = String> {
    id_to_item: Map<Id, Type>,
    key_to_id: Map<Key, Id>,
}

pub trait Identifiable {
    type Id: Copy + Eq + Hash;
    type Key: Eq + Hash;

    fn get_id(&self) -> Self::Id;
    fn get_key(&self) -> Self::Key;
}

impl<Type: Identifiable> KeyIdMap<Type::Id, Type, Type::Key> {
    #[inline]
    pub fn new() -> Self {
        Self {
            id_to_item: Default::default(),
            key_to_id: Default::default(),
        }
    }

    pub fn insert(&mut self, item: Type) {
        let id = item.get_id();
        let key = item.get_key();
        assert!(self.key_to_id.insert(key, id).is_none());
        assert!(self.id_to_item.insert(id, item).is_none());
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.id_to_item.is_empty()
    }

    #[inline]
    pub fn get_by_id(&self, id: Type::Id) -> Option<&Type> {
        self.id_to_item.get(&id)
    }

    #[inline]
    pub fn get_mut_by_id(&mut self, id: Type::Id) -> Option<&mut Type> {
        self.id_to_item.get_mut(&id)
    }

    #[inline]
    pub fn get_by_key<Q: Eq + Hash + ?Sized>(&self, key: &Q) -> Option<&Type>
    where
        Type::Key: Borrow<Q>,
    {
        self.get_id_by_key(&key).and_then(|id| self.get_by_id(id))
    }

    #[inline]
    pub fn get_id_by_key<Q: Eq + Hash + ?Sized>(&self, key: &Q) -> Option<Type::Id>
    where
        Type::Key: Borrow<Q>,
    {
        self.key_to_id.get(key).copied()
    }

    #[inline]
    pub fn remove_by_id(&mut self, id: Type::Id) {
        let item = self.id_to_item.remove(&id).unwrap();
        assert!(self.key_to_id.remove(&item.get_key()).is_some());
    }

    #[inline]
    pub fn values(&self) -> Values<'_, Type::Id, Type> {
        self.id_to_item.values()
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{Identifiable, KeyIdMap};

    #[derive(Default, Clone)]
    struct Val {
        id: u64,
        key: String,
    }

    impl Identifiable for Val {
        type Id = u64;
        type Key = String;

        fn get_id(&self) -> Self::Id {
            self.id
        }

        fn get_key(&self) -> Self::Key {
            self.key.clone()
        }
    }

    #[test]
    fn insert_remove() {
        let mut map = create_map();
        let item = Val {
            id: 0,
            key: "test".to_owned(),
        };
        map.insert(item);
        map.remove_by_id(0);
    }

    #[test]
    fn get_by_id() {
        let mut map = create_map();
        map.insert(Val {
            id: 0,
            key: "test".to_owned(),
        });
        assert!(map.get_by_id(0).is_some());
    }

    #[test]
    fn get_by_key() {
        let mut map = create_map();
        map.insert(Val {
            id: 0,
            key: "test".to_owned(),
        });
        assert!(map.get_by_key("test").is_some());
    }

    fn create_map() -> KeyIdMap<u64, Val> {
        Default::default()
    }
}
