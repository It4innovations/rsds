use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::de::Visitor;
use serde::export::Formatter;
use std::ops::Deref;
use std::error::Error;
use std::borrow::Borrow;
use std::fmt::{Debug, Display};

/*pub type DaskKey = String;
pub type DaskKeyRef = str;

#[inline]
pub fn to_dask_key(string: &str) -> DaskKey {
    string.to_owned()
}
#[inline]
pub fn dask_key_ref_to_string(key: &DaskKeyRef) -> String {
    key.to_owned()
}*/

pub type DaskKeyRef = [u8];

#[derive(Hash, PartialEq, Eq, Clone, Default)]
pub struct DaskKey {
    bytes: Box<[u8]>
}

#[inline]
pub fn to_dask_key(string: &str) -> DaskKey {
    DaskKey::from(string)
}
#[inline]
pub fn dask_key_ref_to_string(key: &DaskKeyRef) -> String {
    String::from_utf8_lossy(key).to_string()
}

impl Display for DaskKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        Debug::fmt(self, f)
    }
}

impl Debug for DaskKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(self.as_str())
    }
}

impl DaskKey {
    #[inline]
    pub fn to_string(self) -> String {
        String::from_utf8(self.bytes.into_vec()).expect("Couldn't convert DaskKey to String")
    }
    #[inline]
    pub fn as_bytes(&self) -> &DaskKeyRef {
        self.as_ref()
    }
    #[inline]
    pub fn as_str(&self) -> &str {
        unsafe { std::mem::transmute(self.as_ref()) }
    }
}

impl From<String> for DaskKey {
    #[inline]
    fn from(data: String) -> Self {
        data.into_bytes().into()
    }
}
impl From<Vec<u8>> for DaskKey {
    #[inline]
    fn from(data: Vec<u8>) -> Self {
        DaskKey { bytes: data.into_boxed_slice() }
    }
}
impl From<&DaskKeyRef> for DaskKey {
    #[inline]
    fn from(data: &DaskKeyRef) -> Self {
        DaskKey { bytes: Box::from(data) }
    }
}
impl From<&str> for DaskKey {
    #[inline]
    fn from(data: &str) -> Self {
        DaskKey { bytes: Box::from(data.as_bytes()) }
    }
}

impl From<DaskKey> for Vec<u8> {
    #[inline]
    fn from(key: DaskKey) -> Self {
        key.bytes.into_vec()
    }
}

impl Borrow<DaskKeyRef> for DaskKey {
    #[inline]
    fn borrow(&self) -> &DaskKeyRef {
        self.deref()
    }
}

impl AsRef<DaskKeyRef> for DaskKey {
    #[inline]
    fn as_ref(&self) -> &DaskKeyRef {
        self.deref()
    }
}
impl Deref for DaskKey {
    type Target = DaskKeyRef;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl Serialize for DaskKey {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
        S: Serializer {
        serializer.serialize_str(self.as_str())
    }
}

struct DaskKeyVisitor;
impl <'a> Visitor<'a> for DaskKeyVisitor {
    type Value = DaskKey;

    #[inline]
    fn expecting(&self, formatter: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        writeln!(formatter, "Expecting dask key")
    }

    #[inline]
    fn visit_borrowed_str<E>(self, v: &'a str) -> Result<Self::Value, E> where
        E: Error, {
        Ok(v.into())
    }

    #[inline]
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E> where
        E: Error, {
        Ok(v.into())
    }

    #[inline]
    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> where
        E: Error, {
        Ok(v.into())
    }

    #[inline]
    fn visit_borrowed_bytes<E>(self, v: &'a [u8]) -> Result<Self::Value, E> where
        E: Error, {
        Ok(v.into())
    }

    #[inline]
    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E> where
        E: Error, {
        Ok(v.into())
    }
}

impl <'de> Deserialize<'de> for DaskKey {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error> where
        D: Deserializer<'de> {
        deserializer.deserialize_str(DaskKeyVisitor)
    }
}
