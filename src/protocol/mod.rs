use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize};
use smallvec::alloc::fmt::Formatter;

pub mod clientmsg;
pub mod generic;
pub mod key;
pub mod protocol;
pub mod workermsg;


pub type Priority = i32;

#[derive(Serialize, Debug, Default, PartialOrd, PartialEq)]
pub struct Float(f64);

struct FloatVisitor;

impl<'de> Visitor<'de> for FloatVisitor {
    type Value = f64;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a number")
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: std::error::Error,
    {
        Ok(v as f64)
    }
    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: std::error::Error,
    {
        Ok(v as f64)
    }
    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: std::error::Error,
    {
        Ok(v)
    }
}

impl<'de> Deserialize<'de> for Float {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Float(deserializer.deserialize_any(FloatVisitor)?))
    }
}

impl From<f64> for Float {
    fn from(value: f64) -> Self {
        Self(value)
    }
}
impl From<Float> for f64 {
    fn from(value: Float) -> Self {
        value.0
    }
}
