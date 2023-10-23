use std::str::FromStr;

use pgrx::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub enum FTSQueryOperator {
    AND,
    OR,
}

impl Default for FTSQueryOperator {
    fn default() -> Self {
        Self::OR
    }
}

impl FromStr for FTSQueryOperator {
    type Err = Box<dyn std::error::Error>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "AND" => Ok(FTSQueryOperator::AND),
            "OR" => Ok(FTSQueryOperator::OR),
            _ => Err("Query operator type not matched!".into()),
        }
    }
}

#[derive(Serialize, Deserialize, PostgresType, Clone)]
pub struct Query {
    pub query: String,
    #[serde(default = "FTSQueryOperator::default")]
    pub operator: FTSQueryOperator,
    #[serde(default)]
    pub exact: bool,
    pub column: String,
}

#[derive(Serialize, Deserialize, PostgresType, Debug)]
#[serde(transparent)]
#[inoutfuncs]
pub struct Fulltext(pub String);

impl InOutFuncs for Fulltext {
    fn input(input: &std::ffi::CStr) -> Self {
        let input = input
            .to_str()
            .expect("quria.fulltext input is not valid UTF8");
        Fulltext::from_str(input).unwrap()
    }

    fn output(&self, buffer: &mut StringInfo)
    where
        Self: serde::ser::Serialize,
    {
        serde_json::to_writer(buffer, &self.as_value())
            .expect("failed to write quria.fulltext to buffer");
    }
}

impl ToString for Fulltext {
    fn to_string(&self) -> String {
        self.0.clone()
    }
}

impl core::ops::Deref for Fulltext {
    type Target = String;

    fn deref(&self) -> &String {
        &self.0
    }
}

impl std::str::FromStr for Fulltext {
    type Err = Box<dyn std::error::Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Fulltext { 0: s.to_string() })
    }
}

impl Fulltext {
    fn as_value(&self) -> serde_json::Value {
        serde_json::to_value(&self).expect("failed to serialize to json")
    }
}
