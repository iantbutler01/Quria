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
}

#[derive(Serialize, Deserialize, PostgresType, Debug)]
#[serde(transparent)]
pub struct Fulltext(pub String);

impl ToString for Fulltext {
    fn to_string(&self) -> String {
        self.0.clone()
    }
}

impl std::str::FromStr for Fulltext {
    type Err = Box<dyn std::error::Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Fulltext { 0: s.to_string() })
    }
}
