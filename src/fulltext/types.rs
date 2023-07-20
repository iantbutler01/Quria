use pgrx::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PostgresType, Clone)]
pub struct Query {
    pub query: String,
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
