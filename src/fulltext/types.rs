use pgrx::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PostgresType, Clone)]
pub struct Query {
    pub query: String,
}

#[derive(Serialize, Deserialize, PostgresType, Debug)]
pub struct Fulltext(pub String);

impl ToString for Fulltext {
    fn to_string(&self) -> String {
        self.0.clone()
    }
}
