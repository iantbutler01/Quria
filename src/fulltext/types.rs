use pgrx::*;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, PostgresType)]
pub struct Query {
    pub query_string: String
}

#[derive(Serialize, Deserialize, PostgresType)]
pub struct Fulltext(String);
