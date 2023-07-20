#![recursion_limit = "256"]

use pgrx::{item_pointer_to_u64, prelude::*, PgRelation};
mod fulltext;
mod vector;

use dirs::data_local_dir;
use fulltext::hooks::init_hooks;
use fulltext::index::inverted::extension::options;
use fulltext::{cleanup, create_fulltext_data_location};
use fulltext::{get_executor_manager, index::inverted::get_index_manager, types::*};

pgrx::pg_module_magic!();

extension_sql_file!("../sql/_bootstrap.sql", bootstrap);
#[pg_extern(immutable, parallel_safe)]
fn query_from_text(input: &str) -> Query {
    Query {
        query: input.to_string(),
    }
}

#[pg_extern(immutable, parallel_safe)]
fn fulltext_from_text(input: &str) -> Fulltext {
    Fulltext {
        0: input.to_string(),
    }
}

extension_sql!(
    r#"
CREATE CAST (text AS quria.query) WITH FUNCTION query_from_text(text) AS IMPLICIT;
CREATE CAST (text AS quria.fulltext) WITH FUNCTION fulltext_from_text(text) AS IMPLICIT;
"#,
    name = "quria_casts"
);

fn create_data_location() {
    let mut data_dir = data_local_dir().expect("Expected to find a data directory.");
    data_dir.extend(vec![".quria"]);

    if !std::path::Path::new(&data_dir).exists() {
        std::fs::create_dir(data_dir).expect("Expected folder to be created.");
    }
}

#[allow(non_snake_case)]
#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    init_hooks();
    options::init();
    create_data_location();
    create_fulltext_data_location();
}

#[pg_extern(immutable)]
pub unsafe fn ft_score(
    ctid: Option<pg_sys::ItemPointerData>,
    query: Query,
    fcinfo: pg_sys::FunctionCallInfo,
) -> f64 {
    match ctid {
        Some(ctid) => {
            let executor_manager = get_executor_manager();
            let (query_desc, query_state) = executor_manager.peek_query_state().unwrap();
            let index_oid = query_state.lookup_index_for_first_field(*query_desc, fcinfo).expect("The '~>' operator could not find a \"USING quria_fts\" index that matches the left-hand-side of the expression");
            let pgrel = unsafe {
                pg_sys::index_open(index_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)
            };

            let indexrel = unsafe { PgRelation::from_pg(pgrel) };
            let index_name =
                vec![indexrel.namespace().clone(), ".", indexrel.name().clone()].concat();
            let index_manager = get_index_manager();
            let index = index_manager.get_or_init_index(index_name.clone());

            let u64_ctid = item_pointer_to_u64(ctid);

            let score = index.bm25(query, u64_ctid);

            unsafe { pg_sys::index_close(pgrel, pg_sys::AccessShareLock as pg_sys::LOCKMODE) };

            score
        }
        None => 0.0f64,
    }
}

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C" fn _PG_fini() {
    cleanup();
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<String>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
