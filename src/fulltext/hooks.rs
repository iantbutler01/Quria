use pgrx::*;
use crate::fulltext::{ExecutorManager, get_executor_manager};

struct FulltextHooks;
impl PgHooks for FulltextHooks{
    fn executor_start(
        &mut self,
        query_desc: PgBox<pg_sys::QueryDesc>,
        eflags: i32,
        prev_hook: fn(PgBox<pg_sys::QueryDesc>, i32) -> HookResult<()>,
    ) -> HookResult<()> {
        get_executor_manager().push_query(&query_desc);
        prev_hook(query_desc, eflags)
    }

    fn executor_end(
        &mut self,
        query_desc: PgBox<pg_sys::QueryDesc>,
        prev_hook: fn(PgBox<pg_sys::QueryDesc>) -> HookResult<()>,
    ) -> HookResult<()> {
        let result = prev_hook(query_desc);
        get_executor_manager().pop_query();
        result
    }
}