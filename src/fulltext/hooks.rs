use crate::fulltext::get_executor_manager;
use crate::fulltext::index::inverted::get_index_manager;
use pgrx::*;

struct FulltextHooks;
impl PgHooks for FulltextHooks {
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
        match get_executor_manager().pop_query() {
            Some((_, q)) => {
                if q.index_name != "" {
                    let index_manager = get_index_manager();
                    let index = index_manager.get_or_init_index_mut(q.index_name);

                    if index.should_flush() {
                        index
                            .flush_to_disk()
                            .expect("Expected flush to disk in hook to complete.");
                    }
                }
            }
            _ => (),
        }
        result
    }
}
static mut HOOKS: FulltextHooks = FulltextHooks;

pub unsafe fn init_hooks() {
    register_hook(&mut HOOKS)
}
