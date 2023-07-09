pub mod hooks;
pub mod index;
pub mod operators;
pub mod types;

use pgrx::{pg_sys::PlannedStmt, *};
use rustc_hash::{FxHashMap, FxHashSet};

pub struct QueryState {
    index_lookup: FxHashMap<pg_sys::Oid, pg_sys::Oid>,
    ctids: FxHashSet<u64>,
}

impl QueryState {
    pub fn new() -> Self {
        Self {
            index_lookup: FxHashMap::default(),
            ctids: FxHashSet::default(),
        }
    }
    pub fn lookup_index_for_first_field(
        &mut self,
        query_desc: *mut pg_sys::QueryDesc,
        fcinfo: pg_sys::FunctionCallInfo,
    ) -> Option<pg_sys::Oid> {
        let fcinfo = unsafe { PgBox::from_pg(fcinfo) };
        let flinfo = unsafe { PgBox::from_pg(fcinfo.flinfo) };
        let func_expr = unsafe { PgBox::from_pg(flinfo.fn_expr as *mut pg_sys::FuncExpr) };
        let arg_list = unsafe { PgList::<pg_sys::Node>::from_pg(func_expr.args) };
        let first_arg = arg_list.get_ptr(0).expect("no arguments provided.");

        unsafe {
            if is_a(first_arg, pg_sys::NodeTag_T_Var) {
                // lookup the table from which the 'ctid' value comes, so we can get its oid
                let rtable = query_desc
                    .as_ref()
                    .unwrap()
                    .plannedstmt
                    .as_ref()
                    .unwrap()
                    .rtable;

                let var = PgBox::from_pg(first_arg as *mut pg_sys::Var);
                #[cfg(any(feature = "pg10", feature = "pg11", feature = "pg12"))]
                let rentry = pg_sys::rt_fetch(var.varnoold, rtable);
                #[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15"))]
                let rentry = pg_sys::rt_fetch(
                    if var.varnosyn != 0 {
                        var.varnosyn
                    } else {
                        var.varattnosyn as _
                    },
                    rtable,
                );
                let heap_oid = rentry.as_ref().unwrap().relid;

                if let Some(index_oid) = self.index_lookup.get(&heap_oid) {
                    return Some(*index_oid);
                }

                let mut any_relation =
                    &PgRelation::with_lock(heap_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

                let indices = PgList::<pg_sys::Oid>::from_pg(pg_sys::RelationGetIndexList(
                    any_relation.as_ptr(),
                ));
                let fts_index = indices
                    .iter_oid()
                    .filter(|oid| *oid != pg_sys::InvalidOid)
                    .map(|oid| {
                        PgRelation::with_lock(oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)
                    })
                    .find(|rel| {
                        #[cfg(any(feature = "pg10", feature = "pg11"))]
                        let routine = rel.rd_amroutine;
                        #[cfg(any(
                            feature = "pg12",
                            feature = "pg13",
                            feature = "pg14",
                            feature = "pg15"
                        ))]
                        let routine = rel.rd_indam;
                        let indam = unsafe { PgBox::from_pg(routine) };

                        if indam.amvalidate
                            == Some(
                                crate::fulltext::index::inverted::extension::amhandler::amvalidate,
                            )
                        {
                            true
                        } else {
                            false
                        }
                    });

                if let Some(idx) = fts_index {
                    self.index_lookup.insert(heap_oid, idx.oid());
                    Some(idx.oid())
                } else {
                    None
                }
            } else {
                None
            }
        }
    }
}

struct ExecutorManager {
    queries: Vec<(*mut pg_sys::QueryDesc, QueryState)>,
}

impl ExecutorManager {
    fn new() -> Self {
        Self {
            queries: Vec::new(),
        }
    }

    pub fn peek_query_state(&mut self) -> Option<&mut (*mut pg_sys::QueryDesc, QueryState)> {
        let len = self.queries.len();
        if len == 0 {
            None
        } else {
            self.queries.get_mut(len - 1)
        }
    }

    fn push_query(&mut self, query_desc: &PgBox<pg_sys::QueryDesc>) {
        self.queries.push((query_desc.as_ptr(), QueryState::new()));
    }

    fn pop_query(&mut self) {
        self.queries.pop();
    }
}

static mut EXECUTOR_MANAGER: ExecutorManager = ExecutorManager::new();

pub fn get_executor_manager() -> &'static mut ExecutorManager {
    unsafe { &mut EXECUTOR_MANAGER }
}
