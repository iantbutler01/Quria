pub mod hooks;
pub mod index;
pub mod operators;
pub mod types;

use dirs::data_local_dir;
use index::inverted::get_index_manager;
use once_cell::sync::Lazy;
use pgrx::{pg_sys::QueryDesc, *};
use rustc_hash::{FxHashMap, FxHashSet};

pub fn data_dir() -> std::path::PathBuf {
    let mut data_dir = data_local_dir().expect("Expected to find a data directory.");
    data_dir.extend(vec![".quria", "fulltext"]);

    data_dir
}

pub fn create_fulltext_data_location() -> () {
    let fts_dir = data_dir();

    if !std::path::Path::new(&fts_dir).exists() {
        std::fs::create_dir(fts_dir).expect("Expected folder to be created.");
    }
}

pub fn cleanup() -> () {
    let index_manager = get_index_manager();

    index_manager
        .flush_all_indexes()
        .expect("Expected all indexes to flush.")
}

pub struct QueryState {
    index_lookup: FxHashMap<pg_sys::Oid, pg_sys::Oid>,
    pub index_name: String,
    ctids: FxHashSet<u64>,
    terms: FxHashSet<String>,
}

impl QueryState {
    pub fn new() -> Self {
        Self {
            index_lookup: FxHashMap::default(),
            index_name: "".to_string(),
            ctids: FxHashSet::default(),
            terms: FxHashSet::default(),
        }
    }
    pub fn lookup_index_for_column(
        &mut self,
        column: &str,
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

                let any_relation =
                    &PgRelation::with_lock(heap_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

                let rel_data = any_relation.rd_att.as_ref().unwrap();
                let col_vec: Vec<_> = rel_data.attrs.as_slice(rel_data.natts as usize).into();
                let col_opt = col_vec.iter().find(|attr| attr.name() == column);

                if let Some(col) = col_opt {
                    let col_key = col.attnum;
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
                            let indam = PgBox::from_pg(routine);

                            let indkey = &PgBox::from_pg(rel.rd_index).indkey;

                            if indkey.dim1 != 1 {
                                panic!("quria_fts indices should only be constructed over 1 column.");
                            }

                            let idx_col_key = indkey.values.as_slice(indkey.dim1 as usize)[0];

                            if indam.amvalidate
                                == Some(
                                    crate::fulltext::index::inverted::extension::amhandler::amvalidate,
                                ) && idx_col_key == col_key
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
            } else {
                None
            }
        }
    }
}

pub struct ExecutorManager {
    queries: Vec<(*mut pg_sys::QueryDesc, QueryState)>,
    uncommitted_tx: Vec<u64>,
    hooks_registered: bool,
}

impl ExecutorManager {
    fn new() -> Self {
        Self {
            queries: Vec::new(),
            uncommitted_tx: Vec::new(),
            hooks_registered: false,
        }
    }

    fn cleanup(&mut self) {
        self.uncommitted_tx = Vec::new();
        self.queries = Vec::new();
        self.hooks_registered = false;
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

    fn pop_query(&mut self) -> Option<(*mut QueryDesc, QueryState)> {
        self.queries.pop()
    }

    fn commit_tx(&mut self, xid: u64) {
        let pos_opt = self.uncommitted_tx.iter().position(|x| *x == xid);

        if let Some(pos) = pos_opt {
            self.uncommitted_tx.remove(pos);
        };
    }

    pub fn register_hooks() {
        if !get_executor_manager().hooks_registered {
            // called when the top-level transaction commits
            register_xact_callback(PgXactCallbackEvent::PreCommit, || {
                get_executor_manager().cleanup()
            });

            // called when the top-level transaction aborts
            register_xact_callback(PgXactCallbackEvent::Abort, || {
                get_executor_manager().cleanup()
            });

            // called when a subtransaction aborts
            register_subxact_callback(
                PgSubXactCallbackEvent::AbortSub,
                |_my_sub_id, _parent_sub_id| {
                    let current_xid = unsafe { pg_sys::GetCurrentTransactionIdIfAny() };
                    if current_xid != pg_sys::InvalidTransactionId {
                        get_executor_manager().commit_tx(xid_to_64bit(current_xid));
                    }
                },
            );

            get_executor_manager().hooks_registered = true;
        }
    }

    fn find_vacuum_targets(&self, ltq: Option<u64>, gtq: Option<u64>) -> Vec<u64> {
        self.uncommitted_tx
            .iter()
            .filter(|xid| {
                if let Some(op) = ltq {
                    **xid <= op
                } else if let Some(op) = gtq {
                    **xid >= op
                } else {
                    false
                }
            })
            .map(|x| x.clone())
            .collect()
    }
}

static mut EXECUTOR_MANAGER: Lazy<ExecutorManager> = Lazy::new(|| ExecutorManager::new());

pub fn get_executor_manager() -> &'static mut ExecutorManager {
    unsafe { &mut EXECUTOR_MANAGER }
}
