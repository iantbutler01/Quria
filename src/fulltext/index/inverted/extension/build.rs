use pgrx::*;

use crate::fulltext::{
    get_executor_manager,
    index::inverted::{get_index_manager, IndexBuildState, OptionalRange},
    ExecutorManager,
};

use crate::fulltext::types::*;

struct BuildState<'a> {
    memcxt: PgMemoryContexts,
    idx_build_state: &'a IndexBuildState<'a>,
}

impl<'a> BuildState<'a> {
    fn new(idx_build_state: &'a IndexBuildState<'a>) -> Self {
        BuildState {
            memcxt: PgMemoryContexts::new("quria_fts_build_context"),
            idx_build_state,
        }
    }
}

#[pg_guard]
pub extern "C" fn ambuild(
    heaprel: pg_sys::Relation,
    indexrel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let heap_relation = unsafe { PgRelation::from_pg(heaprel) };
    let index_relation = unsafe { PgRelation::from_pg(indexrel) };
    pgrx::debug1!("ambuild:starting");
    unsafe {
        if !index_info
            .as_ref()
            .expect("index_info is null")
            .ii_Predicate
            .is_null()
        {
            panic!("Quria indices cannot contain WHERE clauses");
        } else if index_info
            .as_ref()
            .expect("index_info is null")
            .ii_Concurrent
        {
            panic!("Quria indices cannot be created CONCURRENTLY");
        }
    }

    let index_name = vec![
        index_relation.namespace().clone(),
        ".",
        index_relation.name().clone(),
    ]
    .concat();

    let index_manager = get_index_manager();
    let index = index_manager.init_index_mut(index_name.clone());

    // register a callback to delete the newly-created index if our transaction aborts
    register_xact_callback(PgXactCallbackEvent::Abort, move || {
        let index_manager = get_index_manager();

        index_manager.poison_index(index_name.clone());

        if let Err(e) = index_manager.delete_index(index_name.clone()) {
            // we can't panic here b/c we're already in the ABORT stage
            warning!(
                "failed to delete Quria FTS index on transaction abort: {:?}",
                e
            )
        }
    });

    let timer = std::time::Instant::now();

    let ntuples = do_heap_scan(index_info, &heap_relation, &index_relation);

    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = ntuples as f64;
    result.index_tuples = ntuples as f64;

    index
        .flush_to_disk()
        .expect("Expected successful flush to disk.");

    pgrx::info!(
        "Time Elapsed For Index Construction: {}",
        timer.elapsed().as_secs()
    );
    result.into_pg()
}

fn do_heap_scan<'a>(
    index_info: *mut pg_sys::IndexInfo,
    heap_relation: &'a PgRelation,
    index_relation: &'a PgRelation,
) -> usize {
    pgrx::debug1!("do_heap_scan:starting");
    let index_manager = get_index_manager();
    let index_name = vec![
        index_relation.namespace().clone(),
        ".",
        index_relation.name().clone(),
    ]
    .concat();
    ExecutorManager::register_hooks();
    pgrx::debug1!("do_heap_scan: After EM register hooks.");

    let tupdesc = index_relation.tuple_desc();

    let x = unsafe {
        PgMemoryContexts::TopTransactionContext
            .switch_to(|_| PgTupleDesc::from_pg_is_copy(tupdesc.into_pg()))
    };
    pgrx::debug1!("do_heap_scan: After lookup tupedesc");

    let idx_build_state = index_manager.get_or_insert_build_state(index_name, x);
    let mut state = BuildState::new(idx_build_state);
    pgrx::debug1!("do_heap_scan: After new build state");

    unsafe {
        pg_sys::IndexBuildHeapScan(
            heap_relation.as_ptr(),
            index_relation.as_ptr(),
            index_info,
            Some(build_callback),
            &mut state,
        );
    }

    let ntuples = state.idx_build_state.num_inserted;

    ntuples
}

#[pg_guard]
pub extern "C" fn ambuildempty(_index_relation: pg_sys::Relation) {}

#[cfg(any(feature = "pg10", feature = "pg11", feature = "pg12", feature = "pg13"))]
#[pg_guard]
pub unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    pgrx::debug1!("aminsert:starting");
    aminsert_internal(index_relation, values, heap_tid)
}

#[cfg(any(feature = "pg14", feature = "pg15"))]
#[pg_guard]
pub unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck,
    _index_unchanged: bool,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    aminsert_internal(index_relation, values, heap_tid)
}

#[inline(always)]
unsafe fn aminsert_internal(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    heap_tid: pg_sys::ItemPointer,
) -> bool {
    pgrx::debug1!("aminsert_internal:starting");
    let index_relation = PgRelation::from_pg(index_relation);
    let index_manager = get_index_manager();
    let index_name = vec![
        index_relation.namespace().clone(),
        ".",
        index_relation.name().clone(),
    ]
    .concat();

    let (_, qstate) = get_executor_manager().peek_query_state().unwrap();
    qstate.index_name = index_name.clone();

    let values = std::slice::from_raw_parts(values, 1);

    let index = index_manager.get_or_init_index_mut(index_name);

    let row = value_to_string(values[0]);
    let xmin = xid_to_64bit(pg_sys::GetCurrentTransactionId());
    let xmax = pg_sys::InvalidTransactionId as u64;

    let em = get_executor_manager();
    ExecutorManager::register_hooks();
    em.uncommitted_tx.push(xmin);

    let doc_id = item_pointer_to_u64(*heap_tid);
    let doc = row;

    pgrx::debug1!("aminsinternal row: {}", doc.to_string());
    let xrange = OptionalRange {
        min: Some(xmin),
        max: Some(xmax),
    };

    match index.add_doc(doc_id, doc.to_string(), xrange) {
        Ok(_) => true,
        Err(e) => {
            error!("failed to insert document into Quria FTS index: {:?}", e);
        }
    }
}

#[cfg(any(feature = "pg10", feature = "pg11", feature = "pg12"))]
#[pg_guard]
unsafe extern "C" fn build_callback(
    _index: pg_sys::Relation,
    htup: pg_sys::HeapTuple,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    let htup = htup.as_ref().unwrap();

    build_callback_internal(htup.t_self, values, state);
}

#[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15"))]
#[pg_guard]
unsafe extern "C" fn build_callback(
    _index: pg_sys::Relation,
    ctid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    build_callback_internal(*ctid, values, state);
}

#[inline(always)]
unsafe extern "C" fn build_callback_internal(
    ctid: pg_sys::ItemPointerData,
    values: *mut pg_sys::Datum,
    state: *mut std::os::raw::c_void,
) {
    pgrx::debug1!("build_callback_internal:starting");
    check_for_interrupts!();

    let state = (state as *mut BuildState).as_mut().unwrap();

    let mut old_context = state.memcxt.set_as_current();

    let values = std::slice::from_raw_parts(values, 1);

    let datum = values[0];

    if datum.is_null() {
        return;
    }

    let u64_ctid = item_pointer_to_u64(ctid);
    pgrx::info!("{:?}", u64_ctid);

    let row = value_to_string(datum);

    let xmin = pg_sys::FirstNormalTransactionId as u64;
    let xmax = pg_sys::InvalidTransactionId as u64;

    let index_manager = get_index_manager();

    let index = index_manager
        .get_index_mut(&state.idx_build_state.index_id)
        .expect("Index not found");

    let doc_id = u64_ctid;
    let doc = row;

    pgrx::debug1!("buildcbinternal row: {}", doc);
    let xrange = OptionalRange {
        min: Some(xmin),
        max: Some(xmax),
    };

    index
        .add_doc(doc_id, doc.to_string(), xrange)
        .expect("Failed to insert document into Quria FTS index");

    old_context.set_as_current();
    state.memcxt.reset();
}

unsafe fn value_to_string(row: pg_sys::Datum) -> String {
    let value = unsafe {
        let data = Fulltext::from_datum(row, false);

        match data {
            Some(ft) => ft.to_string(),
            None => "".to_string(),
        }
    };

    value
}
