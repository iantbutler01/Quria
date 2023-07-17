use crate::fulltext::get_executor_manager;
use crate::fulltext::index::inverted::{get_index_manager, IndexResultIterator};
use crate::fulltext::types::*;
use pgrx::*;

struct InvertedIndexScanState {
    index_oid: pg_sys::Oid,
    index_id: String,
    iterator: *mut IndexResultIterator,
}

#[pg_guard]
pub extern "C" fn ambeginscan(
    index_relation: pg_sys::Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    pgrx::debug1!("Starting ambeginscan");
    let mut scandesc: PgBox<pg_sys::IndexScanDescData> = unsafe {
        PgBox::from_pg(pg_sys::RelationGetIndexScan(
            index_relation,
            nkeys,
            norderbys,
        ))
    };

    let index_relation = unsafe { PgRelation::from_pg(index_relation) };
    let index_id = vec![
        index_relation.namespace().clone(),
        ".",
        index_relation.name().clone(),
    ]
    .concat();

    let state = InvertedIndexScanState {
        index_oid: (*index_relation).rd_id,
        index_id,
        iterator: std::ptr::null_mut(),
    };

    scandesc.opaque =
        PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(state) as void_mut_ptr;

    scandesc.into_pg()
}

#[pg_guard]
pub extern "C" fn amrescan(
    scan: pg_sys::IndexScanDesc,
    keys: pg_sys::ScanKey,
    nkeys: ::std::os::raw::c_int,
    _orderbys: pg_sys::ScanKey,
    _norderbys: ::std::os::raw::c_int,
) {
    pgrx::debug1!("Starting amrescan");
    if nkeys == 0 {
        panic!("No ScanKeys provided");
    }
    let scan: PgBox<pg_sys::IndexScanDescData> = unsafe { PgBox::from_pg(scan) };
    let indexrel = unsafe { PgRelation::from_pg(scan.indexRelation) };

    let mut state = unsafe { (scan.opaque as *mut InvertedIndexScanState).as_mut() }
        .expect("no scandesc state");
    let nkeys = nkeys as usize;
    let keys = unsafe { std::slice::from_raw_parts(keys as *const pg_sys::ScanKeyData, nkeys) };

    let query_data = keys[0].sk_argument;
    let query = unsafe { Query::from_datum(query_data, false).expect("Expected query struct.") };
    let query_string = query.query;

    let index_manager = get_index_manager();
    let index_name = vec![indexrel.namespace().clone(), ".", indexrel.name().clone()].concat();

    let index = index_manager.get_or_init_index(index_name);

    let results = index.scan(query_string);

    let iter = IndexResultIterator::from(results);

    state.iterator = PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(iter);
    pgrx::info!("HERE");
}

#[pg_guard]
pub extern "C" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection,
) -> bool {
    pgrx::debug1!("Starting amgettuple");
    let mut scan: PgBox<pg_sys::IndexScanDescData> = unsafe { PgBox::from_pg(scan) };
    let state = unsafe { (scan.opaque as *mut InvertedIndexScanState).as_mut() }
        .expect("no scandesc state");

    // no need to recheck the returned tuples as ZomboDB indices are not lossy
    scan.xs_recheck = false;

    let iter = unsafe { state.iterator.as_mut() }.expect("Iterator should exist");
    match iter.next() {
        Some((term, doc_id, _freq)) => {
            #[cfg(any(feature = "pg10", feature = "pg11"))]
            let tid = &mut scan.xs_ctup.t_self;
            #[cfg(any(feature = "pg12", feature = "pg13", feature = "pg14", feature = "pg15"))]
            let tid = &mut scan.xs_heaptid;

            u64_to_item_pointer(doc_id, tid);
            if unsafe { !item_pointer_is_valid(tid) } {
                panic!("invalid item pointer: {:?}", item_pointer_get_both(*tid));
            }

            let (_, qstate) = get_executor_manager().peek_query_state().unwrap();
            qstate.ctids.insert(doc_id.clone());
            qstate.terms.insert(term);

            true
        }
        None => false,
    }
}

#[pg_guard]
pub extern "C" fn amendscan(_scan: pg_sys::IndexScanDesc) {
    // nothing to do here
}

#[pg_guard]
pub extern "C" fn ambitmapscan(scan: pg_sys::IndexScanDesc, tbm: *mut pg_sys::TIDBitmap) -> i64 {
    pgrx::debug1!("Starting ambitmapscan");
    let scan = unsafe { PgBox::from_pg(scan) };
    let state = unsafe { (scan.opaque as *mut InvertedIndexScanState).as_mut() }
        .expect("no scandesc state");
    let (_query, qstate) = get_executor_manager().peek_query_state().unwrap();

    let mut cnt = 0i64;
    let itr = unsafe { state.iterator.as_mut() }.expect("no iterator in state");
    for (term, doc_id, _freq) in itr {
        let mut tid = pg_sys::ItemPointerData::default();

        u64_to_item_pointer(doc_id.clone(), &mut tid);

        unsafe {
            pg_sys::tbm_add_tuples(tbm, &mut tid, 1, false);
        }

        qstate.ctids.insert(doc_id.clone());
        qstate.terms.insert(term);

        cnt += 1;
    }

    cnt
}
