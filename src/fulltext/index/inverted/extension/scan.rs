use crate::fulltext::index::inverted::{
    get_index_manager, Index, IndexManager, IndexResultIterator,
};

use crate::fulltext::get_executor_manager;
use pgrx::*;

struct InvertedIndexScanState<'a> {
    index_oid: pg_sys::Oid,
    index_id: String,
    iterator: *mut IndexResultIterator<'a>,
}

#[pg_guard]
pub extern "C" fn ambeginscan(
    index_relation: pg_sys::Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    let mut scandesc: PgBox<pg_sys::IndexScanDescData> = unsafe {
        PgBox::from_pg(pg_sys::RelationGetIndexScan(
            index_relation,
            nkeys,
            norderbys,
        ))
    };

    let index_relation = PgRelation::from_pg(index_relation);
    let index_id = vec![
        index_relation.namespace().clone(),
        ".",
        index_relation.name().clone(),
    ]
    .concat();

    let state = InvertedIndexScanState {
        index_oid: unsafe { (*index_relation).rd_id },
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
    let column = keys[0].sk_attno;
    let query_string = handle_as_generic_string(query_data);

    let index_manager = get_index_manager();
    let index_name = vec![indexrel.namespace().clone(), ".", indexrel.name().clone()].concat();

    let mut index = index_manager.get_or_init_index(&index_name);

    let results = index.scan(query_string);

    let iter = IndexResultIterator::from(results);

    state.iterator = PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(iter);
}

#[pg_guard]
pub extern "C" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection,
) -> bool {
    let mut scan: PgBox<pg_sys::IndexScanDescData> = unsafe { PgBox::from_pg(scan) };
    let state = unsafe { (scan.opaque as *mut InvertedIndexScanState).as_mut() }
        .expect("no scandesc state");

    // no need to recheck the returned tuples as ZomboDB indices are not lossy
    scan.xs_recheck = false;

    let index_manager = get_index_manager();
    let index = index_manager
        .get_index(&state.index_id)
        .expect("index should have been created by this point");

    let iter = unsafe { state.iterator.as_mut() }.expect("no iterator in state");
    match iter.next() {
        Some((term, doc_id, freq)) => {
            #[cfg(any(feature = "pg10", feature = "pg11"))]
            let tid = &mut scan.xs_ctup.t_self;
            #[cfg(any(feature = "pg12", feature = "pg13", feature = "pg14", feature = "pg15"))]
            let tid = &mut scan.xs_heaptid;

            let ctid = index
                .docid_to_ctid
                .get(&doc_id)
                .expect("doc_id should have ctid at this point");

            u64_to_item_pointer(*ctid, tid);
            if unsafe { !item_pointer_is_valid(tid) } {
                panic!("invalid item pointer: {:?}", item_pointer_get_both(*tid));
            }

            // TODO:  the score/highlight values we stash away here relates to the index ctid, not
            //        the heap ctid.  These could be different in the case of HOT-updated tuples
            //        it's not clear how we can efficiently resolve the HOT chain here.
            //        Likely side-effects of this will be that `zdb.score(ctid)` and `zdb.highlight(ctid...`
            //        will end up returning NULL on the SQL-side of things.
            let (_, qstate) = get_executor_manager().peek_query_state().unwrap();
            qstate.ctids.insert(ctid.clone());

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
    let scan = unsafe { PgBox::from_pg(scan) };
    let index_relation = unsafe { PgRelation::from_pg(scan.indexRelation) };
    let state = unsafe { (scan.opaque as *mut InvertedIndexScanState).as_mut() }
        .expect("no scandesc state");
    let (_query, qstate) = get_executor_manager().peek_query_state().unwrap();

    let index_manager = get_index_manager();
    let index = index_manager
        .get_index(&state.index_id)
        .expect("Expected index to exist at this point");

    let mut cnt = 0i64;
    let itr = unsafe { state.iterator.as_mut() }.expect("no iterator in state");
    for (term, doc_id, freq) in itr {
        let mut tid = pg_sys::ItemPointerData::default();

        let ctid = index
            .docid_to_ctid
            .get(&doc_id)
            .expect("Expected ctid to be matched with a docid at this point");

        u64_to_item_pointer(ctid.clone(), &mut tid);

        unsafe {
            pg_sys::tbm_add_tuples(tbm, &mut tid, 1, false);
        }

        qstate.ctids.insert(ctid.clone());

        cnt += 1;
    }

    cnt
}

fn handle_as_generic_string(datum: pg_sys::Datum) -> String {
    let mut output_func = pg_sys::InvalidOid;
    let mut is_varlena = false;
    let result =
        unsafe { std::ffi::CStr::from_ptr(pg_sys::OidOutputFunctionCall(output_func, datum)) };
    let result_str = result
        .to_str()
        .expect("failed to convert unsupported type to a string");

    result_str.to_string()
}
