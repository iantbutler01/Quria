use crate::fulltext::get_executor_manager;
use crate::fulltext::index::inverted::get_index_manager;
use crate::fulltext::types::*;
use pgrx::*;
use rustc_hash::FxHashSet;

#[pg_extern(immutable, parallel_safe)]
fn fts(element: Fulltext, query: Query, fcinfo: pg_sys::FunctionCallInfo) -> bool {
    let executor_manager = get_executor_manager();

    let (query_desc, query_state) = executor_manager.peek_query_state().unwrap();

    let index_oid = query_state.lookup_index_for_first_field(*query_desc, fcinfo).expect("The '~>' operator could not find a \"USING quria_fts\" index that matches the left-hand-side of the expression");

    let _lookup = do_seqscan(query.clone(), index_oid);

    let (_query_desc, query_state) = executor_manager.peek_query_state().unwrap();

    let string = element.to_string();

    let pgrel =
        unsafe { pg_sys::index_open(index_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE) };
    let indexrel = unsafe { PgRelation::from_pg(pgrel) };
    unsafe { pg_sys::index_close(pgrel, pg_sys::AccessShareLock as pg_sys::LOCKMODE) };

    let index_name = vec![indexrel.namespace().clone(), ".", indexrel.name().clone()].concat();

    let index_manager = get_index_manager();
    let index = index_manager.get_or_init_index(index_name.clone());

    let terms = index.normalize(string.clone());

    terms.iter().any(|term| query_state.terms.contains(term))
}

#[inline]
fn do_seqscan(query: Query, index_oid: pg_sys::Oid) -> FxHashSet<u64> {
    unsafe {
        let index = pg_sys::index_open(index_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        let heap = pg_sys::relation_open(
            index.as_ref().unwrap().rd_index.as_ref().unwrap().indrelid,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        );

        let mut keys = PgBox::<pg_sys::ScanKeyData>::alloc0();
        keys.sk_argument = query.into_datum().unwrap();

        let scan = pg_sys::index_beginscan(heap, index, pg_sys::GetTransactionSnapshot(), 1, 0);
        pg_sys::index_rescan(scan, keys.into_pg(), 1, std::ptr::null_mut(), 0);

        let mut lookup = FxHashSet::default();
        loop {
            check_for_interrupts!();

            #[cfg(any(feature = "pg10", feature = "pg11"))]
            let tid = {
                let htup = pg_sys::index_getnext(scan, pg_sys::ScanDirection_ForwardScanDirection);
                if htup.is_null() {
                    break;
                }
                item_pointer_to_u64(htup.as_ref().unwrap().t_self)
            };

            #[cfg(any(feature = "pg12", feature = "pg13", feature = "pg14", feature = "pg15"))]
            let tid = {
                let slot = pg_sys::MakeSingleTupleTableSlot(
                    heap.as_ref().unwrap().rd_att,
                    &pg_sys::TTSOpsBufferHeapTuple,
                );

                if !pg_sys::index_getnext_slot(
                    scan,
                    pg_sys::ScanDirection_ForwardScanDirection,
                    slot,
                ) {
                    pg_sys::ExecDropSingleTupleTableSlot(slot);
                    break;
                }

                let tid = item_pointer_to_u64(slot.as_ref().unwrap().tts_tid);
                pg_sys::ExecDropSingleTupleTableSlot(slot);
                tid
            };

            lookup.insert(tid);
        }
        pg_sys::index_endscan(scan);
        pg_sys::index_close(index, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        pg_sys::relation_close(heap, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        lookup
    }
}

extension_sql!(
    r#"
CREATE OPERATOR pg_catalog.~>(
    PROCEDURE = quria.fts,
    LEFTARG = quria.fulltext,
    RIGHTARG = quria.query
);

CREATE OPERATOR CLASS fts_quria_ops DEFAULT FOR TYPE quria.fulltext USING quria_fts AS
    OPERATOR 1 pg_catalog.~>(quria.fulltext, quria.query),
    STORAGE quria.fulltext;

"#,
    name = "quria_ops_anyelement_fts_operator"
);
