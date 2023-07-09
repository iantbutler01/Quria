use pgrx::*;
use crate::fulltext::types::*;
use crate::fulltext::{ExecutorManager, get_executor_manager};
use rustc_hash::{FxHashMap, FxHashSet};

#[pg_extern(immutable, parallel_safe)]
fn anyelement_fts(
    element: AnyElement,
    query: Query,
    fcinfo: pg_sys::FunctionCallInfo,
) -> bool {
    let mut executor_manager = get_executor_manager();

    let (query_desc, query_state) = executor_manager.peek_query_state().unwrap();

    let tid = if element.oid() == pg_sys::TIDOID {
        // use the ItemPointerData passed into us as the first argument
        Some(item_pointer_to_u64(
            unsafe { pg_sys::ItemPointerData::from_datum(element.datum(), false) }.unwrap(),
        ))
    } else {
        panic!(
            "The '~>' operator could not find a \"USING quria_fts\" index that matches the left-hand-side of the expression"
        );
    };

    let index_oid = query_state.lookup_index_for_first_field(*query_desc, fcinfo).expect("The '~>' operator could not find a \"USING quria_fts\" index that matches the left-hand-side of the expression");

    match tid {
        Some(tid) => unsafe {
            let mut lookup_by_query = pg_func_extra(fcinfo, || {
                FxHashMap::<(pg_sys::Oid, Option<String>), FxHashSet<u64>>::default()
            });

            lookup_by_query
                .entry((index_oid, Some(query.query_string)))
                .or_insert_with(|| do_seqscan(query, index_oid))
                .contains(&tid)
        },
        None => false,
    }
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
CREATE OPERATOR pg_catalog.~> (
    PROCEDURE = quria_fts,
    LEFTARG = quria.fulltext,
    RIGHTARG = quria_fts_query
);

CREATE OPERATOR CLASS fts_quria_ops DEFAULT quria.fulltext USING quria_fts AS
    OPERATOR 1 pg_catalog.~>(quria.fulltext, quria_fts_query)
    STORAGE quria.fulltext;

"#,
    name = "quria_ops_anyelement_fts_operator"
);