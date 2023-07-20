use crate::fulltext::index::inverted::{get_index_manager, Index};
use crate::fulltext::{get_executor_manager, ExecutorManager};
use pgrx::*;

#[pg_guard]
pub extern "C" fn ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
    _callback: pg_sys::IndexBulkDeleteCallback,
    _callback_state: *mut ::std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let result = unsafe { PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0() };
    let info = unsafe { PgBox::from_pg(info) };
    let index_relation = unsafe { PgRelation::from_pg(info.index) };

    let em = get_executor_manager();

    let index_manager = get_index_manager();
    let index_name = vec![
        index_relation.namespace().clone(),
        ".",
        index_relation.name().clone(),
    ]
    .concat();

    pgrx::debug1!("ambulkdelete: Index Name: {:?}", index_name);
    let mut index = index_manager
        .get_index_mut(index_name.as_str())
        .expect("Expected to have an index already for vacuum.");

    let oldest_xmin = {
        #[cfg(any(feature = "pg10", feature = "pg11", feature = "pg12", feature = "pg13"))]
        unsafe {
            pg_sys::TransactionIdLimitedForOldSnapshots(
                pg_sys::GetOldestXmin(info.index, pg_sys::PROCARRAY_FLAGS_VACUUM as i32),
                info.index,
            )
        }

        #[cfg(any(feature = "pg14", feature = "pg15"))]
        unsafe {
            pg_sys::GetOldestNonRemovableTransactionId(std::ptr::null_mut())
        }
    };

    let _by_xmin = delete_by_xmin(em, &mut index, oldest_xmin);

    // Find all rows with what we think is a *committed* xmax
    // These rows can be deleted
    let _by_xmax = delete_by_xmax(em, &mut index, oldest_xmin);

    // Find all rows with what we think is an *aborted* xmax
    // These rows can have their xmax reset to null because they're still live
    let _vacuumed = vacuum_xmax(em, &mut index, oldest_xmin);

    // Finally, any "zdb_aborted_xid" value we have can be removed if it's
    // known to be aborted and no longer referenced anywhere in the index
    let _aborted = remove_aborted_xids(&index, oldest_xmin);

    result.into_pg()
}

#[pg_guard]
pub extern "C" fn amvacuumcleanup(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let result = unsafe { PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0() };
    let info = unsafe { PgBox::from_pg(info) };

    if stats.is_null() {
        ambulkdelete(info.as_ptr(), result.as_ptr(), None, std::ptr::null_mut());
    }

    result.into_pg()
}

fn remove_aborted_xids(_index: &Index, _oldest_xmin: u32) -> usize {
    0
}

fn vacuum_xmax(em: &ExecutorManager, index: &mut Index, oldest_xmin: u32) -> usize {
    let xmin = xid_to_64bit(oldest_xmin);
    let mut cnt = 0;

    let targets = em.find_vacuum_targets(None, Some(xmin));

    for xid in targets.into_iter() {
        check_for_interrupts!();

        let xmax64 = xid;
        let xmax = xmax64 as pg_sys::TransactionId;

        if unsafe { pg_sys::TransactionIdPrecedes(xmax, oldest_xmin) }
            && unsafe { pg_sys::TransactionIdDidAbort(xmax) }
            && !unsafe { pg_sys::TransactionIdDidCommit(xmax) }
            && !unsafe { pg_sys::TransactionIdIsInProgress(xmax) }
        {
            index
                .delete_by_xid(None, Some(xmax64))
                .expect("Expected delete to succeed.");

            cnt += 1;
        }
    }

    cnt
}

fn delete_by_xmax(em: &ExecutorManager, index: &mut Index, oldest_xmin: u32) -> usize {
    let mut cnt = 0;

    let xmin = xid_to_64bit(oldest_xmin);

    let targets = em.find_vacuum_targets(None, Some(xmin));

    for xid in targets.into_iter() {
        check_for_interrupts!();

        let xmax64 = xid;
        let xmax = xmax64 as pg_sys::TransactionId;

        if unsafe { pg_sys::TransactionIdPrecedes(xmax, oldest_xmin) }
            && unsafe { pg_sys::TransactionIdDidCommit(xmax) }
            && !unsafe { pg_sys::TransactionIdDidAbort(xmax) }
            && !unsafe { pg_sys::TransactionIdIsInProgress(xmax) }
        {
            index
                .delete_by_xid(None, Some(xmax64))
                .expect("Expected delete to succeed.");

            cnt += 1;
        }
    }

    cnt
}

fn delete_by_xmin(em: &ExecutorManager, index: &mut Index, oldest_xmin: u32) -> usize {
    let mut cnt = 0;

    let xmin = xid_to_64bit(oldest_xmin);

    let targets = em.find_vacuum_targets(Some(xmin), None);

    for xid in targets.into_iter() {
        check_for_interrupts!();

        let xmin64 = xid;
        let xmin = xmin64 as pg_sys::TransactionId;

        if unsafe { pg_sys::TransactionIdPrecedes(xmin, oldest_xmin) }
            && unsafe { pg_sys::TransactionIdDidAbort(xmin) }
            && !unsafe { pg_sys::TransactionIdDidCommit(xmin) }
            && !unsafe { pg_sys::TransactionIdIsInProgress(xmin) }
        {
            index
                .delete_by_xid(Some(xmin64), None)
                .expect("Expected delete to succeed.");

            cnt += 1;
        }
    }
    cnt
}
