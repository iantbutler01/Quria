use crate::fulltext::index::inverted::extension::{build, cost, options, scan, vacuum};
use pgrx::*;

#[pg_extern(sql = "
CREATE OR REPLACE FUNCTION amhandler(internal) RETURNS index_am_handler PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'amhandler_wrapper';
CREATE ACCESS METHOD quria_fts TYPE INDEX HANDLER amhandler;
")]
fn amhandler(_fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::IndexAmRoutine> {
    let mut amroutine =
        unsafe { PgBox::<pg_sys::IndexAmRoutine>::alloc_node(pg_sys::NodeTag_T_IndexAmRoutine) };

    amroutine.amstrategies = 1;
    amroutine.amsupport = 0;
    amroutine.amcanmulticol = false;
    amroutine.amsearcharray = false;

    amroutine.amkeytype = pg_sys::InvalidOid;

    amroutine.amvalidate = Some(amvalidate);
    amroutine.ambuild = Some(build::ambuild);
    amroutine.ambuildempty = Some(build::ambuildempty);
    amroutine.aminsert = Some(build::aminsert);
    amroutine.ambulkdelete = Some(vacuum::ambulkdelete);
    amroutine.amvacuumcleanup = Some(vacuum::amvacuumcleanup);
    amroutine.amcostestimate = Some(cost::amcostestimate);
    amroutine.amoptions = Some(options::amoptions);
    amroutine.ambeginscan = Some(scan::ambeginscan);
    amroutine.amrescan = Some(scan::amrescan);
    amroutine.amgettuple = Some(scan::amgettuple);
    amroutine.amgetbitmap = Some(scan::ambitmapscan);
    amroutine.amendscan = Some(scan::amendscan);

    amroutine.into_pg_boxed()
}

#[pg_guard]
pub extern "C" fn amvalidate(_opclassoid: pg_sys::Oid) -> bool {
    true
}
