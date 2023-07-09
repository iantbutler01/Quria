use pgrx::*;

// use crate::access_method::triggers::create_triggers;
use crate::fulltext::index::inverted::{get_index_manager, Index, IndexBuildState};
use rustc_hash::FxHashMap;

struct BuildState<'a> {
    memcxt: PgMemoryContexts,
    idx_build_state: &'a IndexBuildState,
}

impl<'a> BuildState<'a> {
    fn new(idx_build_state: &IndexBuildState) -> Self {
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

    let index_manager = get_index_manager();
    let index_name = vec![
        index_relation.namespace().clone(),
        ".",
        index_relation.name().clone(),
    ]
    .concat();

    let index = index_manager.get_or_init_index(&index_name);

    // register a callback to delete the newly-created index if our transaction aborts
    register_xact_callback(PgXactCallbackEvent::Abort, move || {
        let index_manager = get_index_manager();

        index_manager.poison_index(&index_name);

        if let Err(e) = index_manager.delete_index(&index_name) {
            // we can't panic here b/c we're already in the ABORT stage
            warning!(
                "failed to delete Quria FTS index on transaction abort: {:?}",
                e
            )
        }
    });

    let ntuples = do_heap_scan(index_info, &heap_relation, &index_relation, index);

    // // create the triggers we need on the table to which this index is attached
    // if !heap_relation.is_matview() {
    //     create_triggers(&index_relation);
    // }

    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = ntuples as f64;
    result.index_tuples = ntuples as f64;

    result.into_pg()
}

fn do_heap_scan<'a>(
    index_info: *mut pg_sys::IndexInfo,
    heap_relation: &'a PgRelation,
    index_relation: &'a PgRelation,
    index: &Index,
) -> usize {
    let index_manager = get_index_manager();
    let index_name = vec![
        index_relation.namespace().clone(),
        ".",
        index_relation.name().clone(),
    ]
    .concat();

    let index_tupedesc = lookup_index_tupdesc(index_relation);

    let mut idx_build_state = index_manager.get_or_insert_build_state(&index_name, index_tupedesc);
    let mut state = BuildState::new(idx_build_state);

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
    let index_relation = PgRelation::from_pg(index_relation);
    let index_manager = get_index_manager();
    let index_name = vec![
        index_relation.namespace().clone(),
        ".",
        index_relation.name().clone(),
    ]
    .concat();

    let ctid = item_pointer_to_u64(*heap_tid);

    let idx_build_state = index_manager
        .get_build_state(&index_name)
        .expect("IndexBuildState not found");
    let index = index_manager
        .get_index(&index_name)
        .expect("Index not found");

    let values = std::slice::from_raw_parts(values, 1);
    let row = row_to_hashmap(values[0], idx_build_state.tupedesc, ctid);
    let cmin = pg_sys::GetCurrentCommandId(true);
    let cmax = cmin;
    let xmin = xid_to_64bit(pg_sys::GetCurrentTransactionId());
    let xmax = pg_sys::InvalidTransactionId as u64;

    let doc_id_data = row.get("id").expect("id column not found").value;
    let doc_data = row.get("doc").expect("doc column not found").value;

    let doc_id = match doc_id_data {
        RowData::Int(i) => i as u64,
        _ => panic!("id column must be an integer"),
    };

    let doc = match doc_data {
        RowData::String(s) => s,
        _ => panic!("doc column must be a jsonb value"),
    };

    match index.add_doc(doc_id, &doc, ctid, (xmin, xmax), (cmin, cmax)) {
        Ok(_) => {
            idx_build_state.num_inserted += 1;
            true
        }
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
    check_for_interrupts!();

    let state = (state as *mut BuildState).as_mut().unwrap();

    let mut old_context = state.memcxt.set_as_current();

    let values = std::slice::from_raw_parts(values, 1);

    let u64_ctid = item_pointer_to_u64(ctid);

    let row = row_to_hashmap(values[0], state.idx_build_state.tupedesc, u64_ctid);

    let cmin = pg_sys::FirstCommandId;
    let cmax = cmin;
    let xmin = pg_sys::FirstNormalTransactionId as u64;
    let xmax = pg_sys::InvalidTransactionId as u64;

    let index_manager = get_index_manager();
    let index = index_manager
        .get_index(&state.idx_build_state.index_id)
        .expect("Index not found");

    let doc_id_data = row.get("id").expect("id column not found").value;
    let doc_data = row.get("doc").expect("doc column not found").value;

    let doc_id = match doc_id_data {
        RowData::Int(i) => i as u64,
        _ => panic!("id column must be an integer"),
    };

    let doc = match doc_data {
        RowData::String(s) => s,
        _ => panic!("doc column must be a jsonb value"),
    };

    index
        .add_doc(doc_id, &doc, u64_ctid, (xmin, xmax), (cmin, cmax))
        .expect("Failed to insert document into Quria FTS index");

    old_context.set_as_current();
    state.memcxt.reset();
}

pub fn lookup_index_tupdesc(indexrel: &PgRelation) -> PgTupleDesc<'static> {
    let tupdesc = indexrel.tuple_desc();

    let typid = tupdesc
        .get(0)
        .expect("no attribute #0 on tupledesc")
        .type_oid()
        .value();
    let typmod = tupdesc
        .get(0)
        .expect("no attribute #0 on tupledesc")
        .type_mod();

    // lookup the tuple descriptor for the rowtype we're *indexing*, rather than
    // using the tuple descriptor for the index definition itself
    unsafe {
        PgMemoryContexts::TopTransactionContext.switch_to(|_| {
            PgTupleDesc::from_pg_is_copy(pg_sys::lookup_rowtype_tupdesc_copy(typid, typmod))
        })
    }
}

unsafe fn row_to_hashmap(
    row: pg_sys::Datum,
    row_desc: PgTupleDesc,
    row_ctid: u64,
) -> FxHashMap<String, RowAttribute> {
    let value = unsafe {
        let mut mapping = FxHashMap::<String, RowAttribute>::default();
        let td = pg_sys::pg_detoast_datum(row.cast_mut_ptr::<pg_sys::varlena>())
            as pg_sys::HeapTupleHeader;
        let mut tmptup = pg_sys::HeapTupleData {
            t_len: varsize(td as *mut pg_sys::varlena) as u32,
            t_self: Default::default(),
            t_tableOid: pg_sys::Oid::INVALID,
            t_data: td,
        };

        let mut datums = vec![pg_sys::Datum::from(0 as usize); row_desc.natts.try_into().unwrap()];
        let mut nulls = vec![false; row_desc.natts.try_into().unwrap()];

        pg_sys::heap_deform_tuple(
            &mut tmptup,
            row_desc.as_ptr(),
            datums.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );

        for (attno, attribute) in row_desc.iter().enumerate() {
            let name = attribute.name().to_string();
            let typoid = attribute.type_oid().value();

            let datum = *&datums[attno];
            let data = RowAttribute::convert(datum, typoid);

            match data {
                Some(data) => {
                    let is_primary = attribute.name().to_string() == "id";
                    let is_null = attribute.is_dropped();

                    let attribute =
                        RowAttribute::new(name, typoid, attno, data, row_ctid, is_null, is_primary);

                    mapping.insert(name, attribute);
                }
                None => continue,
            }
        }

        mapping
    };

    value
}
pub struct RowAttribute {
    pub attname: String,
    pub typoid: pg_sys::Oid,
    pub attno: usize,
    pub value: RowData,
    pub ctid: u64,
    pub is_null: bool,
    pub is_primary: bool,
}

enum RowData {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
}

impl RowAttribute {
    pub fn new(
        attname: String,
        typoid: pg_sys::Oid,
        attno: usize,
        value: RowData,
        ctid: u64,
        is_null: bool,
        is_primary: bool,
    ) -> Self {
        RowAttribute {
            attname,
            typoid,
            attno,
            value,
            ctid,
            is_null,
            is_primary,
        }
    }

    pub fn convert(datum: pg_sys::Datum, oid: pg_sys::Oid) -> Option<RowData> {
        let mut attr_oid = unsafe { pg_sys::get_element_type(oid) };

        loop {
            let base_oid = PgOid::from(attr_oid);

            break match &base_oid {
                PgOid::BuiltIn(builtin) => match builtin {
                    PgBuiltInOids::TEXTOID => Some(RowData::String(
                        RowAttribute::handle_as_generic_string(datum),
                    )),
                    PgBuiltInOids::INT2OID => {
                        let val = unsafe { i16::from_datum(datum, false) }
                            .expect("failed to convert int2 to i16");

                        Some(RowData::Int(val.into()))
                    }
                    PgBuiltInOids::INT4OID => {
                        let val = unsafe { i32::from_datum(datum, false) }
                            .expect("failed to convert int4 to i32");

                        Some(RowData::Int(val.into()))
                    }
                    PgBuiltInOids::INT8OID => {
                        let val = unsafe { i64::from_datum(datum, false) }
                            .expect("failed to convert int8 to i64");

                        Some(RowData::Int(val.into()))
                    }
                    _ => None,
                },
                PgOid::Custom(custom) => {
                    if let Some((oid, _)) = RowAttribute::type_is_domain(*custom) {
                        attr_oid = oid;
                        continue;
                    } else {
                        Some(RowData::String(RowAttribute::handle_as_generic_string(
                            datum,
                        )))
                    }
                }

                PgOid::Invalid => {
                    panic!("InvalidOid in row conversion.")
                }
            };
        }
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
    pub fn type_is_domain(typoid: pg_sys::Oid) -> Option<(pg_sys::Oid, String)> {
        let (is_domain, base_type, name) = Spi::get_three_with_args::<bool, pg_sys::Oid, String>(
            "SELECT typtype = 'd', typbasetype, typname::text FROM pg_type WHERE oid = $1",
            vec![(PgBuiltInOids::OIDOID.oid(), typoid.into_datum())],
        )
        .expect("SPI failed");

        if is_domain.unwrap_or(false) {
            Some((base_type.unwrap(), name.unwrap()))
        } else {
            None
        }
    }
}
