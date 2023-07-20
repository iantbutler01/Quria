// use memoffset::*;
use pgrx::prelude::*;
use pgrx::*;
// use std::ffi::CStr;
use std::fmt::Debug;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
struct InvertedIndexOptionsInternal {
    /* varlena header (do not touch directly!) */
    #[allow(dead_code)]
    vl_len_: i32,
}

#[allow(dead_code)]
impl InvertedIndexOptionsInternal {
    fn from_relation(relation: &PgRelation) -> PgBox<InvertedIndexOptionsInternal> {
        if relation.rd_index.is_null() {
            panic!("'{}' is not a Quria index", relation.name())
        } else if relation.rd_options.is_null() {
            // use defaults
            let ops = unsafe { PgBox::<InvertedIndexOptionsInternal>::alloc0() };

            unsafe {
                set_varsize(
                    ops.as_ptr().cast(),
                    std::mem::size_of::<InvertedIndexOptionsInternal>() as i32,
                );
            }
            ops.into_pg_boxed()
        } else {
            unsafe { PgBox::from_pg(relation.rd_options as *mut InvertedIndexOptionsInternal) }
        }
    }

    fn into_bytes(options: PgBox<InvertedIndexOptionsInternal>) -> Vec<u8> {
        unsafe {
            let ptr = options.as_ptr().cast::<u8>();
            let varsize = varsize(ptr.cast());
            let mut bytes = Vec::with_capacity(varsize);
            std::ptr::copy(ptr, bytes.as_mut_ptr(), varsize);
            bytes.set_len(varsize);
            bytes
        }
    }
    fn copy_to(
        options: &PgBox<InvertedIndexOptionsInternal>,
        mut memcxt: PgMemoryContexts,
    ) -> PgBox<InvertedIndexOptionsInternal> {
        unsafe {
            let ptr = options.as_ptr();
            PgBox::from_pg(memcxt.copy_ptr_into(ptr.cast(), varsize(ptr.cast())))
        }
    }
}

#[derive(Clone)]
pub struct InvertedIndexOptions {
    internal: Vec<u8>,
    oid: pg_sys::Oid,
    // alias: String,
    // uuid: String,
    // options: Option<Vec<String>>,
}

#[allow(dead_code)]
impl InvertedIndexOptions {
    #[inline(always)]
    fn internal(&self) -> &InvertedIndexOptionsInternal {
        let ptr = self.internal.as_ptr();
        unsafe {
            (ptr as *const _ as *const InvertedIndexOptionsInternal)
                .as_ref()
                .unwrap()
        }
    }

    pub fn index_relation(&self) -> PgRelation {
        unsafe { PgRelation::with_lock(self.oid(), pg_sys::AccessShareLock as pg_sys::LOCKMODE) }
    }

    pub fn heap_relation(&self) -> PgRelation {
        self.index_relation().heap_relation().expect("not an index")
    }

    pub fn oid(&self) -> pg_sys::Oid {
        self.oid
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    raw,
    no_guard,
    sql = r#"
        -- we don't want any SQL generated for the "shadow" function, but we do want its '_wrapper' symbol
        -- exported so that shadow indexes can reference it using whatever argument type they want
    "#
)]
fn shadow(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    unsafe { pg_getarg_datum_raw(fcinfo, 0) }
}

static mut RELOPT_KIND_QURIA: pg_sys::relopt_kind = 0;

// #[pg_guard]
// extern "C" fn validate_translog_durability(value: *const std::os::raw::c_char) {
//     if value.is_null() {
//         // null is fine -- we'll just use our default
//         return;
//     }

//     let value = unsafe { CStr::from_ptr(value) }
//         .to_str()
//         .expect("failed to convert translog_durability to utf8");
//     if value != "request" && value != "async" {
//         panic!(
//             "invalid translog_durability setting.  Must be one of 'request' or 'async': {}",
//             value
//         )
//     }
// }

const NUM_REL_OPTS: usize = 0;
#[allow(clippy::unneeded_field_pattern)] // b/c of offset_of!()
#[pg_guard]
pub unsafe extern "C" fn amoptions(
    reloptions: pg_sys::Datum,
    validate: bool,
) -> *mut pg_sys::bytea {
    // TODO:  how to make this const?  we can't use offset_of!() macro in const definitions, apparently
    let tab: [pg_sys::relopt_parse_elt; NUM_REL_OPTS] = [];

    build_relopts(reloptions, validate, tab)
}

#[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15"))]
unsafe fn build_relopts(
    reloptions: pg_sys::Datum,
    validate: bool,
    tab: [pg_sys::relopt_parse_elt; NUM_REL_OPTS],
) -> *mut pg_sys::bytea {
    let rdopts;

    /* Parse the user-given reloptions */
    rdopts = pg_sys::build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND_QURIA,
        std::mem::size_of::<InvertedIndexOptionsInternal>(),
        tab.as_ptr(),
        NUM_REL_OPTS as i32,
    );

    rdopts as *mut pg_sys::bytea
}

#[cfg(any(feature = "pg10", feature = "pg11", feature = "pg12"))]
unsafe fn build_relopts(
    reloptions: pg_sys::Datum,
    validate: bool,
    tab: [pg_sys::relopt_parse_elt; NUM_REL_OPTS],
) -> *mut pg_sys::bytea {
    let mut noptions = 0;
    let options = pg_sys::parseRelOptions(reloptions, validate, RELOPT_KIND_QURIA, &mut noptions);
    if noptions == 0 {
        return std::ptr::null_mut();
    }

    for relopt in std::slice::from_raw_parts_mut(options, noptions as usize) {
        relopt.gen.as_mut().unwrap().lockmode = pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE;
    }

    let rdopts = pg_sys::allocateReloptStruct(
        std::mem::size_of::<InvertedIndexOptionsInternal>(),
        options,
        noptions,
    );
    pg_sys::fillRelOptions(
        rdopts,
        std::mem::size_of::<InvertedIndexOptionsInternal>(),
        options,
        noptions,
        validate,
        tab.as_ptr(),
        tab.len() as i32,
    );
    pg_sys::pfree(options as void_mut_ptr);

    rdopts as *mut pg_sys::bytea
}

pub unsafe fn init() {}
