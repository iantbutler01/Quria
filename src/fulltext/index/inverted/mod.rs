pub mod extension;
mod inverted;

pub use inverted::{
    get_index_manager, Index, IndexBuildState, IndexManager, IndexResultIterator, OptionalRange,
};
