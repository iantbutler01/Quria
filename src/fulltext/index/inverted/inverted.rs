use dashmap::{DashMap, DashSet};
use pgrx::pg_sys::ScanKeyData;
use pgrx::*;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use shardio::*;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::available_parallelism;

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct TermFrequency<'a> {
    term: &'a str,
    doc_id: u64,
    frequency: u64,
    ctid: u64,
    xrange: (u64, u64),
    crange: (u32, u32),
}

pub struct Snapshot<'a> {
    pub term_frequencies: Vec<TermFrequency<'a>>,
    pub table: &'a str,
}

impl<'a> Snapshot<'a> {
    fn from_index(index: &Index) -> Self {
        let table = index.table.clone();
        let mut term_frequencies = Vec::<TermFrequency>::new();

        index.term_frequencies_per_doc.iter().for_each(|term| {
            term.value().iter().for_each(|freq| {
                let tf = TermFrequency {
                    term: term.key(),
                    doc_id: *freq.key(),
                    frequency: freq.value().into_inner(),
                    ctid: index
                        .docid_to_ctid
                        .get(freq.key())
                        .expect("Expected CTID to exist for doc_id")
                        .clone(),
                    xrange: index
                        .docid_to_xrange
                        .get(freq.key())
                        .expect("Expected doc_id to match to an xmin")
                        .clone(),
                    crange: index
                        .docid_to_crange
                        .get(freq.key())
                        .expect("Expected doc_id to match to an xmin")
                        .clone(),
                };
                term_frequencies.push(tf);
            });
        });

        Snapshot {
            term_frequencies,
            table,
        }
    }

    fn from_disk(table: &'a str) -> Result<Self, Box<dyn Error>> {
        let reader = ShardReader::<TermFrequency>::open(table)?;
        let default_parallelism_approx = available_parallelism().unwrap().get();

        let mut tfs = Vec::<TermFrequency>::new();
        let chunks = reader.make_chunks(default_parallelism_approx, &shardio::Range::all());

        chunks.par_iter().for_each(|c| {
            let mut range_iter = reader.iter_range(&c).unwrap();
            for i in range_iter {
                let tf = i.unwrap();

                tfs.push(tf);
            }
        });

        Ok(Snapshot {
            term_frequencies: tfs,
            table,
        })
    }

    fn into_index(&self) -> Result<Index, Box<dyn Error>> {
        let mut tfpd = DashMap::<&'a str, DashMap<u64, AtomicU64>>::new();
        let mut docid_to_ctid = FxHashMap::<u64, u64>::default();

        self.term_frequencies.par_iter().for_each(|tf| {
            let term_frequencies = tfpd
                .entry(tf.term)
                .or_insert_with(|| DashMap::<u64, AtomicU64>::new());
            let val = term_frequencies
                .entry(tf.doc_id)
                .or_insert_with(|| AtomicU64::new(0));

            docid_to_ctid.entry(tf.doc_id).or_insert(tf.ctid);

            val.fetch_add(tf.frequency, Ordering::SeqCst);
        });

        Ok(Index::new(self.table, Some(tfpd)))
    }

    fn flush_to_disk(&self) -> Result<usize, Box<dyn Error>> {
        let mut writer: ShardWriter<TermFrequency> =
            ShardWriter::new(self.table, 1024, 4096, 1 << 16)?;
        let mut sender = writer.get_sender();

        self.term_frequencies.par_iter().for_each(|tf| {
            sender.send(tf.clone()).unwrap();
        });

        Ok(writer.finish()?)
    }
}

pub struct IndexBuildState {
    pub num_inserted: usize,
    pub index_id: String,
    pub tupedesc: PgTupleDesc<'static>,
}
impl<'a> IndexBuildState {
    pub fn new(index_id: String, tupedesc: PgTupleDesc) -> Self {
        IndexBuildState {
            num_inserted: 0,
            index_id,
            tupedesc,
        }
    }
}
pub struct IndexManager<'a> {
    pub indexes: FxHashMap<&'a str, Index<'a>>,
    pub index_build_states: FxHashMap<&'a str, IndexBuildState>,
}

impl<'a> IndexManager<'a> {
    pub fn new() -> Self {
        IndexManager {
            indexes: FxHashMap::<&'a str, Index<'a>>::default(),
            index_build_states: FxHashMap::<&'a str, IndexBuildState>::default(),
        }
    }

    pub fn get_or_init_index(&mut self, table: &'a str) -> &Index<'a> {
        match self.indexes.get(table) {
            Some(index) => index,

            None => {
                let index = Index::load_or_init(table)
                    .expect("Index should have been loaded or initialized");

                self.indexes.insert(table, index);

                self.indexes.get(table).unwrap()
            }
        }
    }

    pub fn get_index(&self, table: &'a str) -> Option<&Index<'a>> {
        self.indexes.get(table)
    }

    pub fn poison_index(&mut self, table: &'a str) {
        match self.indexes.get_mut(table) {
            Some(index) => {
                index.poisoned = true;
            }

            None => {}
        }
    }

    pub fn get_or_insert_build_state(
        &mut self,
        table: &'a str,
        tupdesc: PgTupleDesc,
    ) -> &IndexBuildState {
        match self.index_build_states.get(table) {
            Some(state) => state,

            None => {
                let state = IndexBuildState::new(table.to_string(), tupdesc);

                self.index_build_states.insert(table, state);
                self.index_build_states.get(table).unwrap()
            }
        }
    }

    pub fn get_build_state(&self, table: &'a str) -> Option<&IndexBuildState> {
        self.index_build_states.get(table)
    }

    pub fn delete_index(&mut self, table: &'a str) -> Result<(), Box<dyn Error>> {
        match self.indexes.remove(table) {
            Some(index) => Ok(()),

            None => Ok(()),
        }
    }
}

static mut INDEX_MANAGER: IndexManager = IndexManager::new();

pub fn get_index_manager() -> &'static mut IndexManager<'static> {
    unsafe { &mut INDEX_MANAGER }
}

#[derive(Serialize, Deserialize)]
pub struct Index<'a> {
    pub term_frequencies_per_doc: DashMap<&'a str, DashMap<u64, AtomicU64>>,
    pub docid_to_ctid: FxHashMap<u64, u64>,
    pub docid_to_xrange: FxHashMap<u64, (u64, u64)>,
    pub docid_to_crange: FxHashMap<u64, (u32, u32)>,
    pub normalizer: Normalizer<'a>,
    pub table: &'a str,
    pub poisoned: bool,
}

impl<'a> Index<'a> {
    fn new(
        table: &'a str,
        term_frequencies_per_doc: Option<DashMap<&'a str, DashMap<u64, AtomicU64>>>,
    ) -> Self {
        let mut tfpd =
            term_frequencies_per_doc.unwrap_or(DashMap::<&'a str, DashMap<u64, AtomicU64>>::new());

        Index {
            term_frequencies_per_doc: tfpd,
            docid_to_ctid: FxHashMap::default(),
            docid_to_xrange: FxHashMap::default(),
            docid_to_crange: FxHashMap::default(),
            normalizer: Normalizer::new(),
            table,
            poisoned: false,
        }
    }

    pub fn add_doc(
        &mut self,
        doc_id: u64,
        doc: &str,
        doc_ctid: u64,
        doc_xrange: (u64, u64),
        doc_crange: (u32, u32),
    ) -> Result<(), Box<dyn Error>> {
        let words = doc.split_whitespace().collect::<Vec<&str>>();
        let words = self.normalizer.normalize(words);

        self.docid_to_xrange.insert(doc_id, doc_xrange);
        self.docid_to_crange.insert(doc_id, doc_crange);

        words.par_iter().for_each(|word| {
            if self.term_frequencies_per_doc.contains_key(word) {
                let term_frequencies = self
                    .term_frequencies_per_doc
                    .get(word)
                    .expect("Term frequencies should exist but were not found");

                if term_frequencies.contains_key(&doc_id) {
                    let val = term_frequencies
                        .get(&doc_id)
                        .expect("Term frequencies should exist but were not found");
                    val.fetch_add(1, Ordering::SeqCst);
                }
            } else {
                let term_frequencies = DashMap::<u64, AtomicU64>::new();
                term_frequencies.insert(doc_id, AtomicU64::new(1));

                self.term_frequencies_per_doc.insert(word, term_frequencies);
            }
        });

        Ok(())
    }

    pub fn search(&self, term: &'a str) -> Vec<(&'a str, u64, u64)> {
        let mut results = Vec::<(&'a str, u64, u64)>::new();

        match self.term_frequencies_per_doc.get(term) {
            Some(term_frequencies) => {
                term_frequencies.iter().for_each(|tf| {
                    results.push((term, *tf.key(), tf.value().into_inner()));
                });
            }

            None => {}
        }

        results
    }

    pub fn scan(&self, query: String) -> Vec<(&str, u64, u64)> {
        let mut vecs = Arc::new(Vec::<(&'a str, u64, u64)>::new());

        let words = query.split_whitespace().collect::<Vec<&str>>();
        let words = self.normalizer.normalize(words);

        let mutex = Mutex::new(vecs);

        words.par_iter().map(|term| {
            let res = self.search(term);

            match mutex.lock() {
                Ok(mut res_vec) => res.iter().for_each(|ele| {
                    res_vec.push(*ele);
                }),
                Err(e) => {
                    panic!("Unable to acquire mutex lock for result vector in scan.");
                }
            }
        });

        vecs.to_vec()
    }

    fn load_or_init(table: &'a str) -> Result<Self, Box<dyn Error>> {
        match Self::load_from_disk(table) {
            Ok(index) => Ok(index),

            Err(_) => {
                let index = Index::new(table, None);

                Ok(index)
            }
        }
    }

    pub fn flush_to_disk(&self) -> Result<usize, Box<dyn Error>> {
        let mut snapshot = Snapshot::from_index(self);

        snapshot.flush_to_disk()
    }

    pub fn poison(&mut self) {
        self.poisoned = true;
    }

    fn load_from_disk(table: &'a str) -> Result<Self, Box<dyn Error>> {
        let snapshot = Snapshot::from_disk(table)?;

        snapshot.into_index()
    }
}

impl Drop for Index<'_> {
    fn drop(&mut self) {
        if !self.poisoned {
            self.flush_to_disk()
                .expect("Index should have been flushed to disk");
        }
    }
}

pub struct IndexResultIterator<'a> {
    results_iter: Box<dyn Iterator<Item = (&'a str, u64, u64)>>,
}

impl<'a> Iterator for IndexResultIterator<'a> {
    type Item = (&'a str, u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        self.results_iter.next()
    }
}

impl From<Vec<(&str, u64, u64)>> for IndexResultIterator<'_> {
    fn from(results: Vec<(&str, u64, u64)>) -> Self {
        IndexResultIterator {
            results_iter: Box::new(results.into_iter()),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Lemmatizer<'a> {
    lemma_file_content: &'a str,
    lemmas: DashMap<&'a str, &'a str>,
}

impl<'a> Lemmatizer<'a> {
    fn new() -> Self {
        Lemmatizer {
            lemma_file_content: include_str!("./data/lemmas.en.txt"),
            lemmas: DashMap::<&'a str, &'a str>::new(),
        }
    }

    fn load_lemmas(&self) -> Result<(), Box<dyn Error>> {
        let lines = self.lemma_file_content.split("\n").collect::<Vec<&str>>();
        lines.iter().skip(5).for_each(|line| {
            let parts: Vec<&str> = line.split("->").collect();
            let lemma = parts[0].trim();
            let forms = parts[1].trim();

            forms.split(",").for_each(|form| {
                self.lemmas.insert(form.trim(), lemma);
            })
        });

        Ok(())
    }

    fn lemmatize(&self, word: &str) -> &str {
        match self.lemmas.get(word) {
            Some(val) => &val,
            None => word,
        }
    }
}
#[derive(Serialize, Deserialize)]
pub struct Normalizer<'a> {
    lemmatizer: Lemmatizer<'a>,
    stopwords: StopWords<'a>,
}

impl<'a> Normalizer<'a> {
    fn new() -> Self {
        Normalizer {
            lemmatizer: Lemmatizer::new(),
            stopwords: StopWords::new(),
        }
    }

    fn normalize(&self, document: Vec<&str>) -> Vec<&str> {
        document
            .par_iter()
            .map(|word| {
                let word = word.to_lowercase().as_str();
                self.lemmatizer.lemmatize(word)
            })
            .filter(|word| !self.stopwords.check_word(word))
            .collect()
    }
}
#[derive(Serialize, Deserialize)]
pub struct StopWords<'a> {
    word_set: DashSet<&'a str>,
}

impl<'a> StopWords<'a> {
    fn new() -> Self {
        StopWords {
            word_set: DashSet::<&'a str>::with_capacity(1000000),
        }
    }

    fn load_basic_stop_words(&self) -> Result<(), Box<dyn Error>> {
        let stop_words = include_str!("./data/stopwords.en.txt");
        let stop_words = stop_words.split("\n").collect::<Vec<&str>>();
        stop_words.iter().for_each(|word| {
            self.word_set.insert(word);
        });

        Ok(())
    }

    fn add_word(&self, word: &str) -> Result<bool, Box<dyn Error>> {
        Ok(self.word_set.insert(word))
    }

    fn remove_word(&self, word: &str) -> Result<Option<&'a str>, Box<dyn Error>> {
        Ok(self.word_set.remove(word))
    }

    fn check_word(&self, word: &str) -> bool {
        self.word_set.contains(word)
    }

    fn filter_stop_words(&self, document: Vec<&str>) -> Vec<&str> {
        document
            .par_iter()
            .filter(|word| self.check_word(word))
            .copied()
            .collect()
    }
}
