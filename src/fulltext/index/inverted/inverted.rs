use crate::fulltext::data_dir;
use crate::fulltext::types::*;
use dashmap::{DashMap, DashSet};
use once_cell::sync::Lazy;
use pgrx::*;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use shardio::*;
use std::error::Error;
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::available_parallelism;

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub struct TermFrequency {
    term: String,
    doc_id: u64,
    frequency: u64,
    ctid: u64,
    xrange: OptionalRange<u64>,
    crange: OptionalRange<u32>,
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexStatistics {
    average_doc_length: f64,
    doc_lengths: FxHashMap<u64, u64>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Copy)]
pub struct OptionalRange<T> {
    pub min: Option<T>,
    pub max: Option<T>,
}

pub struct Snapshot {
    pub term_frequencies: Vec<TermFrequency>,
    pub index_statistics: IndexStatistics,
    pub table: String,
}

impl Snapshot {
    fn from_index(index: &Index) -> Self {
        let table = index.table.clone();
        let mut term_frequencies = Vec::<TermFrequency>::new();

        let index_statistics = IndexStatistics {
            doc_lengths: index.doc_lengths.clone(),
            average_doc_length: index.average_doc_length,
        };

        index.term_frequencies_per_doc.iter().for_each(|term| {
            term.value().iter().for_each(|freq| {
                let tf = TermFrequency {
                    term: term.key().to_string(),
                    doc_id: *freq.key(),
                    frequency: freq.fetch_add(0, Ordering::Relaxed),
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
            index_statistics,
            table,
        }
    }

    fn from_disk(table: String) -> Result<Self, Box<dyn Error>> {
        let mut path = data_dir();
        path.extend(vec![table.clone()]);
        let mut path = data_dir();
        path.extend(vec![table.clone()]);

        let mut stats_path = path.clone();
        stats_path.extend(vec!["statistics.json"]);

        let stats_json = std::fs::read(stats_path)?;
        let stats: IndexStatistics = serde_json::from_slice(&stats_json)?;

        let mut data_path = path.clone();
        data_path.extend(vec!["shards", "data"]);

        // Shardio unwraps files which ignores the soft error return entirely, so this is a little hack to short cut that without having to trap a panic.
        let f = File::open(&data_path)?;
        std::mem::drop(f);

        let reader = ShardReader::<TermFrequency>::open(data_path)?;
        pgrx::info!("Did not boom.");
        let default_parallelism_approx = available_parallelism().unwrap().get();

        let tfs = Arc::new(Mutex::new(Vec::<TermFrequency>::new()));
        let chunks = reader.make_chunks(default_parallelism_approx, &shardio::Range::all());

        chunks.par_iter().for_each(|c| {
            let range_iter = reader.iter_range(&c).unwrap();

            let inter: Vec<TermFrequency> = range_iter.map(|i| i.unwrap()).collect();

            let locked = tfs.lock();
            locked.unwrap().extend(inter);
        });

        let vec = Arc::try_unwrap(tfs).unwrap();

        Ok(Snapshot {
            term_frequencies: vec.into_inner().unwrap(),
            index_statistics: stats,
            table,
        })
    }

    fn into_index(&self) -> Result<Index, Box<dyn Error>> {
        let tfpd = DashMap::<String, DashMap<u64, AtomicU64>>::new();

        let docid_to_ctid: FxHashMap<u64, u64> = self
            .term_frequencies
            .par_iter()
            .map(|tf| {
                let term_frequencies = tfpd
                    .entry(tf.term.clone())
                    .or_insert_with(|| DashMap::<u64, AtomicU64>::new());
                let val = term_frequencies
                    .entry(tf.doc_id)
                    .or_insert_with(|| AtomicU64::new(0));

                val.fetch_add(tf.frequency, Ordering::SeqCst);

                (tf.doc_id, tf.ctid)
            })
            .collect();

        let docid_to_xrange: FxHashMap<u64, OptionalRange<u64>> = self
            .term_frequencies
            .par_iter()
            .map(|tf| (tf.doc_id, tf.xrange))
            .collect();

        let docid_to_crange: FxHashMap<u64, OptionalRange<u32>> = self
            .term_frequencies
            .par_iter()
            .map(|tf| (tf.doc_id, tf.crange))
            .collect();

        Ok(Index::new(
            self.table.clone(),
            Some(tfpd),
            Some(docid_to_ctid),
            Some(docid_to_xrange),
            Some(docid_to_crange),
            Some(self.index_statistics.doc_lengths.clone()),
            Some(self.index_statistics.average_doc_length),
        ))
    }

    fn create_data_location(&self, path: &std::path::PathBuf) -> () {
        if !std::path::Path::new(path).exists() {
            std::fs::create_dir(path).expect("Expected folder to be created.");
        }
    }

    fn flush_to_disk(&self) -> Result<usize, Box<dyn Error>> {
        let mut path = data_dir();
        path.extend(vec![self.table.clone()]);
        self.create_data_location(&path);

        self.create_data_location(&path);

        let mut stats_path = path.clone();
        stats_path.extend(vec!["statistics.json"]);

        let stats_json = serde_json::to_string(&self.index_statistics)?;

        std::fs::write(stats_path, stats_json)?;

        let mut data_path = path.clone();
        data_path.extend(vec!["shards"]);

        self.create_data_location(&data_path);

        data_path.extend(vec!["data"]);

        let mut writer: ShardWriter<TermFrequency> =
            ShardWriter::new(data_path, 1024, 4096, 1 << 16)?;
        let mut sender = writer.get_sender();

        self.term_frequencies.iter().for_each(|tf| {
            sender.send(tf.clone()).unwrap();
        });

        sender.finished()?;

        Ok(writer.finish()?)
    }
}

pub struct IndexBuildState<'a> {
    pub num_inserted: usize,
    pub index_id: String,
    pub tupedesc: PgTupleDesc<'a>,
}
impl<'a> IndexBuildState<'a> {
    pub fn new(index_id: String, tupedesc: PgTupleDesc<'a>) -> Self {
        IndexBuildState {
            num_inserted: 0,
            index_id,
            tupedesc,
        }
    }
}
pub struct IndexManager<'a> {
    pub indexes: FxHashMap<String, Index>,
    pub index_build_states: FxHashMap<String, IndexBuildState<'a>>,
}

impl<'a> IndexManager<'a> {
    pub fn new() -> Self {
        IndexManager {
            indexes: FxHashMap::<String, Index>::default(),
            index_build_states: FxHashMap::<String, IndexBuildState>::default(),
        }
    }

    pub fn get_or_init_index(&mut self, table: String) -> &Index {
        let index = self.indexes.entry(table.clone()).or_insert_with(|| {
            let index =
                Index::load_or_init(table).expect("Index should have been loaded or initialized");

            index
        });

        index
    }

    pub fn get_or_init_index_mut(&mut self, table: String) -> &mut Index {
        let index = self.indexes.entry(table.clone()).or_insert_with(|| {
            let index =
                Index::load_or_init(table).expect("Index should have been loaded or initialized");

            index
        });

        index
    }

    pub fn flush_all_indexes(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.indexes.iter().for_each(|(_k, v)| {
            let _ = v.flush_to_disk();
        });

        Ok(())
    }

    // pub fn get_index(&self, table: &str) -> Option<&Index> {
    //     self.indexes.get(table)
    // }

    pub fn get_index_mut(&mut self, table: &str) -> Option<&mut Index> {
        self.indexes.get_mut(table)
    }

    pub fn poison_index(&mut self, table: String) {
        match self.indexes.get_mut(&table) {
            Some(index) => {
                index.poisoned = true;
            }

            None => {}
        }
    }

    pub fn get_or_insert_build_state(
        &mut self,
        table: String,
        tupdesc: PgTupleDesc<'static>,
    ) -> &IndexBuildState {
        self.index_build_states
            .entry(table.clone())
            .or_insert_with(|| IndexBuildState::new(table.to_string(), tupdesc))
    }

    // pub fn get_build_state(&self, table: String) -> Option<&IndexBuildState> {
    //     self.index_build_states.get(&table)
    // }

    // pub fn get_build_state_mut(&mut self, table: String) -> Option<&'a mut IndexBuildState> {
    //     self.index_build_states.get_mut(table.as_str())
    // }

    pub fn delete_index(&mut self, table: String) -> Result<(), Box<dyn Error>> {
        match self.indexes.remove(&table) {
            Some(_index) => Ok(()),

            None => Ok(()),
        }
    }
}

static mut INDEX_MANAGER: Lazy<IndexManager> = Lazy::new(|| IndexManager::new());

pub fn get_index_manager() -> &'static mut IndexManager<'static> {
    unsafe { &mut INDEX_MANAGER }
}

#[derive(Serialize, Deserialize)]
pub struct Index {
    pub term_frequencies_per_doc: DashMap<String, DashMap<u64, AtomicU64>>,
    pub docid_to_ctid: FxHashMap<u64, u64>,
    pub docid_to_xrange: FxHashMap<u64, OptionalRange<u64>>,
    pub docid_to_crange: FxHashMap<u64, OptionalRange<u32>>,
    pub normalizer: Normalizer,
    pub table: String,
    pub poisoned: bool,
    pub k: f64,
    pub b: f64,
    pub doc_lengths: FxHashMap<u64, u64>,
    pub average_doc_length: f64,
}

impl Index {
    fn new(
        table: String,
        term_frequencies_per_doc: Option<DashMap<String, DashMap<u64, AtomicU64>>>,
        docid_to_ctid: Option<FxHashMap<u64, u64>>,
        docid_to_xrange: Option<FxHashMap<u64, OptionalRange<u64>>>,
        docid_to_crange: Option<FxHashMap<u64, OptionalRange<u32>>>,
        doc_lengths: Option<FxHashMap<u64, u64>>,
        average_doc_length: Option<f64>,
    ) -> Self {
        let tfpd =
            term_frequencies_per_doc.unwrap_or(DashMap::<String, DashMap<u64, AtomicU64>>::new());
        let docid_to_ctid = docid_to_ctid.unwrap_or(FxHashMap::default());
        let docid_to_xrange = docid_to_xrange.unwrap_or(FxHashMap::default());
        let docid_to_crange = docid_to_crange.unwrap_or(FxHashMap::default());
        let doc_lengths = doc_lengths.unwrap_or(FxHashMap::default());
        let adl = average_doc_length.unwrap_or(0.0f64);

        Index {
            term_frequencies_per_doc: tfpd,
            docid_to_ctid,
            docid_to_xrange,
            docid_to_crange,
            normalizer: Normalizer::new(),
            table,
            poisoned: false,
            k: 1.6,
            b: 0.75,
            doc_lengths: doc_lengths,
            average_doc_length: adl,
        }
    }

    pub fn delete_by_xid(
        &mut self,
        xmin: Option<u64>,
        xmax: Option<u64>,
    ) -> Result<bool, Box<dyn Error>> {
        let safe_xrange = Arc::new(self.docid_to_xrange.clone());

        self.term_frequencies_per_doc.par_iter().for_each(|x| {
            x.value()
                .into_iter()
                .map(|y| {
                    let doc_id = y.key();

                    doc_id.clone()
                })
                .filter(|doc_id| {
                    let xrange = safe_xrange
                        .as_ref()
                        .get(doc_id)
                        .expect("XRange must exist by this point.");

                    if let Some(xid) = xmin.clone() {
                        xrange.max == Some(xid) || xrange.min == Some(xid)
                    } else if let Some(xid) = xmax.clone() {
                        xrange.max == Some(xid) || xrange.min == Some(xid)
                    } else {
                        false
                    }
                })
                .for_each(|ele| {
                    x.value().remove(&ele);
                });
        });

        Ok(true)
    }

    pub fn normalize(&self, doc: String) -> Vec<String> {
        let words = doc.split_whitespace().collect::<Vec<&str>>();
        self.normalizer.normalize(words)
    }

    #[allow(non_snake_case)]
    pub fn bm25(&self, query: Query, docid: u64) -> f64 {
        let query_string = query.query;

        let terms = self.normalize(query_string);

        let N = self.docid_to_ctid.len();

        terms
            .par_iter()
            .map(|term| {
                let score = match self.term_frequencies_per_doc.get(term) {
                    Some(mapping) => match mapping.get(&docid) {
                        Some(val) => {
                            let qD = val.fetch_add(0, Ordering::Relaxed) as f64;

                            let nq = mapping.len();
                            let idf = self.idf(nq, N);
                            let dl = *self
                                .doc_lengths
                                .get(&docid)
                                .expect("Expected doc to have recorded length.")
                                as f64;

                            let inter = qD * (self.k + 1.0) / qD
                                + self.k
                                    * (1.0f64 - self.b + self.b * dl / self.average_doc_length);

                            let score = idf * inter;

                            score
                        }
                        None => 0.0,
                    },
                    _ => 0.0,
                };

                score
            })
            .sum()
    }

    #[allow(non_snake_case)]
    fn idf(&self, nq: usize, N: usize) -> f64 {
        let nq = nq as f64;
        let N = N as f64;

        let inter = (N - nq + 0.5f64) / (nq + 0.5f64);

        f64::ln(inter + 1.0f64)
    }

    pub fn add_doc(
        &mut self,
        doc_id: u64,
        doc: String,
        doc_ctid: u64,
        doc_xrange: OptionalRange<u64>,
        doc_crange: OptionalRange<u32>,
    ) -> Result<(), Box<dyn Error>> {
        pgrx::info!("{}", doc);

        let words = self.normalize(doc);

        self.docid_to_xrange.insert(doc_id, doc_xrange);
        self.docid_to_crange.insert(doc_id, doc_crange);
        self.docid_to_ctid.insert(doc_id, doc_ctid);

        words.par_iter().for_each(|word| {
            let term_frequencies = self
                .term_frequencies_per_doc
                .entry(word.to_string())
                .or_insert_with(|| DashMap::<u64, AtomicU64>::new());

            if term_frequencies.contains_key(&doc_id) {
                let val = term_frequencies
                    .get(&doc_id)
                    .expect("Term frequencies should exist but were not found");
                val.fetch_add(1, Ordering::SeqCst);
            } else {
                term_frequencies.insert(doc_id, AtomicU64::new(1));
            }
        });

        self.doc_lengths.insert(doc_id, words.len() as u64);

        self.average_doc_length = self
            .doc_lengths
            .values()
            .map(|v| *v)
            .reduce(|acc, e| acc + e)
            .expect("Expected sum.") as f64
            / self.doc_lengths.len() as f64;

        Ok(())
    }

    pub fn search(&self, term: String) -> Vec<(String, u64, u64)> {
        let mut results = Vec::<(String, u64, u64)>::new();

        match self.term_frequencies_per_doc.get(&term) {
            Some(term_frequencies) => {
                pgrx::info!("Search:TFLen: {:?}", term_frequencies.len());
                term_frequencies.iter().for_each(|tf| {
                    results.push((
                        term.clone(),
                        *tf.key(),
                        tf.value().fetch_add(0, Ordering::Relaxed),
                    ));
                });
            }

            None => {}
        }

        results
    }

    pub fn scan(&self, query: String) -> Vec<(String, u64, u64)> {
        let mut vecs = Vec::<(String, u64, u64)>::new();

        let words = query.split_whitespace().collect::<Vec<&str>>();
        let words = self.normalizer.normalize(words);
        pgrx::info!("{:?}", words);

        let words = words.clone().to_owned();

        words.iter().for_each(|term| {
            let res = self.search(term.to_string());

            res.iter().for_each(|ele| {
                let (term, did, freq) = ele;
                vecs.push((term.clone(), did.clone(), freq.clone()));
            })
        });

        vecs
    }

    fn load_or_init(table: String) -> Result<Self, Box<dyn Error>> {
        match Self::load_from_disk(table.clone()) {
            Ok(index) => Ok(index),

            Err(_) => {
                let index = Index::new(table.clone(), None, None, None, None, None, None);

                Ok(index)
            }
        }
    }

    pub fn flush_to_disk(&self) -> Result<usize, Box<dyn Error>> {
        pgrx::info!("Flushing to disk!");
        let snapshot = Snapshot::from_index(self);

        snapshot.flush_to_disk()
    }

    // pub fn poison(&mut self) {
    //     self.poisoned = true;
    // }

    fn load_from_disk(table: String) -> Result<Self, Box<dyn Error>> {
        let snapshot = Snapshot::from_disk(table)?;

        snapshot.into_index()
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        if !self.poisoned {
            pgrx::info!("FLUSHING!");
            self.flush_to_disk()
                .expect("Index should have been flushed to disk");
        }
        pgrx::info!("POISONED!");
    }
}

pub struct IndexResultIterator {
    results_iter: Box<dyn Iterator<Item = (String, u64, u64)>>,
}

impl<'a> Iterator for IndexResultIterator {
    type Item = (String, u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        self.results_iter.next()
    }
}

impl From<Vec<(String, u64, u64)>> for IndexResultIterator {
    fn from(results: Vec<(String, u64, u64)>) -> Self {
        IndexResultIterator {
            results_iter: Box::new(results.into_iter()),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Lemmatizer {
    lemma_file_content: String,
    lemmas: DashMap<String, String>,
}

impl Lemmatizer {
    fn new() -> Self {
        Lemmatizer {
            lemma_file_content: include_str!("./data/lemmas.en.txt").to_string(),
            lemmas: DashMap::<String, String>::new(),
        }
    }

    fn load_lemmas(&self) -> Result<(), Box<dyn Error>> {
        let lines = self.lemma_file_content.split("\n").collect::<Vec<&str>>();

        lines.iter().skip(10).for_each(|line| {
            if *line != "" {
                let parts: Vec<&str> = line.split("->").collect();
                let lemma = parts[0].trim();
                let forms = parts[1].trim();

                forms.split(",").for_each(|form| {
                    self.lemmas
                        .insert(form.trim().to_string(), lemma.to_string());
                })
            }
        });

        Ok(())
    }

    fn lemmatize(&self, word: String) -> String {
        match self.lemmas.get(&word) {
            Some(val) => val.to_string(),
            None => word,
        }
    }
}
#[derive(Serialize, Deserialize)]
pub struct Normalizer {
    lemmatizer: Lemmatizer,
    stopwords: StopWords,
}

impl Normalizer {
    fn new() -> Self {
        let stopwords = StopWords::new();
        stopwords
            .load_basic_stop_words()
            .expect("Expected default stopwords to load.");

        let lemmatizer = Lemmatizer::new();
        lemmatizer
            .load_lemmas()
            .expect("Expected default lemmas to load.");

        Normalizer {
            lemmatizer: lemmatizer,
            stopwords: stopwords,
        }
    }

    fn normalize(&self, document: Vec<&str>) -> Vec<String> {
        document
            .par_iter()
            .map(|word| {
                let word = word.to_lowercase();
                self.lemmatizer.lemmatize(word)
            })
            .filter(|word| !self.stopwords.check_word(word))
            .collect()
    }
}
#[derive(Serialize, Deserialize)]
pub struct StopWords {
    word_set: DashSet<String>,
}

impl StopWords {
    fn new() -> Self {
        StopWords {
            word_set: DashSet::<String>::with_capacity(1000000),
        }
    }

    fn load_basic_stop_words(&self) -> Result<(), Box<dyn Error>> {
        let stop_words = include_str!("./data/stopwords.en.txt");

        let stop_words = stop_words.split("\n").collect::<Vec<&str>>();
        stop_words.iter().for_each(|word| {
            self.word_set.insert(word.to_string());
        });

        Ok(())
    }

    // fn add_word(&self, word: String) -> Result<bool, Box<dyn Error>> {
    //     Ok(self.word_set.insert(word))
    // }

    // fn remove_word(&self, word: String) -> Result<Option<String>, Box<dyn Error>> {
    //     Ok(self.word_set.remove(&word))
    // }

    fn check_word(&self, word: &str) -> bool {
        self.word_set.contains(word)
    }

    // fn filter_stop_words(&self, document: Vec<String>) -> Vec<String> {
    //     document
    //         .iter()
    //         .filter(|word| self.check_word(word))
    //         .map(|str| str.clone())
    //         .collect()
    // }
}
