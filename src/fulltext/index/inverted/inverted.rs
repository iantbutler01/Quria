use crate::fulltext::data_dir;
use crate::fulltext::types::*;
use art_tree::Art;
use art_tree::ByteString;
use art_tree::KeyBuilder;
use dashmap::{DashMap, DashSet};
use num_cpus;
use once_cell::sync::Lazy;
use pgrx::*;
use rayon::prelude::*;
use rudy::rudymap::RudyMap;
use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use shardio::*;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use unicode_segmentation::UnicodeSegmentation;

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub struct TermFrequency {
    term: String,
    doc_id: u64,
    frequency: u64,
    xrange: OptionalRange<u64>,
}
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexStatistics {
    doc_lengths: BTreeMap<u64, u64>,
    total_doc_length: u64,
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
            total_doc_length: index.total_doc_length,
        };

        index.term_frequencies_per_doc.iter().for_each(|term| {
            term.1.iter().for_each(|freq| {
                let tf = TermFrequency {
                    term: term.0.clone(),
                    doc_id: freq.0.clone(),
                    frequency: freq.1.fetch_add(0, Ordering::Relaxed),
                    xrange: index
                        .docid_to_xrange
                        .get(freq.0)
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
        let default_parallelism_approx = num_cpus::get();

        let tfs = Arc::new(Mutex::new(Vec::<TermFrequency>::new()));
        let chunks = reader.make_chunks(default_parallelism_approx, &shardio::Range::all());

        let manager = get_index_manager();

        manager.thread_pool.install(|| {
            chunks.par_iter().for_each(|c| {
                let range_iter = reader.iter_range(&c).unwrap();

                let inter: Vec<TermFrequency> = range_iter.map(|i| i.unwrap()).collect();

                let locked = tfs.lock();
                locked.unwrap().extend(inter);
            });
        });

        let vec = Arc::try_unwrap(tfs).unwrap();

        Ok(Snapshot {
            term_frequencies: vec.into_inner().unwrap(),
            index_statistics: stats,
            table,
        })
    }

    fn into_index(&self) -> Result<Index, Box<dyn Error>> {
        let mut docid_to_xrange = BTreeMap::<u64, OptionalRange<u64>>::default();
        let mut tfpd = Art::<ByteString, BTreeMap<u64, AtomicU64>>::new();
        self.term_frequencies.iter().for_each(|tf| {
            let entry = tfpd
                .entry(tf.term.clone())
                .or_insert_with(BTreeMap::<u64, AtomicU64>::default);

            entry.insert(tf.doc_id, tf.frequency.into());

            docid_to_xrange.insert(tf.doc_id, tf.xrange);
        });

        let doc_lengths: BTreeMap<u64, u64> = self
            .index_statistics
            .doc_lengths
            .clone()
            .into_iter()
            .collect();

        let index = Index::new(
            self.table.clone(),
            Some(tfpd),
            Some(docid_to_xrange),
            Some(doc_lengths),
            Some(self.index_statistics.total_doc_length),
        );

        pgrx::info!("FINISHED LOADING");

        Ok(index)
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
    pub thread_pool: rayon::ThreadPool,
}
impl<'a> Drop for IndexManager<'a> {
    fn drop(&mut self) {
        self.flush_all_indexes()
            .expect("Expected all indexes to be flushed.");
    }
}

impl<'a> IndexManager<'a> {
    pub fn new() -> Self {
        let num_cpus = num_cpus::get();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_cpus)
            .build()
            .unwrap();
        IndexManager {
            indexes: FxHashMap::<String, Index>::default(),
            index_build_states: FxHashMap::<String, IndexBuildState>::default(),
            thread_pool: pool,
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

    pub fn init_index_mut(&mut self, table: String) -> &mut Index {
        let index = Index::init_index(table.clone()).expect("Expected Index");

        self.indexes.insert(table.clone(), index);

        self.indexes.get_mut(&table).unwrap()
    }

    pub fn flush_all_indexes(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.indexes.iter_mut().for_each(|(_k, v)| {
            if !v.poisoned {
                let _ = v.flush_to_disk();
            }
        });

        Ok(())
    }

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

pub struct Index {
    pub term_frequencies_per_doc: Art<ByteString, RudyMap<u64, AtomicU64>>,
    pub docid_to_xrange: BTreeMap<u64, OptionalRange<u64>>,
    pub normalizer: Normalizer,
    pub table: String,
    pub poisoned: bool,
    pub k: f64,
    pub b: f64,
    pub doc_lengths: BTreeMap<u64, u64>,
    pub total_doc_length: u64,
    pub unflushed: u64,
    pub last_flush: u128,
    pub count: u64,
    pub ngram: usize,
}

impl Index {
    fn new(
        table: String,
        term_frequencies_per_doc: Option<Art<ByteString, RudyMap<u64, AtomicU64>>>,
        docid_to_xrange: Option<BTreeMap<u64, OptionalRange<u64>>>,
        doc_lengths: Option<BTreeMap<u64, u64>>,
        total_doc_length: Option<u64>,
    ) -> Self {
        let tfpd =
            term_frequencies_per_doc.unwrap_or(Art::<ByteString, RudyMap<u64, AtomicU64>>::new());
        let docid_to_xrange = docid_to_xrange.unwrap_or(BTreeMap::default());
        let doc_lengths = doc_lengths.unwrap_or(BTreeMap::default());
        let total_doc_length = total_doc_length.unwrap_or(0);

        Index {
            term_frequencies_per_doc: tfpd,
            docid_to_xrange,
            normalizer: Normalizer::new(),
            table,
            poisoned: false,
            k: 1.6,
            b: 0.75,
            doc_lengths,
            total_doc_length,
            unflushed: 0,
            last_flush: 0,
            count: 0,
            ngram: 3,
        }
    }

    fn get_time_microseconds() -> u128 {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        since_the_epoch.as_micros()
    }

    pub fn should_flush(&self) -> bool {
        let time = Index::get_time_microseconds();
        let delta = time - self.last_flush;
        let threshold = 5000000u128;

        self.unflushed > 1000 || delta > threshold
    }

    pub fn delete_by_xid(
        &mut self,
        xmin: Option<u64>,
        xmax: Option<u64>,
    ) -> Result<bool, Box<dyn Error>> {
        let safe_xrange = Arc::new(self.docid_to_xrange.clone());

        self.term_frequencies_per_doc.iter_mut().for_each(|x| {
            let mut eles = Vec::<u64>::new();
            x.1.into_iter()
                .map(|y| {
                    let doc_id = y.0;

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
                    eles.push(ele);
                });

            eles.iter().for_each(|e| {
                x.1.remove(e);
            })
        });

        Ok(true)
    }

    pub fn normalize(&self, doc: String) -> Vec<String> {
        self.normalizer.normalize(doc)
    }

    #[allow(non_snake_case)]
    pub fn bm25(&self, query: Query, docid: u64) -> f64 {
        let query_string = query.query;

        let terms = self.normalize(query_string);
        let terms = if !query.exact {
            self.tokenize(terms)
        } else {
            terms
        };

        let N = self.doc_lengths.len();

        let average_doc_length = self.total_doc_length as f64 / self.doc_lengths.len() as f64;

        terms
            .iter()
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
                                + self.k * (1.0f64 - self.b + self.b * dl / average_doc_length);

                            let score = idf * inter;

                            score
                        }
                        None => match query.operator {
                            FTSQueryOperator::AND => return 0.0,
                            _ => 0.0,
                        },
                    },
                    _ => match query.operator {
                        FTSQueryOperator::AND => return 0.0,
                        _ => 0.0,
                    },
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

    fn tokenize(&self, words: Vec<String>) -> Vec<String> {
        words
            .iter()
            .flat_map(|word| {
                if self.ngram as usize > word.len() {
                    vec![word.clone()]
                } else {
                    let mut grams = self.grapheme_windows(word, self.ngram);
                    grams.push(word.clone());

                    grams
                }
            })
            .collect()
    }

    fn grapheme_windows(&self, src: &str, win_size: usize) -> Vec<String> {
        let mut vec = Vec::<String>::new();

        let graphemes: Vec<_> = src.graphemes(true).collect();
        let len = graphemes.len();

        for i in 0..len {
            if len - i < win_size {
                vec.push(graphemes[i..].join(""));
            } else {
                vec.push(graphemes[i..i + win_size].join(""));
            }
        }

        vec
    }

    pub fn add_doc(
        &mut self,
        doc_id: u64,
        doc: String,
        doc_xrange: OptionalRange<u64>,
    ) -> Result<(), Box<dyn Error>> {
        let words = self.normalize(doc);
        let words = self.tokenize(words);

        self.docid_to_xrange.insert(doc_id, doc_xrange);

        pgrx::info!("{:?}", self.doc_lengths.len());

        words.iter().cloned().for_each(|word| {
            let term_frequencies = self
                .term_frequencies_per_doc
                .entry(word)
                .or_insert_with(|| BTreeMap::<u64, AtomicU64>::default());

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
        self.total_doc_length = self.total_doc_length + words.len() as u64;
        self.count = self.count + 1;

        Ok(())
    }

    pub fn search(&self, term: &str) -> Vec<(String, u64, u64)> {
        let mut results = Vec::<(String, u64, u64)>::new();

        match self.term_frequencies_per_doc.get(term) {
            Some(term_frequencies) => {
                term_frequencies..iter().for_each(|tf| {
                    results.push((term.to_owned(), *tf.0, tf.1.fetch_add(0, Ordering::Relaxed)));
                });
            }

            None => {}
        }

        results
    }

    pub fn scan(&self, query: Query) -> Vec<(String, u64, u64)> {
        let query_string = query.query;

        let words = self.normalizer.normalize(query_string);
        let words = if !query.exact {
            self.tokenize(words)
        } else {
            words
        };

        match query.operator {
            FTSQueryOperator::OR => {
                let mut vecs = Vec::<(String, u64, u64)>::new();
                words.iter().for_each(|term| {
                    let res = self.search(term);

                    res.iter().for_each(|ele| {
                        let (term, did, freq) = ele;
                        vecs.push((term.clone(), did.clone(), freq.clone()));
                    })
                });

                vecs
            }
            FTSQueryOperator::AND => {
                let mut masterset = FxHashSet::<u64>::default();
                let mut candidates = FxHashMap::<u64, (String, u64, u64)>::default();

                let mut ret = Vec::<(String, u64, u64)>::new();

                words.iter().for_each(|term| {
                    let mut iterset = FxHashSet::<u64>::default();

                    let res = self.search(term);

                    for ele in res.iter() {
                        iterset.insert(ele.1);
                        candidates.insert(ele.1, ele.clone());
                    }

                    if masterset.is_empty() {
                        masterset = iterset;
                    } else {
                        masterset = &masterset & &iterset;
                    }
                });

                for docid in masterset.iter() {
                    ret.push(candidates.get(docid).unwrap().to_owned());
                }

                ret
            }
        }
    }

    fn load_or_init(table: String) -> Result<Self, Box<dyn Error>> {
        match Self::load_from_disk(table.clone()) {
            Ok(index) => Ok(index),

            Err(_) => {
                let index = Index::new(table.clone(), None, None, None, None);

                Ok(index)
            }
        }
    }

    fn init_index(table: String) -> Result<Self, Box<dyn Error>> {
        let index = Index::new(table.clone(), None, None, None, None);

        Ok(index)
    }

    pub fn flush_to_disk(&mut self) -> Result<(), Box<dyn Error>> {
        pgrx::info!("Flushing to disk!");
        let snapshot = Snapshot::from_index(self);

        snapshot.flush_to_disk()?;

        self.unflushed = 0;
        self.last_flush = Index::get_time_microseconds();

        Ok(())
    }

    // pub fn poison(&mut self) {
    //     self.poisoned = true;
    // }

    fn load_from_disk(table: String) -> Result<Self, Box<dyn Error>> {
        let snapshot = Snapshot::from_disk(table)?;

        pgrx::info!("TERMS: {:?}", snapshot.term_frequencies.len());
        pgrx::info!("TERMS: {:?}", snapshot.term_frequencies.len());
        pgrx::info!("TERMS: {:?}", snapshot.term_frequencies.len());

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
            lemmas: DashMap::<String, String>::with_capacity(10000),
        }
    }

    fn load_lemmas(&self) -> Result<(), Box<dyn Error>> {
        let lines = self.lemma_file_content.split("\n").collect::<Vec<&str>>();

        lines.iter().skip(10).for_each(|line| {
            if *line != "" {
                let parts: Vec<&str> = line.split("->").collect();
                let lemma_w_count = parts[0].trim();
                let lemma_parts: Vec<&str> = lemma_w_count.split("/").collect();
                let lemma = lemma_parts[0].trim();
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
            Some(val) => val.to_owned(),
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

    fn normalize(&self, document: String) -> Vec<String> {
        document
            .unicode_words()
            .into_iter()
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
            word_set: DashSet::<String>::with_capacity(10000),
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
