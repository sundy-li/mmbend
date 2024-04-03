use mmbend::{perf, result::Result};

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = "databend://root:@localhost:8000/default?sslmode=disable".to_string();
    let blob_files = "./data/simple.sql";
    perf::run_perf(&mut sql_files::Query::from_path(blob_files), &dsn).await
}

mod sql_files {
    use mmbend::perf;
    use std::io::BufRead;

    pub struct Query {
        sqls: Vec<String>,
        index: usize,
    }

    impl Query {
        pub fn from_path(file_blob_pattern: &str) -> Self {
            let mut sqls = Vec::new();

            // Use the glob function to get an iterator over entries matching the pattern
            for entry in glob::glob(file_blob_pattern).expect("Failed to read glob pattern") {
                match entry {
                    Ok(path) => {
                        // If the path is a file, read lines from the file
                        if path.is_file() {
                            let file = std::fs::File::open(path).unwrap();
                            let reader = std::io::BufReader::new(file);

                            let mut sql = String::new();
                            for line in reader.lines() {
                                let line = line.unwrap(); // In real code, handle errors properly
                                if line.trim().is_empty() {
                                    continue;
                                }
                                if line.trim().ends_with(";") {
                                    sql.push('\n');
                                    sql.push_str(&line);
                                    sqls.push(sql.trim().trim_end_matches(';').to_string());
                                    sql = String::new();
                                } else {
                                    sql.push_str(&line);
                                }
                            }
                            if sql.trim().len() > 0 {
                                sqls.push(sql.trim().to_string());
                            }
                        }
                    }
                    Err(e) => panic!("Error processing entry: {}", e),
                }
            }

            Self { sqls, index: 0 }
        }
    }

    impl perf::Perf for Query {
        fn next_sql(&mut self) -> Option<String> {
            if self.index >= self.sqls.len() {
                return None;
            }
            self.index += 1;
            Some(self.sqls[self.index - 1].clone())
        }

        fn bench_times(&self) -> usize {
            3
        }

        fn result_type(&self) -> perf::ResultType {
            perf::ResultType::Best
        }
    }
}
