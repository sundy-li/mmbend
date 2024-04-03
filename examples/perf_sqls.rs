use mmbend::{perf, result::Result};

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = "databend://root:@localhost:8000/default?sslmode=disable".to_string();
    let blob_files = "./data/simple.sql";
    perf::run_perf(&mut sql_files::Query::from_path(blob_files), &dsn).await
}

mod sql_files {
    use mmbend::{perf, utils::read_sqls};

    pub struct Query {
        sqls: Vec<String>,
        index: usize,
    }

    impl Query {
        pub fn from_path(file_blob_pattern: &str) -> Self {
            Self {
                sqls: read_sqls(file_blob_pattern),
                index: 0,
            }
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
