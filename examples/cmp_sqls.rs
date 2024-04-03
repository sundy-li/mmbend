use mmbend::cmp;
use mmbend::result::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = "databend://root:@localhost:8000/default?sslmode=disable";
    let file_or_path = "./data/*.sql";
    cmp::run_compare(&mut sql_files::Query::from_path(file_or_path), dsn, dsn).await
}

mod sql_files {
    use mmbend::{cmp, utils::read_sqls};
    

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

    impl cmp::Comparator for Query {
        fn next_sql(&mut self) -> Option<String> {
            if self.index >= self.sqls.len() {
                return None;
            }
            self.index += 1;
            Some(self.sqls[self.index - 1].clone())
        }
    }
}
