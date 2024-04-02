use mmbend::cmp;
use mmbend::result::Result;

/// step up script
// create table test_agg (
//     a int,
//     b string,
//     c float,
//     d decimal(24, 10),
//     e decimal(55, 10),
//     f date,
//     g timestamp,
//     h tuple(int, string),
//     i array(int),
//     j variant
// );

// create table test_agg like test_agg engine = Random;

// insert into test_agg select * from test_agg limit 1888;
// insert into test_agg select * from test_agg limit 28888;
// insert into test_agg select * from test_agg limit 38888;
// insert into test_agg select * from test_agg limit 48888;
// insert into test_agg select * from test_agg limit 58888;
// insert into test_agg select * from test_agg limit 68888;
// insert into test_agg select * from test_agg limit 78888;
// insert into test_agg select * from test_agg limit 88888;
// insert into test_agg select * from test_agg limit 98888;
// insert into test_agg select * from test_agg;
// insert into test_agg select * from test_agg;
// insert into test_agg select * from test_agg;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = "databend://root:@localhost:8000/default?sslmode=disable";
    cmp::run_compare(&mut test_agg::Query {}, dsn, dsn).await
}

mod test_agg {
    use mmbend::{cmp, generator};

    pub struct Query {}

    impl cmp::Comparator for Query {
        fn a_prepare_sqls(&self) -> Vec<String> {
            vec!["set enable_experimental_aggregate_hashtable = 0".to_string()]
        }
        fn b_prepare_sqls(&self) -> Vec<String> {
            vec!["set enable_experimental_aggregate_hashtable = 1".to_string()]
        }

        fn next_sql(&mut self) -> Option<String> {
            let str_cols: Vec<&'static str> = vec!["b", "h", "i", "j", "f", "g"];
            let int_cols: Vec<&'static str> = vec!["a", "c", "d", "e"];

            let unsorted_cols: Vec<&'static str> = vec!["i", "h"];
            let mut gen = generator::Generator::new(
                str_cols.iter().map(|x| x.to_string()).collect(),
                int_cols.iter().map(|x| x.to_string()).collect(),
                "default.test_agg".to_string(),
            );
            gen.set_unsorted_fields(unsorted_cols.iter().map(|x| x.to_string()).collect());
            Some(gen.generate(1..3, 1..3))
        }
    }
}
