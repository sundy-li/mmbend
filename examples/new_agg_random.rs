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

// create table test_agg_random like test_agg engine = Random;

// insert into test_agg select * from test_agg_random limit 1888;
// insert into test_agg select * from test_agg_random limit 28888;
// insert into test_agg select * from test_agg_random limit 38888;
// insert into test_agg select * from test_agg_random limit 48888;
// insert into test_agg select * from test_agg_random limit 58888;
// insert into test_agg select * from test_agg_random limit 68888;
// insert into test_agg select * from test_agg_random limit 78888;
// insert into test_agg select * from test_agg_random limit 88888;
// insert into test_agg select * from test_agg_random limit 98888;
// insert into test_agg select * from test_agg;
// insert into test_agg select * from test_agg;
// insert into test_agg select * from test_agg;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = "databend://root:@localhost:8000/default?sslmode=disable".to_string();
    cmp::run_compare::<hits::Query>(&dsn).await
}

mod hits {
    use mmbend::{cmp, generator};

    pub struct Query {}

    impl cmp::Comparator for Query {
        fn a_prepare_sqls() -> Vec<String> {
            vec!["set enable_experimental_aggregate_hashtable = 0".to_string()]
        }
        fn b_prepare_sqls() -> Vec<String> {
            vec!["set enable_experimental_aggregate_hashtable = 1".to_string()]
        }

        fn random_sql() -> String {
            use rand::seq::IteratorRandom;
            use rand::Rng;

            let str_cols: Vec<&'static str> = vec!["b", "h", "i", "j", "f", "g"];
            let int_cols: Vec<&'static str> = vec!["a", "c", "d", "e"];

            let unsorted_cols: Vec<&'static str> = vec!["i", "h"];
            let mut gen = generator::Generator::new(
                str_cols.iter().map(|x| x.to_string()).collect(),
                int_cols.iter().map(|x| x.to_string()).collect(),
                "default.test_agg".to_string(),
            );
            gen.set_unsorted_fields(unsorted_cols.iter().map(|x| x.to_string()).collect());
            gen.generate(1..3, 1..3)
        }
    }
}
