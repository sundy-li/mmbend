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

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = "databend://root:@localhost:8000/default?sslmode=disable".to_string();
    cmp::run_compare::<hits::Query>(&dsn).await
}

mod hits {
    use mmbend::cmp;

    pub struct Query {}

    impl cmp::Comparator for Query {
        fn prepare_a() -> Vec<String> {
            vec!["set enable_experimental_aggregate_hashtable = 0".to_string()]
        }
        fn prepare_b() -> Vec<String> {
            vec!["set enable_experimental_aggregate_hashtable = 1".to_string()]
        }

        fn random_sql() -> String {
            use rand::seq::IteratorRandom;
            use rand::Rng;

            let str_cols: Vec<&'static str> = vec!["b", "h", "i", "j", "f", "g"];
            let int_cols: Vec<&'static str> = vec!["a", "c", "d", "e"];
            let unsorted_cols: Vec<&'static str> = vec!["i", "h"];

            let mut rng = rand::thread_rng();

            let r = 2..=4;
            // pick 2-3 group fileds
            let group_num: u64 = rng.gen_range(r);
            let dims = str_cols
                .iter()
                .chain(int_cols.iter())
                .map(|c| c.to_string())
                .choose_multiple(&mut rng, group_num as _);

            // pick 3 min, max, avg, distinct fields
            let aggrs = int_cols
                .iter()
                .map(|c| c.to_string())
                .choose_multiple(&mut rng, 3);

            let dim_cols = dims.join(", ");
            let sorted_cols: Vec<String> = dims
                .iter()
                .filter(|x| !unsorted_cols.contains(&x.as_str()))
                .map(|x| x.to_string())
                .chain(["x", "y", "z"].iter().map(|x| x.to_string()))
                .collect();

            format!(
            "SELECT {}, min({}) x, max({}) y, avg({}) z FROM default.test_agg GROUP BY ALL ORDER BY {} LIMIT 10",
            dim_cols,
            aggrs[0],
            aggrs[1],
            aggrs[2],
            sorted_cols.join(" ,"),
        )
        }
    }
}
