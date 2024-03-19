mod cmp;
mod result;

use crate::result::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = "databend://root:@localhost:8000/default?sslmode=disable".to_string();
    cmp::run_compare::<hits::Query>(&dsn).await
}

mod hits {
    use crate::cmp;

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

            let str_cols: Vec<&'static str> = vec![
                "title",
                "url",
                "referer",
                "flashminor2",
                "useragentminor",
                "mobilephonemodel",
                "params",
                "searchphrase",
                "pagecharset",
                "originalurl",
                "hitcolor",
                "browserlanguage",
                "browsercountry",
                "socialnetwork",
                "socialaction",
                "socialsourcepage",
                "paramorderid",
                "paramcurrency",
                "openstatservicename",
                "openstatcampaignid",
            ];

            let int_cols: Vec<&'static str> = vec![
                "javaenable",
                "goodevent",
                "counterid",
                "clientip",
                "regionid",
                "userid",
                "counterclass",
                "os",
                "useragent",
                "isrefresh",
                "referercategoryid",
                "refererregionid",
                "urlcategoryid",
                "urlregionid",
                "resolutionwidth",
                "resolutionheight",
                "resolutiondepth",
                "flashmajor",
                "flashminor",
            ];

            let mut rng = rand::thread_rng();

            let r = 1..=2;
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

            format!(
            "SELECT {}, min({}), max({}), avg({}) FROM default.hits GROUP BY ALL ORDER BY {} LIMIT 10",
            dim_cols,
            aggrs[0],
            aggrs[1],
            aggrs[2],
            dim_cols,
        )
        }
    }
}
