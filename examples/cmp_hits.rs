use mmbend::cmp;
use mmbend::result::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = "databend://root:@localhost:8000/default?sslmode=disable".to_string();
    cmp::run_compare(&mut hits::Query {}, &dsn).await
}

mod hits {
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

            let gen = generator::Generator::new(
                str_cols.iter().map(|x| x.to_string()).collect(),
                int_cols.iter().map(|x| x.to_string()).collect(),
                "default.hits".to_string(),
            );

            Some(gen.generate(1..3, 2..4))
        }
    }
}
