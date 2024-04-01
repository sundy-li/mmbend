use std::ops::Range;

use rand::{seq::IteratorRandom, Rng};

// Generator which generates random aggregation queries
pub struct Generator {
    string_fields: Vec<String>,
    number_fields: Vec<String>,
    unsorted_fields: Vec<String>,
    fields: Vec<String>,
    table: String,
}

const NUM_AGGS: [&str; 5] = ["COUNT", "SUM", "AVG", "MIN", "MAX"];
const STR_AGGS: [&str; 3] = ["COUNT", "MIN", "MAX"];

impl Generator {
    pub fn new(string_fields: Vec<String>, number_fields: Vec<String>, table: String) -> Self {
        let fields = string_fields
            .iter()
            .chain(number_fields.iter())
            .cloned()
            .collect();

        Self {
            string_fields,
            number_fields,
            fields,
            table,
            unsorted_fields: vec![],
        }
    }

    pub fn set_unsorted_fields(&mut self, unsorted_fields: Vec<String>) {
        self.unsorted_fields = unsorted_fields;
    }

    pub fn generate(&self, groups: Range<usize>, aggrs: Range<usize>) -> String {
        let mut rng = rand::thread_rng();
        let mut query = String::new();

        let n = rng.gen_range::<usize, _>(groups.clone()).max(1);
        let group_cols = self
            .fields
            .iter()
            .filter(|x| !self.unsorted_fields.contains(x))
            .cloned()
            .choose_multiple(&mut rng, n);

        let n = rng.gen_range::<usize, _>(aggrs.clone()).max(1);
        let aggrs_cols = self.fields.iter().cloned().choose_multiple(&mut rng, n);

        query.push_str("SELECT ");
        query.push_str(group_cols.join(", ").as_str());

        for aggr in aggrs_cols {
            query.push_str(", ");

            if self.number_fields.contains(&aggr) {
                query.push_str(NUM_AGGS.iter().choose(&mut rng).unwrap());
            } else {
                assert!(self.string_fields.contains(&aggr));
                query.push_str(STR_AGGS.iter().choose(&mut rng).unwrap());
            }
            query.push_str(&format!("({})", aggr));
        }

        query.push_str(" FROM ");
        query.push_str(&self.table);

        query.push_str(" GROUP BY ALL");
        let sort_cols = group_cols.iter().cloned().collect::<Vec<String>>();
        if !sort_cols.is_empty() {
            query.push_str(" ORDER BY ");
            query.push_str(sort_cols.join(", ").as_str());
        }

        query.push_str(" LIMIT 10");
        query
    }
}
