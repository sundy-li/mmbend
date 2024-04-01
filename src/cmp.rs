use crate::result::Result;
use console::Style;

use similar::{ChangeTag, TextDiff};

// Comparator is a trait that defines the behavior of a custom comparator.
pub trait Comparator {
    fn next_sql(&mut self) -> Option<String>;

    // a_prepare_sqls returns a list of SQLs that should be executed before running the query for A.
    fn a_prepare_sqls(&self) -> Vec<String> {
        vec![]
    }

    // b_prepare_sqls returns a list of SQLs that should be executed before running the query for B.
    fn b_prepare_sqls(&self) -> Vec<String> {
        vec![]
    }
}

pub async fn run_compare<C: Comparator>(c: &mut C, dsn: &str) -> Result<()> {
    use databend_driver::Client;
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();

    let mut q = 1;

    loop {
        let sql = c.next_sql();
        if sql.is_none() {
            break;
        }
        let sql = sql.unwrap();
        print!("Q{q}: {sql}\n");
        for s in c.a_prepare_sqls() {
            let _ = conn.exec(&s).await?;
        }

        let value_a = conn.query_all(&sql).await?;
        for s in c.b_prepare_sqls() {
            let _ = conn.exec(&s).await?;
        }

        let value_b = conn.query_all(&sql).await?;
        let a = value_a
            .into_iter()
            .map(|c| {
                c.into_iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<String>>()
                    .join("\t")
            })
            .collect::<Vec<String>>()
            .join("\n");

        let b = value_b
            .into_iter()
            .map(|c| {
                c.into_iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<String>>()
                    .join("\t")
            })
            .collect::<Vec<String>>()
            .join("\n");

        if a != b {
            println!(" {:?}", Style::new().on_red().apply_to("ERROR"));

            println!("Different results:");

            let diff = TextDiff::from_lines(&a, &b);
            for op in diff.ops() {
                for change in diff.iter_changes(op) {
                    let (sign, style) = match change.tag() {
                        ChangeTag::Delete => ("-", Style::new().red()),
                        ChangeTag::Insert => ("+", Style::new().green()),
                        ChangeTag::Equal => (" ", Style::new()),
                    };
                    print!("{}{}", style.apply_to(sign).bold(), style.apply_to(change));
                }
            }
            println!();
            break;
        } else {
            println!(" {}", Style::new().on_green().apply_to("PASS"));
        }
        println!(
            "-----------------------------------------------------------------------------------------------------------------------------------"
        );

        q += 1;
    }
    Ok(())
}
