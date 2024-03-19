use crate::result::Result;
use console::Style;

use similar::{ChangeTag, TextDiff};

pub trait Comparator {
    fn random_sql() -> String;
    fn prepare_a() -> Vec<String> {
        vec![]
    }
    fn prepare_b() -> Vec<String> {
        vec![]
    }
}

pub async fn run_compare<C: Comparator>(dsn: &str) -> Result<()> {
    use databend_driver::Client;
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();

    let mut q = 1;

    loop {
        let sql = C::random_sql();
        print!("Q{q}: {sql}\n");
        for s in C::prepare_a() {
            let _ = conn.exec(&s).await?;
        }

        let value_a = conn.query_all(&sql).await?;
        for s in C::prepare_b() {
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
