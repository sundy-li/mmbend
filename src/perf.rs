use crate::result::Result;
use console::Style;
use databend_driver::RowWithStats;
use tokio_stream::StreamExt;

// Perf is a trait that defines the behavior of a custom comparator.
pub trait Perf {
    fn next_sql(&mut self) -> Option<String>;

    // prepare_sqls returns a list of SQLs that should be executed before running each query
    fn prepare_sqls(&self) -> Vec<String> {
        vec![]
    }

    fn bench_times(&self) -> usize {
        3
    }

    fn result_type(&self) -> ResultType {
        ResultType::Best
    }
}

/// How to caculate the result
#[derive(Debug)]
pub enum ResultType {
    Best,
    Median,
    Avg,
}

pub async fn run_perf<P: Perf>(p: &mut P, dsn: &str) -> Result<()> {
    use databend_driver::Client;
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();
    let mut q = 1;

    loop {
        let sql = p.next_sql();
        if sql.is_none() {
            break;
        }
        let sql = sql.unwrap();
        for s in p.prepare_sqls() {
            let _ = conn.exec(&s).await?;
        }
        let mut result = vec![0; p.bench_times()];
        for i in 0..p.bench_times() {
            let mut s = conn.query_iter_ext(&sql).await?;
            while let Some(s) = s.next().await {
                match s {
                    Ok(RowWithStats::Stats(stats)) => {
                        result[i] = stats.running_time_ms as u64;
                    }
                    _ => {}
                }
            }
            // assert!(result[i] > 0, "result[i] should be greater than 0");
        }
        result.sort();
        match p.result_type() {
            ResultType::Best => {
                let best = result.iter().min().unwrap();
                println!(
                    "{}: {}",
                    Style::new().bold().apply_to(format!("Query #{}", q)),
                    Style::new().green().apply_to(format!("{:?}ms", best))
                );
            }
            ResultType::Median => {
                let median = result[result.len() / 2];
                println!(
                    "{}: {}",
                    Style::new().bold().apply_to(format!("Query #{}", q)),
                    Style::new().green().apply_to(format!("{:?}ms", median))
                );
            }
            ResultType::Avg => {
                let avg = result.iter().sum::<u64>() / result.len() as u64;
                println!(
                    "{}: {}",
                    Style::new().bold().apply_to(format!("Query #{}", q)),
                    Style::new().green().apply_to(format!("{:?}ms", avg))
                );
            }
        }
        q += 1;
    }
    Ok(())
}
