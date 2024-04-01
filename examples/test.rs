use databend_driver::Client;

#[tokio::test]
async fn test_rollback() {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or("databend://root@127.0.0.1:8000");
    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();

    conn.exec("CREATE OR REPLACE TABLE t(c int);")
        .await
        .unwrap();
    //     conn.begin().await.unwrap();

    conn.exec("INSERT INTO t VALUES(1);").await.unwrap();
    let row = conn.query_row("SELECT * FROM t").await.unwrap();
    let row = row.unwrap();
    let (val,): (i32,) = row.try_into().unwrap();
    assert_eq!(val, 1);

    conn.rollback().await.unwrap();

    let client = Client::new(dsn.to_string());
    let conn = client.get_conn().await.unwrap();
    let row = conn.query_row("SELECT * FROM t").await.unwrap(); // occur error Query failed. query id has finished xxx
    assert!(row.is_none());
}
