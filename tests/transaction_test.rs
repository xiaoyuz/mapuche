use redis::{AsyncCommands, Client, RedisResult};
use tokio::spawn;

#[tokio::test]
async fn sadd_txn() {
    let t1 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 0..1000 {
            let res: RedisResult<String> = con.sadd("testttt", i).await;
        }
    });
    let t2 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 2000..3000 {
            let res: RedisResult<String> = con.sadd("testttt", i).await;
        }
    });
    let t3 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 4000..5000 {
            let res: RedisResult<String> = con.sadd("testttt", i).await;
        }
    });

    let t4 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 0..1000 {
            let res: RedisResult<String> = con.srem("testttt", i).await;
        }
    });
    let t5 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 2000..3000 {
            let res: RedisResult<String> = con.srem("testttt", i).await;
        }
    });
    let t6 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 2000..3000 {
            let res: RedisResult<String> = con.srem("testttt", i).await;
        }
    });


    t3.await;
    t2.await;
    t1.await;


    t4.await;
    t5.await;
    t6.await;
}