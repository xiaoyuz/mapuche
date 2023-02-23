use redis::{AsyncCommands, Client, RedisResult};
use tokio::spawn;

#[tokio::test]
async fn sadd_txn() {
    let t1 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 0..1000 {
            let _res: RedisResult<String> = con.sadd("testset", i).await;
        }
    });
    let t2 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 2000..3000 {
            let _res: RedisResult<String> = con.sadd("testset", i).await;
        }
    });
    let t3 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 4000..5000 {
            let _res: RedisResult<String> = con.sadd("testset", i).await;
        }
    });

    let t4 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 0..1000 {
            let _res: RedisResult<String> = con.srem("testset", i).await;
        }
    });
    let t5 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 2000..3000 {
            let _res: RedisResult<String> = con.srem("testset", i).await;
        }
    });
    let t6 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 4000..5000 {
            let _res: RedisResult<String> = con.srem("testset", i).await;
        }
    });

    t3.await.unwrap();
    t2.await.unwrap();
    t1.await.unwrap();

    t4.await.unwrap();
    t5.await.unwrap();
    t6.await.unwrap();
}

#[tokio::test]
async fn zadd_txn() {
    let t1 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 0..1000 {
            let _res: RedisResult<String> = con.zadd("testzset", i, i).await;
        }
    });
    let t2 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 2000..3000 {
            let _res: RedisResult<String> = con.zadd("testzset", i, i).await;
        }
    });
    let t3 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 4000..5000 {
            let _res: RedisResult<String> = con.zadd("testzset", i, i).await;
        }
    });

    let t4 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 0..1000 {
            let _res: RedisResult<String> = con.zrem("testzset", i).await;
        }
    });
    let t5 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 2000..3000 {
            let _res: RedisResult<String> = con.zrem("testzset", i).await;
        }
    });
    let t6 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 4000..5000 {
            let _res: RedisResult<String> = con.zrem("testzset", i).await;
        }
    });

    t3.await.unwrap();
    t2.await.unwrap();
    t1.await.unwrap();

    t4.await.unwrap();
    t5.await.unwrap();
    t6.await.unwrap();
}

#[tokio::test]
async fn list_txn() {
    let t1 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 0..100 {
            let _res: RedisResult<String> = con.lpush("testlist", i).await;
        }
    });
    let t2 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 200..300 {
            let _res: RedisResult<String> = con.lpush("testlist", i).await;
        }
    });
    let t3 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 400..500 {
            let _res: RedisResult<String> = con.lpush("testlist", i).await;
        }
    });

    // let t4 = spawn(async move {
    //     let client = Client::open("redis://127.0.0.1:6380").unwrap();
    //     let mut con = client.get_async_connection().await.unwrap();
    //     for i in 0..1000 {
    //         let _res: RedisResult<String> = con.zrem("testzset", i).await;
    //     }
    // });
    // let t5 = spawn(async move {
    //     let client = Client::open("redis://127.0.0.1:6380").unwrap();
    //     let mut con = client.get_async_connection().await.unwrap();
    //     for i in 2000..3000 {
    //         let _res: RedisResult<String> = con.zrem("testzset", i).await;
    //     }
    // });
    // let t6 = spawn(async move {
    //     let client = Client::open("redis://127.0.0.1:6380").unwrap();
    //     let mut con = client.get_async_connection().await.unwrap();
    //     for i in 4000..5000 {
    //         let _res: RedisResult<String> = con.zrem("testzset", i).await;
    //     }
    // });

    t3.await.unwrap();
    t2.await.unwrap();
    t1.await.unwrap();

    // t4.await.unwrap();
    // t5.await.unwrap();
    // t6.await.unwrap();
}

#[tokio::test]
async fn zincr_txn() {
    let t1 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 0..100 {
            let _res: RedisResult<String> = con.zincr("testzincr", "a", 1).await;
        }
    });
    let t2 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 200..300 {
            let _res: RedisResult<String> = con.zincr("testzincr", "a", 1).await;
        }
    });
    let t3 = spawn(async move {
        let client = Client::open("redis://127.0.0.1:6380").unwrap();
        let mut con = client.get_async_connection().await.unwrap();
        for i in 400..500 {
            let _res: RedisResult<String> = con.zincr("testzincr", "a", 1).await;
        }
    });

    t3.await.unwrap();
    t2.await.unwrap();
    t1.await.unwrap();
}