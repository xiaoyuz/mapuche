use mapuche::{
    cmd::{Command, Zadd, Zcard, Zrange, Zrem},
    DB,
};
use tokio::spawn;

#[tokio::main]
async fn main() {
    let db = DB::open("./mapuche_store").await.unwrap();

    let db1 = db.clone();
    let t1 = spawn(async move {
        for i in 0..100 {
            let conn = db1.conn();
            let zadd_cmd = Command::Zadd(Zadd::new(
                "testz",
                vec![i.to_string()].as_slice(),
                vec![i.into()].as_slice(),
                None,
                false,
            ));
            conn.execute(zadd_cmd).await.unwrap();
            println!("zadd {:?}", i);
        }
    });

    let db2 = db.clone();
    let t2 = spawn(async move {
        for i in 100..200 {
            let conn = db2.conn();
            let zadd_cmd = Command::Zadd(Zadd::new(
                "testz",
                vec![i.to_string()].as_slice(),
                vec![i.into()].as_slice(),
                None,
                false,
            ));
            conn.execute(zadd_cmd).await.unwrap();
            println!("zadd {:?}", i);
        }
    });

    let db3 = db.clone();
    let t3 = spawn(async move {
        for i in 200..300 {
            let conn = db3.conn();
            let zadd_cmd = Command::Zadd(Zadd::new(
                "testz",
                vec![i.to_string()].as_slice(),
                vec![i.into()].as_slice(),
                None,
                false,
            ));
            conn.execute(zadd_cmd).await.unwrap();
            println!("zadd {:?}", i);
        }
    });

    let db4 = db.clone();
    let t4 = spawn(async move {
        for i in 0..100 {
            let conn = db4.conn();
            let zrem_cmd = Command::Zrem(Zrem::new("testz", vec![i.to_string()].as_slice()));
            conn.execute(zrem_cmd).await.unwrap();
            println!("zrem {:?}", i);
        }
    });

    let db5 = db.clone();
    let t5 = spawn(async move {
        for i in 100..200 {
            let conn = db5.conn();
            let zrem_cmd = Command::Zrem(Zrem::new("testz", vec![i.to_string()].as_slice()));
            conn.execute(zrem_cmd).await.unwrap();
            println!("zrem {:?}", i);
        }
    });

    let db6 = db.clone();
    let t6 = spawn(async move {
        for i in 200..300 {
            let conn = db6.conn();
            let zrem_cmd = Command::Zrem(Zrem::new("testz", vec![i.to_string()].as_slice()));
            conn.execute(zrem_cmd).await.unwrap();
            println!("zrem {:?}", i);
        }
    });

    t1.await.unwrap();
    t2.await.unwrap();
    t3.await.unwrap();
    t4.await.unwrap();
    t5.await.unwrap();
    t6.await.unwrap();

    let conn = db.conn();
    let zcard_cmd = Command::Zcard(Zcard::new("testz"));
    let frame = conn.execute(zcard_cmd).await.unwrap();
    println!("{:?}", frame);

    let zrange_cmd = Command::Zrange(Zrange::new("testz", 0, -1, false, false));
    let frame = conn.execute(zrange_cmd).await.unwrap();
    println!("{:?}", frame);
}
