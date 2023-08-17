use mapuche::{
    cmd::{Command, Get, Lrange, Lrem, Push, Set},
    DB,
};

#[tokio::test]
async fn db_conn() {
    let db = DB::open("./mapuche_store").await.unwrap();
    let conn = db.conn();
    let set_cmd = Command::Set(Set::new("test1", "value", None, None));
    let frame = conn.execute(set_cmd).await.unwrap();
    println!("{:?}", frame);
    let get_cmd = Command::Get(Get::new("test1"));
    let frame = conn.execute(get_cmd).await.unwrap();
    println!("{:?}", frame);

    let push_cmd = Command::Lpush(Push::new("testlist", vec!["aaa", "bbb"].as_slice()));
    let frame = conn.execute(push_cmd).await.unwrap();
    println!("{:?}", frame);

    let lrange_cmd = Command::Lrange(Lrange::new("testlist", 0, -1));
    let frame = conn.execute(lrange_cmd).await.unwrap();
    println!("{:?}", frame);

    let lrem_cmd = Command::Lrem(Lrem::new("testlist", 1, "aaa"));
    let frame = conn.execute(lrem_cmd).await.unwrap();
    println!("{:?}", frame);

    let lrange_cmd = Command::Lrange(Lrange::new("testlist", 0, -1));
    let frame = conn.execute(lrange_cmd).await.unwrap();
    println!("{:?}", frame);
}
