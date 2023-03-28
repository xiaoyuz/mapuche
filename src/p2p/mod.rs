pub mod client;
pub mod message;
pub mod server;

#[cfg(test)]
mod tests {
    use local_ip_address::local_ip;

    #[test]
    fn test_local_ip() {
        let ip = local_ip().unwrap().to_string();
        println!("{}", ip);
    }
}
