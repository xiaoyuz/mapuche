use mapuche::server;
use std::process::exit;

use clap::Parser;
use tokio::net::TcpListener;
use tokio::{fs, signal};

use mapuche::config::{
    config_instance_id_or_default, config_listen_or_default, config_port_or_default,
    config_prometheus_listen_or_default, config_prometheus_port_or_default, set_global_config,
    Config,
};
use mapuche::metrics::PrometheusServer;
use mapuche::rocks::set_instance_id;

#[tokio::main]
pub async fn main() -> mapuche::Result<()> {
    let cli = Cli::from_args();
    let mut config: Option<Config> = None;

    if let Some(config_file_name) = cli.config {
        let config_content = fs::read_to_string(config_file_name)
            .await
            .expect("Failed to read config file");

        // deserialize toml config
        config = match toml::from_str(&config_content) {
            Ok(d) => Some(d),
            Err(e) => {
                println!("Unable to load config file {e}");
                exit(1);
            }
        };
    };

    match &config {
        Some(c) => {
            println!("{c:?}");
            set_global_config(c.clone())
        }
        None => (),
    }

    let c_port = config_port_or_default();
    let port = cli.port.as_deref().unwrap_or(&c_port);
    let c_listen = config_listen_or_default();
    let listen_addr = cli.listen_addr.as_deref().unwrap_or(&c_listen);
    let c_instance_id = config_instance_id_or_default();
    let instance_id_str = cli.instance_id.as_deref().unwrap_or(&c_instance_id);
    let c_prom_listen = config_prometheus_listen_or_default();
    let prom_listen = cli.prom_listen_addr.as_deref().unwrap_or(&c_prom_listen);
    let c_prom_port = config_prometheus_port_or_default();
    let prom_port = cli.prom_port.as_deref().unwrap_or(&c_prom_port);

    let mut instance_id: u64 = 0;
    match instance_id_str.parse::<u64>() {
        Ok(val) => {
            instance_id = val;
            set_instance_id(val);
        }
        Err(_) => set_instance_id(0),
    };

    let pmt_server = PrometheusServer::new(
        format!("{}:{}", &prom_listen, prom_port),
        instance_id as i64,
    );
    tokio::spawn(async move {
        pmt_server.run().await;
    });

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("{}:{}", &listen_addr, port)).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

#[derive(Parser, Debug)]
#[clap(name = "mapuche-server", version, author, about = "A Redis server")]
struct Cli {
    #[structopt(name = "listen", long = "--listen")]
    listen_addr: Option<String>,

    #[structopt(name = "port", long = "--port")]
    port: Option<String>,

    #[structopt(name = "instid", long = "--instid")]
    instance_id: Option<String>,

    #[structopt(name = "promlisten", long = "--promlisten")]
    prom_listen_addr: Option<String>,

    #[structopt(name = "promport", long = "--promport")]
    prom_port: Option<String>,

    #[structopt(name = "config", long = "--config")]
    config: Option<String>,
}
