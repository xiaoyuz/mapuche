use mapuche::{server, P2P_CLIENT};
use std::process::exit;

use clap::Parser;
use local_ip_address::local_ip;
use sysinfo::set_open_files_limit;
use tokio::net::TcpListener;
use tokio::{fs, signal};

use mapuche::config::{
    config_cluster_or_default, config_instance_id_or_default, config_listen_or_default,
    config_max_connection, config_port_or_default, config_prometheus_listen_or_default,
    config_prometheus_port_or_default, config_ring_port_or_default, set_global_config, Config,
};
use mapuche::metrics::PrometheusServer;
use mapuche::p2p::client::P2PClient;
use mapuche::p2p::server::P2PServer;
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

    if !set_open_files_limit((config_max_connection() * 2) as isize) {
        println!("failed to update the open files limit...");
    }

    let pmt_server = PrometheusServer::new(
        format!("{}:{}", &prom_listen, prom_port),
        instance_id as i64,
    );
    tokio::spawn(async move {
        pmt_server.run().await;
    });

    // If cluster enabled, init cluster connections
    if !config_cluster_or_default().is_empty() {
        let p2p_server = P2PServer::new();
        let p2p_client = P2PClient::new();
        p2p_server.start().await?;
        for url in config_cluster_or_default() {
            let local_ip = local_ip()?.to_string();
            let local_port = config_ring_port_or_default();
            let local_p2p_server_url = format!("{}:{}", local_ip, local_port);
            if local_p2p_server_url != url {
                p2p_client.add_con(&url).await?;
            }
            // p2p_client.add_con(&url).await?;
        }
        unsafe {
            P2P_CLIENT.replace(p2p_client);
        }
    }

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
