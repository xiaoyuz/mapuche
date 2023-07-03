use mapuche::{server, P2P_CLIENT, RAFT_CLIENT, RING_NODES};
use std::process::exit;
use std::thread;

use clap::Parser;
use local_ip_address::local_ip;
use slog::info;
use sysinfo::set_open_files_limit;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::{fs, signal};

use mapuche::config::{
    config_cluster_or_default, config_infra_or_default, config_instance_id_or_default,
    config_listen_or_default, config_max_connection, config_port_or_default,
    config_prometheus_listen_or_default, config_prometheus_port_or_default,
    config_raft_api_port_or_default, config_raft_internal_port_or_default,
    config_ring_port_or_default, config_ring_v_node_num_or_default, data_store_dir_or_default,
    set_global_config, Config, LOGGER,
};
use mapuche::hash_ring::{HashRing, NodeInfo};
use mapuche::metrics::PrometheusServer;
use mapuche::p2p::client::P2PClient;
use mapuche::p2p::server::P2PServer;
use mapuche::raft::client::RaftClient;
use mapuche::raft::start_raft_node;
use mapuche::rocks::{get_instance_id, set_instance_id};

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

    start_pmt(prom_listen, prom_port, instance_id)?;

    // If cluster enabled, init cluster connections
    if !config_cluster_or_default().is_empty() {
        start_cluster().await?;
    }

    // Start raft, but not inited
    if config_infra_or_default().need_raft() {
        start_raft()?;
    }

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("{}:{}", &listen_addr, port)).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

fn start_pmt(prom_listen: &str, prom_port: &str, instance_id: u64) -> mapuche::Result<()> {
    let pmt_server =
        PrometheusServer::new(format!("{}:{}", prom_listen, prom_port), instance_id as i64);
    thread::spawn(|| {
        Runtime::new()
            .unwrap()
            .block_on(async move { pmt_server.run().await })
    });
    info!(LOGGER, "pmt node start");
    Ok(())
}

fn start_raft() -> mapuche::Result<()> {
    let raft_api_address = format!("127.0.0.1:{}", config_raft_api_port_or_default());
    let raft_internal_address = format!("127.0.0.1:{}", config_raft_internal_port_or_default());
    let leader_addr = raft_internal_address.clone();
    thread::spawn(|| {
        Runtime::new().unwrap().block_on(async move {
            let raft_store_path = format!("{}/raft", data_store_dir_or_default());
            start_raft_node(
                get_instance_id(),
                raft_store_path,
                &raft_internal_address,
                &raft_api_address,
            )
            .await
        })
    });
    unsafe {
        let raft_client = RaftClient::new(get_instance_id(), leader_addr);
        RAFT_CLIENT.replace(raft_client);
    }
    info!(LOGGER, "raft node start");
    Ok(())
}

async fn start_cluster() -> mapuche::Result<()> {
    let p2p_server = P2PServer::new();
    let p2p_client = P2PClient::new();
    p2p_server.start().await?;
    let mut ring_nodes = vec![];
    for url in config_cluster_or_default() {
        let local_ip = local_ip()?.to_string();
        let local_port = config_ring_port_or_default();
        let local_p2p_server_url = format!("{}:{}", &local_ip, &local_port);
        if local_p2p_server_url != url {
            p2p_client.add_con(&url).await?;
        }
        // p2p_client.add_con(&url).await?;

        let node_info = NodeInfo {
            host: local_ip.clone(),
            port: local_port.parse().unwrap(),
        };
        ring_nodes.push(node_info);
    }
    let hash_ring: HashRing<NodeInfo> =
        HashRing::new(ring_nodes, config_ring_v_node_num_or_default() as usize);
    unsafe {
        P2P_CLIENT.replace(p2p_client);
        RING_NODES.replace(hash_ring);
    }
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
