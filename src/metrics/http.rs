use crate::config::LOGGER;
use crate::metrics::{CURRENT_CONNECTION_COUNTER, INSTANCE_ID_GAUGER, REQUEST_COUNTER};
use actix_web::{get, App, HttpResponse, HttpServer, Responder};

use prometheus::{Encoder, TextEncoder};
use slog::info;

pub struct PrometheusServer {
    listen_addr: String,
}

impl PrometheusServer {
    pub fn new(listen_addr: String, instance_id: i64) -> PrometheusServer {
        INSTANCE_ID_GAUGER.set(instance_id);
        REQUEST_COUNTER.get();
        CURRENT_CONNECTION_COUNTER.get();

        PrometheusServer { listen_addr }
    }

    pub async fn run(&self) -> std::io::Result<()> {
        info!(LOGGER, "Prometheus Server Listen on: {}", &self.listen_addr);

        // Start the actix-web server.
        let server = HttpServer::new(move || App::new().service(metric));

        let x = server.bind(&self.listen_addr)?;
        x.run().await
    }
}

#[get("/")]
async fn metric() -> impl Responder {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();
    HttpResponse::Ok().body(buffer)
}
