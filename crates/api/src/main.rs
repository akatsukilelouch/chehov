use axum::{
    Json,
    extract::State,
    routing::{get, post},
};
use index::{PartitionMap, fxhash::FxHashMap};
use serde::Deserialize;
use snafu::ResultExt;
use std::{path::PathBuf, sync::Arc};
use tracing_subscriber::EnvFilter;

use clap::Parser;
use tokio::net::TcpListener;

#[derive(Debug, Clone, Parser)]
struct Opts {
    #[clap(
        short = 'd',
        long = "directory",
        help = "Where partitions will be stored."
    )]
    directory: PathBuf,
}

type IndexRequest = Vec<[String; 3]>;

async fn index_handle(
    State(map): State<Arc<PartitionMap>>,
    Json(request): Json<IndexRequest>,
) -> String {
    let mut req = FxHashMap::default();

    for [partition, key, value] in request {
        let entries = if let Some(entries) = req.get_mut(&partition) {
            entries
        } else {
            req.insert(partition.clone(), FxHashMap::default());

            req.get_mut(&partition).unwrap()
        };

        let values = if let Some(values) = entries.get_mut(&key) {
            values
        } else {
            entries.insert(key.clone(), Vec::new());

            entries.get_mut(&key).unwrap()
        };

        values.push(value);
    }

    match map.index(req).await {
        Ok(_) => "ok".to_string(),
        Err(err) => err.to_string(),
    }
}

#[derive(Debug, Clone, Deserialize)]
struct SearchRequest {
    query: FxHashMap<String, Vec<String>>,
    limit: Option<usize>,
}

async fn search_handle(
    State(map): State<Arc<PartitionMap>>,
    Json(SearchRequest { query, limit }): Json<SearchRequest>,
) -> String {
    match map.search(query, limit).await {
        Ok(_) => "ok".to_string(),
        Err(err) => err.to_string(),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), snafu::Whatever> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let opts = Opts::parse();

    let map = Arc::new(
        index::PartitionMap::new(opts.directory)
            .await
            .whatever_context("failed to create the partition map")?,
    );

    let router = axum::Router::new()
        .route("/index", post(index_handle))
        .route("/search", get(search_handle))
        .with_state(map);

    let listener = TcpListener::bind("0.0.0.0:8497")
        .await
        .whatever_context("could not bind address")?;

    tracing::info!(
        "starting listening at {:?}",
        listener
            .local_addr()
            .whatever_context("no local address available")?
    );

    axum::serve(listener, router)
        .await
        .whatever_context("failed serving")?;

    Ok(())
}
