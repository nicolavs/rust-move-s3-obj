use aws_config;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_types::region::Region;
use clap::Parser;
use tokio::sync::{broadcast, mpsc};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;

use s3_helper::list_objects;

mod s3_helper;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, value_parser)]
    source_bucket: String,

    #[clap(long, value_parser, default_value = "")]
    source_path: String,

    #[clap(long, value_parser)]
    destination_path: String,

    #[clap(long, value_parser)]
    destination_bucket: Option<String>,

    #[clap(short, long, value_parser, default_value = "ap-southeast-2")]
    region_id: String,

    #[clap(short, long, value_parser, default_value_t = 1)]
    num_workers: u8,
}

// Define a task type
#[derive(Debug, Clone)] // Add Clone derive
struct Task {
    item: String,
    source_bucket: String,
    destination_path: String,
    destination_bucket: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let destination_bucket = args
        .destination_bucket
        .unwrap_or(args.source_bucket.clone());

    // Create a broadcast channel
    let (tx, _) = broadcast::channel(100);
    let (item_tx, mut item_rx): (Sender<String>, Receiver<String>) = mpsc::channel(100);

    // Spawn worker tasks
    let mut handles = vec![];
    for worker_id in 0..args.num_workers {
        let mut rx = tx.subscribe();
        let h = task::spawn(async move {
            while let Ok(task) = rx.recv().await {
                process_task(worker_id, task).await;
            }
        });
        handles.push(h);
    }

    // Initialize the AWS SDK for Rust
    let config = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .region(Region::new(args.region_id))
        .load()
        .await;
    let s3_client = Client::new(&config);

    // List item in S3 bucket
    list_objects(
        &s3_client,
        &args.source_bucket,
        Some(&args.source_path),
        item_tx.clone(),
    )
    .await
    .unwrap();

    // Close the channel by dropping the sender
    drop(item_tx);

    // Send tasks to workers
    while let Some(item) = item_rx.recv().await {
        let task = Task {
            item,
            source_bucket: args.source_bucket.clone(),
            destination_path: args.destination_path.clone(),
            destination_bucket: destination_bucket.clone(),
        };
        tx.send(task).unwrap();
    }

    // Close the channel by dropping the sender
    drop(tx);

    // Wait for a moment to allow workers to finish
    for h in handles {
        h.await.unwrap()
    }
}

async fn process_task(worker_id: u8, task: Task) {
    println!("Worker {} processing item {}", worker_id, &task.item);
    println!("Worker {} completed item {}", worker_id, &task.item);
}
