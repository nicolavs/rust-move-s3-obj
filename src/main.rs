use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_types::region::Region;
use clap::Parser;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{broadcast, mpsc};
use tokio::task;

use s3_helper::{copy_object, delete_object, head_object, list_objects};

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
    source_bucket: String,
    target_bucket: String,
    object_key: String,
    target_key: String,
}

#[derive(Debug)] // Add Clone derive
struct TaskResult {
    _object_key: String,
    status: Status,
}

#[derive(Debug, Clone)] // Add Clone derive
enum Status {
    AlreadyExist,
    Moved,
    Error,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let destination_bucket = args
        .destination_bucket
        .unwrap_or(args.source_bucket.clone());

    is_key_valid(&args.source_path).unwrap();
    is_key_valid(&args.destination_path).unwrap();

    // Create a broadcast channel
    let (tx, _) = broadcast::channel(32);
    let (item_tx, mut item_rx): (Sender<String>, Receiver<String>) = mpsc::channel(100);
    let (result_tx, mut result_rx): (Sender<TaskResult>, Receiver<TaskResult>) = mpsc::channel(100);

    // Initialize the AWS SDK for Rust
    let config = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .region(Region::new(args.region_id))
        .load()
        .await;

    // Spawn worker tasks
    let mut handles = vec![];
    for worker_id in 0..args.num_workers {
        let mut rx = tx.subscribe();
        let s3_client = Client::new(&config);
        let tx2 = result_tx.clone();
        let h = task::spawn(async move {
            while let Ok(task) = rx.recv().await {
                process_task(&s3_client, worker_id, task, &tx2).await;
            }
        });
        handles.push(h);
    }

    // List file in S3 bucket
    let s3_client = Client::new(&config);
    list_objects(
        s3_client,
        &args.source_bucket,
        Some(&args.source_path),
        &item_tx,
    )
    .await
    .unwrap();

    // Close the channel by dropping the sender
    drop(item_tx);

    // Send tasks to workers
    while let Some(filename) = item_rx.recv().await {
        let task = Task {
            source_bucket: args.source_bucket.clone(),
            target_bucket: destination_bucket.clone(),
            object_key: make_key(&args.source_path, &filename),
            target_key: make_key(&args.destination_path, &filename),
        };
        tx.send(task).unwrap();
    }

    drop(tx);

    // Wait for a moment to allow workers to finish
    for h in handles {
        h.await.unwrap()
    }

    drop(result_tx);

    while let Some(res) = result_rx.recv().await {
        println!("{:?}", res);
    }
}

async fn process_task(client: &Client, worker_id: u8, task: Task, tx: &Sender<TaskResult>) {
    println!("Worker {} processing item {}", worker_id, &task.object_key);
    let mut result = TaskResult {
        _object_key: task.object_key.clone(),
        status: Status::Error,
    };

    let head_obj_response = head_object(client, &task.target_bucket, &task.target_key).await;
    match head_obj_response {
        Ok(head_obj) => match head_obj {
            None => {}
            Some(_) => {
                // Object exist in target path
                result.status = Status::AlreadyExist;
                tx.send(result).await.unwrap();
            }
        },
        Err(_) => {
            match copy_object(
                client,
                &task.source_bucket,
                &task.target_bucket,
                &task.object_key,
                &task.target_key,
            )
            .await
            {
                Ok(_) => {
                    result.status = Status::Moved;
                    tx.send(result).await.unwrap();

                    delete_object(client, &task.source_bucket, &task.object_key)
                        .await
                        .unwrap();
                }
                Err(_err) => {
                    tx.send(result).await.unwrap();
                }
            }
        }
    }

    println!("Worker {} completed item {}", worker_id, &task.object_key);
}

fn make_key(folder_path: &str, file_name: &str) -> String {
    let mut key = "".to_string();
    key.push_str(folder_path);
    if !key.ends_with('/') && !folder_path.is_empty() {
        key.push('/');
    }
    key.push_str(file_name);
    key
}

fn is_key_valid(object_key: &str) -> Result<(), String> {
    if object_key.starts_with('/') {
        return Err(format!("Invalid path {}", object_key));
    }

    Ok(())
}
