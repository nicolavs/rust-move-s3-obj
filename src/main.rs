use tokio::sync::broadcast;
use tokio::task;
use std::time::Duration;

// Define a task type
#[derive(Debug, Clone)] // Add Clone derive
struct Task {
    id: u32,
    work: u32,
}

const NUM_WORKERS: u32 = 2;

#[tokio::main]
async fn main() {
    // Create a broadcast channel
    let (tx, _) = broadcast::channel(32);

    // Spawn worker tasks
    let mut handles = vec![];
    for worker_id in 0..NUM_WORKERS {
        let mut rx = tx.subscribe();
        let h = task::spawn(async move {
            while let Ok(task) = rx.recv().await {
                process_task(worker_id, task).await;
            }
        });
        handles.push(h);
    }

    // Send tasks to workers
    for i in 0..20 {
        let task = Task { id: i, work: i * 100 };
        tx.send(task).unwrap();
    }

    // Close the channel by dropping the sender
    drop(tx);

    // Wait for a moment to allow workers to finish
    for h in handles {
        h.await.unwrap()
    }
}

async fn process_task(worker_id: u32, task: Task) {
    println!("Worker {} processing task {:?}", worker_id, task);
    // Simulate some work
    tokio::time::sleep(Duration::from_millis(task.work as u64)).await;
    println!("Worker {} completed task {}", worker_id, task.id);
}