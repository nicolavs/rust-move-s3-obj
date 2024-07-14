use aws_sdk_s3::Client;
use tokio::sync::mpsc::Sender;

pub async fn list_objects(
    client: &Client,
    bucket: &str,
    folder_path: Option<&str>,
    tx: Sender<String>,
) -> Result<(), ()> {
    let prefix = folder_path.unwrap_or_default();

    let mut response = client
        .list_objects_v2()
        .bucket(bucket.to_owned())
        .prefix(prefix.to_owned())
        .into_paginator()
        .send();

    while let Some(result) = response.next().await {
        match result {
            Ok(output) => {
                for object in output.contents() {
                    match object.key() {
                        None => {}
                        Some(k) => {
                            println!("{}", &k);
                            tx.send(k.to_string()).await.unwrap();
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!("{err:?}")
            }
        }
    }

    Ok(())
}
