use std::path::Path;

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::copy_object::{CopyObjectError, CopyObjectOutput};
use aws_sdk_s3::operation::delete_object::{DeleteObjectError, DeleteObjectOutput};
use aws_sdk_s3::Client;
use tokio::sync::mpsc::Sender;

pub async fn list_objects(
    client: Client,
    bucket: &str,
    folder_path: Option<&str>,
    tx: &Sender<String>,
) -> Result<u64, ()> {
    let mut count: u64 = 0;
    let prefix = folder_path.unwrap_or_default();

    let mut response = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();

    while let Some(result) = response.next().await {
        match result {
            Ok(output) => {
                for object in output.contents() {
                    match object.key() {
                        None => {}
                        Some(p) => {
                            let path = Path::new(p);
                            let filename = path.file_name().unwrap();
                            tx.send(filename.to_str().unwrap().to_string())
                                .await
                                .unwrap();
                            count += 1;
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!("{err:?}")
            }
        }
    }

    Ok(count)
}

pub async fn copy_object(
    client: &Client,
    bucket_name: &str,
    target_bucket: &str,
    object_key: &str,
    target_key: &str,
) -> Result<CopyObjectOutput, SdkError<CopyObjectError>> {
    let mut source_bucket_and_object: String = "".to_owned();
    source_bucket_and_object.push_str(bucket_name);
    source_bucket_and_object.push('/');
    source_bucket_and_object.push_str(object_key);

    client
        .copy_object()
        .copy_source(source_bucket_and_object)
        .bucket(target_bucket)
        .key(target_key)
        .send()
        .await
}

pub async fn delete_object(
    client: &Client,
    bucket_name: &str,
    object_key: &str,
) -> Result<DeleteObjectOutput, SdkError<DeleteObjectError>> {
    client
        .delete_object()
        .bucket(bucket_name)
        .key(object_key)
        .send()
        .await
}

pub async fn head_object(
    client: &Client,
    bucket_name: &str,
    target_key: &str,
) -> Result<Option<i64>, ()> {
    let response = client
        .head_object()
        .bucket(bucket_name)
        .key(target_key)
        .send()
        .await;

    match response {
        Ok(head_object_output) => Ok(head_object_output.content_length()),
        Err(_e) => Err(()),
    }
}
