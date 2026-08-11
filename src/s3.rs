use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::primitives::ByteStream;
use std::path::Path;

pub async fn list_s3_objects(client: &S3Client, bucket: &str) -> Result<usize, aws_sdk_s3::Error> {
    let mut paginator = client
        .list_objects_v2()
        .bucket(bucket)
        .into_paginator()
        .send();

    let mut count = 0usize;
    while let Some(result) = paginator.next().await {
        let page = result?;
        let contents = page.contents();
        if !contents.is_empty() {
            for object in contents {
                match object.key() {
                    Some(k) => println!("{}", k),
                    None => println!("(no key)"),
                }
                count += 1;
            }
        }
    }

    Ok(count)
}

/// Count objects under a bucket and optional prefix.
///
/// Example:
/// let n = count_objects_in_prefix(&client, "my-bucket", Some("path/to/")).await?;
pub async fn count_objects_in_prefix(
    client: &S3Client,
    bucket: &str,
    prefix: Option<&str>,
) -> Result<usize, aws_sdk_s3::Error> {
    let mut req = client.list_objects_v2().bucket(bucket);
    if let Some(p) = prefix {
        if !p.is_empty() {
            req = req.prefix(p);
        }
    }

    let mut paginator = req.into_paginator().send();
    let mut count: usize = 0;
    while let Some(page_result) = paginator.next().await {
        let page = page_result?;
        let contents = page.contents();
        if !contents.is_empty() {
            count += contents.len();
        }
    }

    Ok(count)
}

pub async fn list_s3_buckets(client: &S3Client) -> Result<usize, aws_sdk_s3::Error> {
    let resp = client.list_buckets().send().await?;
    let buckets = resp.buckets();
    if buckets.is_empty() {
        println!("No S3 buckets found.");
        return Ok(0);
    }
    for b in buckets {
        if let Some(name) = b.name() {
            println!("{}", name);
        } else {
            println!("(no name)");
        }
    }
    Ok(buckets.len())
}

/// Create an S3 bucket. If `region` is `None` the client's configured region is used.
/// For most regions, calling CreateBucket without a `CreateBucketConfiguration`
/// is sufficient; some regions require a LocationConstraint. Pass `Some(region)`
/// to include a LocationConstraint explicitly.
pub async fn create_s3_bucket(
    client: &S3Client,
    bucket: &str,
    region: Option<&str>,
) -> Result<(), aws_sdk_s3::Error> {
    let mut req = client.create_bucket().bucket(bucket);
    if let Some(r) = region {
        if !r.is_empty() {
            // set LocationConstraint when provided (best-effort)
            req = req.create_bucket_configuration(
                aws_sdk_s3::types::CreateBucketConfiguration::builder()
                    .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::from(r))
                    .build(),
            );
        }
    }

    req.send().await?;
    println!(
        "Created bucket '{}'{}",
        bucket,
        match region {
            Some(r) => format!(" in region {}", r),
            None => "".to_string(),
        }
    );
    Ok(())
}

/// Upload a local file to an S3 bucket.
///
/// If `key` is `None`, the object key defaults to the local file's file name
/// (i.e. the local file name and the S3 object name will match).
///
/// Example:
/// put_s3_object(&client, "audio4input", Some("NQsldKLWj1M/NQsldKLWj1M.mp4"), "/path/to/file.mp4").await?;
/// put_s3_object(&client, "audio4input", None, "/path/to/file.mp4").await?; // key == "file.mp4"
pub async fn put_s3_object(
    client: &S3Client,
    bucket: &str,
    key: Option<&str>,
    local_path: &str,
) -> Result<(), aws_sdk_s3::Error> {
    let path = Path::new(local_path);
    let resolved_key: String = match key {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| local_path.to_string()),
    };

    let body = ByteStream::from_path(path)
        .await
        .unwrap_or_else(|e| panic!("Failed to read local file '{}': {}", local_path, e));

    client
        .put_object()
        .bucket(bucket)
        .key(&resolved_key)
        .body(body)
        .send()
        .await?;

    println!("Uploaded '{}' to '{}/{}'", local_path, bucket, resolved_key);
    Ok(())
}

/// Delete a single object from an S3 bucket.
///
/// Example:
/// delete_s3_object(&client, "audio4input", "NQsldKLWj1M/NQsldKLWj1M.mp4").await?;
pub async fn delete_s3_object(
    client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<(), aws_sdk_s3::Error> {
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    println!("Deleted object '{}/{}'", bucket, key);
    Ok(())
}

/// Empty (delete all objects from) an S3 bucket. This does not handle
/// versioned objects — it deletes the current object versions only.
pub async fn empty_s3_bucket(
    client: &S3Client,
    bucket: &str,
    prefix: Option<&str>,
) -> Result<(), aws_sdk_s3::Error> {
    let mut req = client.list_objects_v2().bucket(bucket);
    if let Some(p) = prefix {
        if !p.is_empty() {
            req = req.prefix(p);
        }
    }

    let mut paginator = req.into_paginator().send();

    while let Some(page) = paginator.next().await {
        let page = page?;
        let contents = page.contents();
        if contents.is_empty() {
            continue;
        }

        let objects: Vec<ObjectIdentifier> = contents
            .iter()
            .filter_map(|o| o.key().map(|k| ObjectIdentifier::builder().key(k).build().unwrap()))
            .collect();

        if !objects.is_empty() {
            let delete = Delete::builder().set_objects(Some(objects)).build().unwrap();
            client
                .delete_objects()
                .bucket(bucket)
                .delete(delete)
                .send()
                .await?;
        }
    }

    println!("Emptied bucket '{}'", bucket);
    Ok(())
}
