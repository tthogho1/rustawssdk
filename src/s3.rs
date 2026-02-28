use aws_sdk_s3::Client as S3Client;

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
