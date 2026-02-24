use aws_sdk_s3::Client as S3Client;
use aws_sdk_dynamodb::Client as DdbClient;
use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut args = std::env::args().skip(1);
        let cmd = args.next().expect(
                "Usage: rustawssdk <command> [...]
Commands:
    list-s3 <bucket>
    describe-table <table>
    fallback (old behavior): <bucket> [dynamodb-table-name]",
        );

    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = S3Client::new(&config);
    let ddb_client = DdbClient::new(&config);

    match cmd.as_str() {
        "list-s3" => {
            let bucket = args.next().expect("Usage: list-s3 <bucket>");
            let count = list_s3_objects(&s3_client, &bucket).await?;
            println!("\nTotal: {} object(s)", count);
        }
        "describe-table" => {
            let table = args.next().expect("Usage: describe-table <table>");
            describe_table_schema(&ddb_client, &table).await?;
        }
        "list-tables" => {
            list_tables(&ddb_client).await?;
        }
        "delete-all" => {
            let table = args.next().expect("Usage: delete-all <table>");
            let deleted = delete_all_items(&ddb_client, &table).await?;
            println!("Deleted {} item(s)", deleted);
        }
        _ => {
            // fallback to original behavior: first argument is bucket, optional second is table
            let bucket = cmd; // cmd was actually the bucket in this fallback
            let table_name = args.next();
            let count = list_s3_objects(&s3_client, &bucket).await?;
            println!("\nTotal: {} object(s)", count);
            if let Some(tbl) = table_name {
                describe_table_schema(&ddb_client, &tbl).await?;
            }
        }
    }

    Ok(())
}

async fn describe_table_schema(
    client: &DdbClient,
    table: &str,
) -> Result<(), aws_sdk_dynamodb::Error> {
    match client.describe_table().table_name(table).send().await {
        Ok(resp) => {
            if let Some(t) = resp.table() {
                println!("\nDynamoDB table: {}", table);
                let attrs = t.attribute_definitions();
                if !attrs.is_empty() {
                    println!("AttributeDefinitions:");
                    for a in attrs {
                        let name = a.attribute_name();
                        let typ = format!("{:?}", a.attribute_type());
                        println!("  - name: {}, type: {}", name, typ);
                    }
                }
                let keys = t.key_schema();
                if !keys.is_empty() {
                    println!("KeySchema:");
                    for k in keys {
                        let name = k.attribute_name();
                        let key_type = format!("{:?}", k.key_type());
                        println!("  - name: {}, key_type: {}", name, key_type);
                    }
                }
            } else {
                println!("Table {} not found or has no metadata.", table);
            }
            Ok(())
        }
        Err(e) => {
            // Convert SdkError into the SDK Error type then inspect its message/code.
            let sdk_err: aws_sdk_dynamodb::Error = e.into();
            let msg = sdk_err.to_string();
            if msg.contains("ResourceNotFoundException") {
                println!("Table '{}' not found.", table);
                return Ok(());
            }
            Err(sdk_err)
        }
    }
}

async fn list_s3_objects(client: &S3Client, bucket: &str) -> Result<usize, aws_sdk_s3::Error> {
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

async fn list_tables(client: &DdbClient) -> Result<(), aws_sdk_dynamodb::Error> {
    let resp = client.list_tables().send().await?;
    let names = resp.table_names();
    if names.is_empty() {
        println!("No DynamoDB tables found.");
    } else {
        println!("DynamoDB tables:");
        for n in names {
            println!("  {}", n);
        }
    }
    Ok(())
}

async fn delete_all_items(client: &DdbClient, table: &str) -> Result<u64, aws_sdk_dynamodb::Error> {
    // Describe table to get key schema
    let resp = client.describe_table().table_name(table).send().await?;
    let table_desc = match resp.table() {
        Some(t) => t,
        None => {
            println!("Table '{}' not found.", table);
            return Ok(0);
        }
    };

    let key_schema = table_desc.key_schema();
    if key_schema.is_empty() {
        println!("Table '{}' has no key schema.", table);
        return Ok(0);
    }

    let key_attrs: Vec<String> = key_schema.iter().map(|k| k.attribute_name().to_string()).collect();

    let mut deleted: u64 = 0;
    let mut paginator = client
        .scan()
        .table_name(table)
        .projection_expression(&key_attrs.join(","))
        .into_paginator()
        .send();

    while let Some(page_res) = paginator.next().await {
        let page = page_res?;
        let items = page.items();
        if items.is_empty() {
            continue;
        }
        for item in items {
            let mut key_map: HashMap<String, AttributeValue> = HashMap::new();
            for k in &key_attrs {
                if let Some(v) = item.get(k) {
                    key_map.insert(k.clone(), v.clone());
                }
            }
            if key_map.len() == key_attrs.len() {
                client
                    .delete_item()
                    .table_name(table)
                    .set_key(Some(key_map))
                    .send()
                    .await?;
                deleted += 1;
            } else {
                println!("Skipping item missing full key: {:?}", item);
            }
        }
    }

    Ok(deleted)
}
