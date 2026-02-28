use aws_sdk_s3::Client as S3Client;
use aws_sdk_dynamodb::Client as DdbClient;

mod s3;
mod dynamodb;

use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut args = std::env::args().skip(1);
        let cmd = args.next().expect(
                "Usage: rustawssdk <command> [...]
                Commands:
                    list-buckets
                    list-s3 <bucket>
                    describe-table <table>
                    scan-table <table>         # print all items in the table (paginated)
                    scan-table-csv <table>     # print all items as CSV (headers inferred)
                    scan-table-tsv <table>     # print all items as TSV (headers inferred)
                    list-tables
                    delete-all <table>
                    item-exists <table> <key1=value1> [key2=value2 ...]
                    set-attr <table> <attribute> <value> <key1=value1> [key2=value2 ...]
                    fallback (old behavior): <bucket> [dynamodb-table-name]",
        );

    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = S3Client::new(&config);
    let ddb_client = DdbClient::new(&config);

    match cmd.as_str() {
        "list-buckets" => {
            let count = s3::list_s3_buckets(&s3_client).await?;
            println!("\nTotal: {} bucket(s)", count);
        }
        "list-s3" => {
            let bucket = args.next().expect("Usage: list-s3 <bucket>");
            let count = s3::list_s3_objects(&s3_client, &bucket).await?;
            println!("\nTotal: {} object(s)", count);
        }
        "describe-table" => {
            let table = args.next().expect("Usage: describe-table <table>");
            dynamodb::describe_table_schema(&ddb_client, &table).await?;
        }
        "scan-table" => {
            let table = args.next().expect("Usage: scan-table <table>");
            let count = dynamodb::scan_table(&ddb_client, &table).await?;
            println!("\nTotal: {} item(s)", count);
        }
        "scan-table-csv" => {
            let table = args.next().expect("Usage: scan-table-csv <table>");
            let count = dynamodb::scan_table_csv(&ddb_client, &table).await?;
            eprintln!("\nWrote {} item(s) as CSV", count);
        }
        "scan-table-tsv" => {
            let table = args.next().expect("Usage: scan-table-tsv <table>");
            let count = dynamodb::scan_table_tsv(&ddb_client, &table).await?;
            eprintln!("\nWrote {} item(s) as TSV", count);
        }
        "list-tables" => {
            dynamodb::list_tables(&ddb_client).await?;
        }
        "delete-all" => {
            let table = args.next().expect("Usage: delete-all <table>");
            let deleted = dynamodb::delete_all_items(&ddb_client, &table).await?;
            println!("Deleted {} item(s)", deleted);
        }
        "item-exists" => {
            let table = args.next().expect("Usage: item-exists <table> <key1=value1> [key2=value2 ...]");
            let mut key_map: HashMap<String, AttributeValue> = HashMap::new();
            for kv in args {
                if let Some((k, v)) = kv.split_once('=') {
                    key_map.insert(k.to_string(), AttributeValue::S(v.to_string()));
                }
            }
            let exists = dynamodb::item_exists(&ddb_client, &table, &key_map).await?;
            println!("{}", exists);
        }
        "set-attr" => {
            // Usage: set-attr <table> <attribute> <value> <key1=value1> [key2=value2 ...]
            let table = args.next().expect("Usage: set-attr <table> <attribute> <value> <key1=value1> [key2=value2 ...]");
            let attr = args.next().expect("missing attribute");
            let val = args.next().expect("missing value");
            let mut key_map: HashMap<String, AttributeValue> = HashMap::new();
            for kv in args {
                if let Some((k, v)) = kv.split_once('=') {
                    key_map.insert(k.to_string(), AttributeValue::S(v.to_string()));
                }
            }
            if key_map.is_empty() {
                eprintln!("No key provided");
            } else {
                // infer type: bool -> Bool, number -> N, otherwise -> S
                let attribute_value = if val.eq_ignore_ascii_case("true") || val.eq_ignore_ascii_case("false") {
                    AttributeValue::Bool(val.eq_ignore_ascii_case("true"))
                } else if val.parse::<f64>().is_ok() {
                    AttributeValue::N(val.to_string())
                } else {
                    AttributeValue::S(val.to_string())
                };

                dynamodb::set_item_attribute(&ddb_client, &table, &key_map, &attr, attribute_value).await?;
                println!("OK");
            }
        }
        _ => {
            // fallback to original behavior: first argument is bucket, optional second is table
            let bucket = cmd; // cmd was actually the bucket in this fallback
            let table_name = args.next();
            let count = s3::list_s3_objects(&s3_client, &bucket).await?;
            println!("\nTotal: {} object(s)", count);
            if let Some(tbl) = table_name {
                dynamodb::describe_table_schema(&ddb_client, &tbl).await?;
            }
        }
    }

    Ok(())
}
