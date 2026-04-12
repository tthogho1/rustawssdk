use aws_sdk_s3::Client as S3Client;
use aws_sdk_dynamodb::Client as DdbClient;
use aws_sdk_ecs::Client as EcsClient;
use aws_sdk_ecr::Client as EcrClient;
use aws_sdk_ec2::Client as Ec2Client;

mod s3;
mod dynamodb;
mod ecs;
mod ecr;
mod securitygroup;

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
                    list-ecs-clusters
                    list-ecs-services <cluster>
                    list-security-groups
                    show-security-group <group-id>
                    create-security-group <name> <description> [vpc-id]
                    delete-security-group <group-id>
                    list-ecr-repos
                    fallback (old behavior): <bucket> [dynamodb-table-name]",
        );

    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = S3Client::new(&config);
    let ddb_client = DdbClient::new(&config);
    let ecs_client = EcsClient::new(&config);
    let ecr_client = EcrClient::new(&config);
    let ec2_client = Ec2Client::new(&config);

    match cmd.as_str() {
        "list-buckets" => {
            let count = s3::list_s3_buckets(&s3_client).await?;
            println!("\nTotal: {} bucket(s)", count);
        }
        "list-ecs-clusters" => {
            let count = ecs::list_ecs_clusters(&ecs_client).await?;
            println!("\nTotal: {} cluster(s)", count);
        }
        "list-ecs-services" => {
            let cluster = args.next().expect("Usage: list-ecs-services <cluster>");
            let count = ecs::list_ecs_services(&ecs_client, &cluster).await?;
            println!("\nTotal: {} service(s)", count);
        }
        "list-ecs-tasks" => {
            let cluster = args.next().expect("Usage: list-ecs-tasks <cluster>");
            let count = ecs::list_ecs_tasks(&ecs_client, &cluster).await?;
            println!("\nTotal: {} task(s)", count);
        }
        "list-ecr-repos" => {
            let count = ecr::list_ecr_repositories(&ecr_client).await?;
            println!("\nTotal: {} repository(ies)", count);
        }
        "list-security-groups" => {
            let count = securitygroup::list_security_groups(&ec2_client).await?;
            println!("\nTotal: {} security group(s)", count);
        }
        "create-security-group" => {
            let name = args.next().expect("Usage: create-security-group <name> <description> [vpc-id]");
            let description = args.next().expect("Usage: create-security-group <name> <description> [vpc-id]");
            let vpc_id = args.next();
            let group_id = securitygroup::create_security_group(
                &ec2_client,
                &name,
                &description,
                vpc_id.as_deref(),
            ).await?;
            println!("Security Group ID: {}", group_id);
        }
        "delete-security-group" => {
            let group_id = args.next().expect("Usage: delete-security-group <group-id>");
            securitygroup::delete_security_group(&ec2_client, &group_id).await?;
            println!("Deleted security group: {}", group_id);
        }
        "show-security-group" => {
            let group_id = args.next().expect("Usage: show-security-group <group-id>");
            securitygroup::show_security_group_ingress(&ec2_client, &group_id).await?;
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
        "get-attrs" => {
            // Usage: get-attrs <table> <attr1,attr2,...> <key1=value1> [key2=value2 ...]
            let table = args.next().expect("Usage: get-attrs <table> <attr1,attr2,...> <key1=value1> [key2=value2 ...]");
            let attrs_csv = args.next().expect("missing attributes");
            let attrs: Vec<&str> = attrs_csv
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            let mut key_map: HashMap<String, AttributeValue> = HashMap::new();
            for kv in args {
                if let Some((k, v)) = kv.split_once('=') {
                    key_map.insert(k.to_string(), AttributeValue::S(v.to_string()));
                }
            }
            // helper: render AttributeValue into a plain string
            let av_to_str = |v: &AttributeValue| -> String {
                if let Ok(s) = v.as_s() { return s.to_string(); }
                if let Ok(n) = v.as_n() { return n.to_string(); }
                if let Ok(b) = v.as_bool() { return b.to_string(); }
                if let Ok(ss) = v.as_ss() { return ss.join(","); }
                if let Ok(ns) = v.as_ns() { return ns.join(","); }
                // fallback to debug if other types (M, L, B, etc.)
                format!("{:?}", v)
            };

            if key_map.is_empty() {
                // No key provided — scan the table and print values for each item
                let items = dynamodb::scan_projected_attributes(&ddb_client, &table, &attrs).await?;
                if items.is_empty() {
                    // print nothing (user asked for only result data)
                } else if attrs.len() == 1 {
                    let a = attrs[0];
                    for it in items {
                        let out = it.get(a).map(|v| av_to_str(v)).unwrap_or_default();
                        println!("{}", out);
                    }
                } else {
                    for it in items {
                        let row = attrs.iter()
                            .map(|a| it.get(*a).map(|v| av_to_str(v)).unwrap_or_default())
                            .collect::<Vec<_>>()
                            .join("\t");
                        println!("{}", row);
                    }
                }
            } else {
                let item = dynamodb::get_item_attributes(&ddb_client, &table, &key_map, &attrs).await?;
                if let Some(map) = item {
                    if attrs.len() == 1 {
                        let a = attrs[0];
                        let out = map.get(a).map(|v| av_to_str(v)).unwrap_or_default();
                        println!("{}", out);
                    } else {
                        let row = attrs.iter()
                            .map(|a| map.get(*a).map(|v| av_to_str(v)).unwrap_or_default())
                            .collect::<Vec<_>>()
                            .join("\t");
                        println!("{}", row);
                    }
                } else {
                    // print nothing when item not found (user asked for only result data)
                }
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
