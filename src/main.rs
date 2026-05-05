use aws_sdk_s3::Client as S3Client;
use aws_sdk_dynamodb::Client as DdbClient;
use aws_sdk_ecs::Client as EcsClient;
use aws_sdk_ecr::Client as EcrClient;
use aws_sdk_ec2::Client as Ec2Client;
use aws_sdk_batch::Client as BatchClient;
use aws_sdk_iam::Client as IamClient;

mod s3;
mod dynamodb;
mod ecs;
mod ecr;
mod securitygroup;
mod awsbatch;
mod vpc;
mod subnet;
mod iam;
mod user;

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
                        list-vpcs
                        create-vpc <cidr-block>
                    list-subnets
                    create-subnet <vpc-id> <cidr-block> [availability-zone]
                    show-security-group <group-id>
                    create-security-group <name> <description> [vpc-id]
                    delete-security-group <group-id>
                    list-ecr-repos
                    list-compute-envs
                    list-job-queues
                    list-job-defs
                    wait-compute-env <name> [timeout-secs] [poll-interval-secs]
                    delete-compute-env <name>
                    create-compute-env <name> <max-vcpus> <subnets,comma-sep> <sgs,comma-sep> [service-role-arn]
                    create-job-queue <name> <compute-env-arn-or-name> <priority>
                    register-job-def <name> <ecr-image-uri> <vcpus> <memory-mib> <exec-role-arn> <log-group> <log-region> <log-prefix>
                    submit-job <job-name> <job-queue> <job-definition>
                    describe-job <job-id>
                    get-role <role-name>
                    list-role-policies <role-name>
                    fallback (old behavior): <bucket> [dynamodb-table-name]",
        );

    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = S3Client::new(&config);
    let ddb_client = DdbClient::new(&config);
    let ecs_client = EcsClient::new(&config);
    let ecr_client = EcrClient::new(&config);
    let ec2_client = Ec2Client::new(&config);
    let batch_client = BatchClient::new(&config);
    let iam_client = IamClient::new(&config);

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
        "list-vpcs" => {
            let count = vpc::list_vpcs(&ec2_client).await?;
            println!("\nTotal: {} vpc(s)", count);
        }
        "create-vpc" => {
            let cidr = args.next().expect("Usage: create-vpc <cidr-block>");
            let maybe_id = vpc::create_vpc(&ec2_client, &cidr).await?;
            if let Some(id) = maybe_id {
                println!("VPC ID: {}", id);
            } else {
                eprintln!("Create VPC returned no VPC id");
            }
        }
        "list-subnets" => {
            let count = subnet::list_subnets(&ec2_client).await?;
            println!("\nTotal: {} subnet(s)", count);
        }
        "create-subnet" => {
            let vpc_id = args.next().expect("Usage: create-subnet <vpc-id> <cidr-block> [availability-zone]");
            let cidr = args.next().expect("missing cidr-block");
            let az = args.next();
            let maybe_id = subnet::create_subnet(&ec2_client, &vpc_id, &cidr, az.as_deref()).await?;
            if let Some(id) = maybe_id {
                println!("Subnet ID: {}", id);
            } else {
                eprintln!("Create Subnet returned no subnet id");
            }
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
        "create-bucket" => {
            let bucket = args.next().expect("Usage: create-bucket <bucket> [region]");
            let region = args.next();
            s3::create_s3_bucket(&s3_client, &bucket, region.as_deref()).await?;
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
        "list-compute-envs" => {
            let count = awsbatch::list_compute_environments(&batch_client).await?;
            println!("\nTotal: {} compute environment(s)", count);
        }
        "list-job-queues" => {
            let count = awsbatch::list_job_queues(&batch_client).await?;
            println!("\nTotal: {} job queue(s)", count);
        }
        "list-job-defs" => {
            let count = awsbatch::list_job_definitions(&batch_client).await?;
            println!("\nTotal: {} job definition(s)", count);
        }
        "delete-compute-env" => {
            let name = args.next().expect("Usage: delete-compute-env <name>");
            awsbatch::delete_compute_environment(&batch_client, &name).await?;
        }
        "wait-compute-env" => {
            let name = args.next().expect("Usage: wait-compute-env <name> [timeout-secs] [poll-interval-secs]");
            let timeout: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(120);
            let interval: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(5);
            let valid = awsbatch::wait_compute_env_valid(&batch_client, &name, timeout, interval).await?;
            if valid {
                println!("Compute environment '{}' is VALID", name);
            } else {
                eprintln!("Compute environment '{}' did not become VALID", name);
                std::process::exit(1);
            }
        }
        "create-compute-env" => {
            let name = args.next().expect("Usage: create-compute-env <name> <max-vcpus> <subnets,comma-sep> <sgs,comma-sep> [service-role-arn]");
            let max_vcpus: i32 = args.next().expect("missing max-vcpus").parse().expect("max-vcpus must be an integer");
            let subnets: Vec<String> = args.next().expect("missing subnets").split(',').map(|s| s.to_string()).collect();
            let sgs: Vec<String> = args.next().expect("missing security-group-ids").split(',').map(|s| s.to_string()).collect();
            let service_role = args.next();
            awsbatch::create_managed_fargate_compute_environment(&batch_client, &name, max_vcpus, subnets, sgs, service_role.as_deref()).await?;
        }
        "create-job-queue" => {
            let name = args.next().expect("Usage: create-job-queue <name> <compute-env> <priority>");
            let compute_env = args.next().expect("missing compute-env");
            let priority: i32 = args.next().expect("missing priority").parse().expect("priority must be an integer");
            awsbatch::create_job_queue(&batch_client, &name, &compute_env, priority).await?;
        }
        "register-job-def" => {
            let name = args.next().expect("Usage: register-job-def <name> <ecr-image-uri> <vcpus> <memory-mib> <exec-role-arn> <log-group> <log-region> <log-prefix>");
            let image = args.next().expect("missing ecr-image-uri");
            let vcpus = args.next().expect("missing vcpus");
            let memory = args.next().expect("missing memory-mib");
            let exec_role = args.next().expect("missing exec-role-arn");
            let log_group = args.next().expect("missing log-group");
            let log_region = args.next().expect("missing log-region");
            let log_prefix = args.next().expect("missing log-prefix");
            awsbatch::register_fargate_job_definition(&batch_client, &name, &image, &vcpus, &memory, &exec_role, &log_group, &log_region, &log_prefix).await?;
        }
        "submit-job" => {
            let job_name = args.next().expect("Usage: submit-job <job-name> <job-queue> <job-definition>");
            let job_queue = args.next().expect("missing job-queue");
            let job_def = args.next().expect("missing job-definition");
            awsbatch::submit_job(&batch_client, &job_name, &job_queue, &job_def).await?;
        }
        "describe-job" => {
            let job_id = args.next().expect("Usage: describe-job <job-id>");
            awsbatch::describe_job(&batch_client, &job_id).await?;
        }
        "get-role" => {
            let role_name = args.next().expect("Usage: get-role <role-name>");
            iam::get_role(&iam_client, &role_name).await?;
        }
        "delete-user" => {
            let user_name = args.next().expect("Usage: delete-user <user-name>");
            // best-effort remove resources then delete the user
            user::delete_user(&iam_client, &user_name).await?;
            println!("delete-user: requested removal of '{}'", user_name);
        }
        "delete-users" => {
            // collect remaining args as usernames
            let users: Vec<String> = args.collect();
            if users.is_empty() {
                eprintln!("Usage: delete-users <user1> [user2 ...]");
            } else {
                let mut deleted = 0usize;
                let mut failed: Vec<String> = Vec::new();
                for u in users {
                    match user::delete_user(&iam_client, &u).await {
                        Ok(_) => {
                            println!("Deleted user: {}", u);
                            deleted += 1;
                        }
                        Err(e) => {
                            eprintln!("Failed to delete {}: {}", u, e);
                            failed.push(u);
                        }
                    }
                }
                println!("Summary: deleted={}, failed={}", deleted, failed.len());
                if !failed.is_empty() {
                    eprintln!("Failed users: {:?}", failed);
                }
            }
        }
        "list-role-policies" => {
            let role_name = args.next().expect("Usage: list-role-policies <role-name>");
            let count = iam::list_attached_role_policies(&iam_client, &role_name).await?;
            println!("\nTotal: {} attached policy(ies)", count);
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
