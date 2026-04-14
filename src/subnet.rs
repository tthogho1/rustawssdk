use aws_sdk_ec2::Client as Ec2Client;

/// List subnets (subnet-id, cidr, availability-zone, vpc-id, state)
pub async fn list_subnets(client: &Ec2Client) -> Result<usize, aws_sdk_ec2::Error> {
    let resp = client.describe_subnets().send().await?;
    let subs = resp.subnets();
    if subs.is_empty() {
        println!("No subnets found.");
        return Ok(0);
    }

    for s in subs {
        let id = s.subnet_id().unwrap_or("");
        let cidr = s.cidr_block().unwrap_or("");
        let az = s.availability_zone().unwrap_or("");
        let vpc = s.vpc_id().unwrap_or("");
        let state = s.state().map(|st| st.as_str()).unwrap_or("-");
        println!("{}\t{}\t{}\t{}\t{}", id, cidr, az, vpc, state);
    }

    Ok(subs.len())
}

/// Create a subnet in the specified VPC with the given CIDR block.
/// Optionally provide an availability zone (e.g. "us-east-1a").
pub async fn create_subnet(
    client: &Ec2Client,
    vpc_id: &str,
    cidr_block: &str,
    availability_zone: Option<&str>,
) -> Result<Option<String>, aws_sdk_ec2::Error> {
    let mut req = client.create_subnet().vpc_id(vpc_id).cidr_block(cidr_block);
    if let Some(az) = availability_zone {
        req = req.availability_zone(az);
    }

    let resp = req.send().await?;
    let subnet_id = resp.subnet().and_then(|s| s.subnet_id().map(|ss| ss.to_string()));
    if let Some(ref id) = subnet_id {
        println!("Created Subnet: {} ({} in VPC {})", id, cidr_block, vpc_id);
    } else {
        eprintln!("Create Subnet: no subnet information returned");
    }
    Ok(subnet_id)
}
