use aws_sdk_ec2::Client as Ec2Client;

/// List VPCs (vpc-id, cidr, state)
pub async fn list_vpcs(client: &Ec2Client) -> Result<usize, aws_sdk_ec2::Error> {
    let resp = client.describe_vpcs().send().await?;
    let vpcs = resp.vpcs();
    if vpcs.is_empty() {
        println!("No VPCs found.");
        return Ok(0);
    }

    for v in vpcs {
        let id = v.vpc_id().unwrap_or("");
        let cidr = v.cidr_block().unwrap_or("");
        let state = v.state().map(|s| s.as_str()).unwrap_or("-");
        println!("{}\t{}\t{}", id, cidr, state);
    }

    Ok(vpcs.len())
}

/// Create a VPC with the given CIDR block. Returns the created VPC id if available.
pub async fn create_vpc(client: &Ec2Client, cidr_block: &str) -> Result<Option<String>, aws_sdk_ec2::Error> {
    let resp = client.create_vpc().cidr_block(cidr_block).send().await?;
    let vpc_id = resp.vpc().and_then(|v| v.vpc_id().map(|s| s.to_string()));
    if let Some(ref id) = vpc_id {
        println!("Created VPC: {} ({})", id, cidr_block);
    } else {
        eprintln!("Create VPC: no VPC information returned");
    }
    Ok(vpc_id)
}
