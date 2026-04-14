use aws_sdk_ec2::Client as Ec2Client;
use aws_sdk_ec2::types::{IpPermission, IpRange};

pub async fn list_security_groups(client: &Ec2Client) -> Result<usize, aws_sdk_ec2::Error> {
    let resp = client.describe_security_groups().send().await?;
    let sgs = resp.security_groups();
    if sgs.is_empty() {
        println!("No security groups found.");
        return Ok(0);
    }

    for sg in sgs {
        let id = sg.group_id().unwrap_or("");
        let name = sg.group_name().unwrap_or("");
        let desc = sg.description().unwrap_or("");
        println!("{}\t{}\t{}", id, name, desc);
    }

    Ok(sgs.len())
}

pub async fn create_security_group(
    client: &Ec2Client,
    name: &str,
    description: &str,
    vpc_id: Option<&str>,
) -> Result<String, aws_sdk_ec2::Error> {
    let mut req = client
        .create_security_group()
        .group_name(name)
        .description(description);

    if let Some(vpc) = vpc_id {
        req = req.vpc_id(vpc);
    }

    let resp = req.send().await?;
    let group_id = resp.group_id().unwrap_or("").to_string();
    println!("Created security group: {}", group_id);
    Ok(group_id)
}

pub async fn delete_security_group(
    client: &Ec2Client,
    group_id: &str,
) -> Result<(), aws_sdk_ec2::Error> {
    client.delete_security_group().group_id(group_id).send().await?;
    println!("Deleted security group: {}", group_id);
    Ok(())
}

pub async fn show_security_group_ingress(
    client: &Ec2Client,
    group_id: &str,
) -> Result<(), aws_sdk_ec2::Error> {
    let resp = client.describe_security_groups().group_ids(group_id).send().await?;
    let sgs = resp.security_groups();
    if sgs.is_empty() {
        println!("No security group found: {}", group_id);
        return Ok(());
    }

    let sg = &sgs[0];
    println!("Security Group: {} {} {}", sg.group_id().unwrap_or(""), sg.group_name().unwrap_or(""), sg.description().unwrap_or(""));

    let perms = sg.ip_permissions();
    if perms.is_empty() {
        println!("No inbound rules.");
        return Ok(());
    }

    for p in perms {
        let proto = p.ip_protocol().unwrap_or("");
        let from_port = p.from_port().map(|v| v.to_string()).unwrap_or_else(|| "-".to_string());
        let to_port = p.to_port().map(|v| v.to_string()).unwrap_or_else(|| "-".to_string());
        println!("Protocol: {}  Ports: {}-{}", proto, from_port, to_port);

        let ip_ranges = p.ip_ranges();
        if !ip_ranges.is_empty() {
            for r in ip_ranges {
                println!("  IPv4: {}  desc: {}", r.cidr_ip().unwrap_or(""), r.description().unwrap_or(""));
            }
        }

        let ipv6_ranges = p.ipv6_ranges();
        if !ipv6_ranges.is_empty() {
            for r in ipv6_ranges {
                println!("  IPv6: {}  desc: {}", r.cidr_ipv6().unwrap_or(""), r.description().unwrap_or(""));
            }
        }

        let user_pairs = p.user_id_group_pairs();
        if !user_pairs.is_empty() {
            for up in user_pairs {
                println!("  Source SG: {}  name: {}", up.group_id().unwrap_or(""), up.group_name().unwrap_or(""));
            }
        }

        let prefixes = p.prefix_list_ids();
        if !prefixes.is_empty() {
            for pr in prefixes {
                println!("  Prefix list: {}", pr.prefix_list_id().unwrap_or(""));
            }
        }
    }

    Ok(())
}

/// Authorize a simple IPv4 ingress rule on the given security group.
/// `protocol` examples: "tcp", "udp", "-1" (all).
pub async fn authorize_ingress(
    client: &Ec2Client,
    group_id: &str,
    cidr: &str,
    protocol: &str,
    from_port: i32,
    to_port: i32,
) -> Result<(), aws_sdk_ec2::Error> {
    let ip_range = IpRange::builder().cidr_ip(cidr).build();

    let perm = IpPermission::builder()
        .ip_protocol(protocol)
        .from_port(from_port)
        .to_port(to_port)
        .set_ip_ranges(Some(vec![ip_range]))
        .build();

    client
        .authorize_security_group_ingress()
        .group_id(group_id)
        .ip_permissions(perm)
        .send()
        .await?;

    println!("Authorized ingress {} {} {}-{} on {}", protocol, cidr, from_port, to_port, group_id);
    Ok(())
}
