use aws_sdk_iam::Client as IamClient;

/// Get an IAM role by name and print its details (like `aws iam get-role`).
pub async fn get_role(client: &IamClient, role_name: &str) -> Result<(), aws_sdk_iam::Error> {
    let resp = client.get_role().role_name(role_name).send().await?;
    if let Some(role) = resp.role() {
        println!("RoleName:    {}", role.role_name());
        println!("RoleId:      {}", role.role_id());
        println!("Arn:         {}", role.arn());
        println!("Path:        {}", role.path());
        if let Some(doc) = role.assume_role_policy_document() {
            // The document is URL-encoded; decode for readability
            let decoded = urldecode(doc);
            println!("TrustPolicy: {}", decoded);
        }
        let ts = role.create_date();
        println!("CreateDate:  {}", ts);
        if let Some(max) = role.max_session_duration() {
            println!("MaxSession:  {}s", max);
        }
    } else {
        println!("Role '{}' not found.", role_name);
    }
    Ok(())
}

/// List attached managed policies for an IAM role
/// (like `aws iam list-attached-role-policies`).
pub async fn list_attached_role_policies(
    client: &IamClient,
    role_name: &str,
) -> Result<usize, aws_sdk_iam::Error> {
    let resp = client
        .list_attached_role_policies()
        .role_name(role_name)
        .send()
        .await?;
    let policies = resp.attached_policies();
    if policies.is_empty() {
        println!("No attached policies for role '{}'.", role_name);
        return Ok(0);
    }
    for p in policies {
        println!(
            "{}\t{}",
            p.policy_name().unwrap_or("-"),
            p.policy_arn().unwrap_or("-"),
        );
    }
    Ok(policies.len())
}

/// Simple percent-decoding (AWS returns URL-encoded JSON for trust policies).
fn urldecode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next().unwrap_or(b'0');
            let lo = chars.next().unwrap_or(b'0');
            let val = hex_val(hi) * 16 + hex_val(lo);
            out.push(val as char);
        } else if b == b'+' {
            out.push(' ');
        } else {
            out.push(b as char);
        }
    }
    out
}

fn hex_val(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}
