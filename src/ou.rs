use aws_sdk_organizations::Client as OrgClient;
use aws_sdk_organizations::types::AccountStatus;

/// Get the root id of the organization.
async fn get_root_id(
    client: &OrgClient,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let resp = client.list_roots().send().await?;
    resp.roots()
        .first()
        .and_then(|r| r.id())
        .map(|s| s.to_string())
        .ok_or_else(|| "No root found in organization".into())
}

/// Recursively search all OUs under `parent_id` for one whose name matches `name`.
/// Returns the OU ID if found.
#[async_recursion::async_recursion]
async fn find_ou_id_by_name(
    client: &OrgClient,
    parent_id: &str,
    name: &str,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    use aws_sdk_organizations::types::ChildType;

    let mut next_token: Option<String> = None;
    loop {
        let mut req = client
            .list_children()
            .parent_id(parent_id)
            .child_type(ChildType::OrganizationalUnit);
        if let Some(ref tok) = next_token {
            req = req.next_token(tok);
        }
        let resp = req.send().await?;
        for child in resp.children() {
            if let Some(ou_id) = child.id() {
                let detail = client
                    .describe_organizational_unit()
                    .organizational_unit_id(ou_id)
                    .send()
                    .await?;
                let ou_name = detail
                    .organizational_unit()
                    .and_then(|ou| ou.name())
                    .unwrap_or("");
                if ou_name == name {
                    return Ok(Some(ou_id.to_string()));
                }
                // recurse into this OU
                if let Some(found) = find_ou_id_by_name(client, ou_id, name).await? {
                    return Ok(Some(found));
                }
            }
        }
        next_token = resp.next_token().map(|s| s.to_string());
        if next_token.is_none() {
            break;
        }
    }
    Ok(None)
}

/// Resolve an OU identifier that is either an OU ID (starts with "ou-") or a name.
/// Returns the OU ID, or an error if the name cannot be found.
pub async fn resolve_ou_id(
    client: &OrgClient,
    ou_id_or_name: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if ou_id_or_name.starts_with("ou-") {
        return Ok(ou_id_or_name.to_string());
    }
    let root_id = get_root_id(client).await?;
    match find_ou_id_by_name(client, &root_id, ou_id_or_name).await? {
        Some(id) => Ok(id),
        None => Err(format!("No OU found with name '{}'", ou_id_or_name).into()),
    }
}

/// Recursively list all OUs under the given parent, printing with indentation.
#[async_recursion::async_recursion]
async fn list_ous_under(
    client: &OrgClient,
    parent_id: &str,
    depth: usize,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    use aws_sdk_organizations::types::ChildType;

    let indent = "  ".repeat(depth);
    let mut total = 0usize;
    let mut next_token: Option<String> = None;

    loop {
        let mut req = client
            .list_children()
            .parent_id(parent_id)
            .child_type(ChildType::OrganizationalUnit);
        if let Some(ref tok) = next_token {
            req = req.next_token(tok);
        }
        let resp = req.send().await?;
        for child in resp.children() {
            if let Some(ou_id) = child.id() {
                // Fetch OU details for the name
                let detail = client
                    .describe_organizational_unit()
                    .organizational_unit_id(ou_id)
                    .send()
                    .await?;
                let name = detail
                    .organizational_unit()
                    .and_then(|ou| ou.name())
                    .unwrap_or("-");
                println!("{}{}\t{}", indent, ou_id, name);
                total += 1;
                total += list_ous_under(client, ou_id, depth + 1).await?;
            }
        }
        next_token = resp.next_token().map(|s| s.to_string());
        if next_token.is_none() {
            break;
        }
    }

    Ok(total)
}

/// List all OUs in the organization, starting from the root.
/// Returns the total number of OUs found.
pub async fn list_ous(
    client: &OrgClient,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let root_id = get_root_id(client).await?;
    println!("Root: {}", root_id);
    let count = list_ous_under(client, &root_id, 1).await?;
    Ok(count)
}

/// List accounts directly under the given parent (OU id or root id).
async fn list_accounts_for_parent(
    client: &OrgClient,
    parent_id: &str,
) -> Result<Vec<String>, aws_sdk_organizations::Error> {
    let mut account_ids = Vec::new();
    let mut next_token: Option<String> = None;

    loop {
        let mut req = client.list_accounts_for_parent().parent_id(parent_id);
        if let Some(ref tok) = next_token {
            req = req.next_token(tok);
        }
        let resp = req.send().await?;
        for acct in resp.accounts() {
            if let Some(id) = acct.id() {
                account_ids.push(id.to_string());
            }
        }
        next_token = resp.next_token().map(|s| s.to_string());
        if next_token.is_none() {
            break;
        }
    }

    Ok(account_ids)
}

/// List child OUs directly under the given parent (OU id or root id).
async fn list_child_ous(
    client: &OrgClient,
    parent_id: &str,
) -> Result<Vec<String>, aws_sdk_organizations::Error> {
    use aws_sdk_organizations::types::ChildType;

    let mut ou_ids = Vec::new();
    let mut next_token: Option<String> = None;

    loop {
        let mut req = client
            .list_children()
            .parent_id(parent_id)
            .child_type(ChildType::OrganizationalUnit);
        if let Some(ref tok) = next_token {
            req = req.next_token(tok);
        }
        let resp = req.send().await?;
        for child in resp.children() {
            if let Some(id) = child.id() {
                ou_ids.push(id.to_string());
            }
        }
        next_token = resp.next_token().map(|s| s.to_string());
        if next_token.is_none() {
            break;
        }
    }

    Ok(ou_ids)
}

/// Wait for an account to reach SUSPENDED status (closed).
/// Polls up to `max_attempts` times with `sleep_secs` seconds between polls.
async fn wait_account_suspended(
    client: &OrgClient,
    account_id: &str,
    max_attempts: u32,
    sleep_secs: u64,
) -> Result<bool, aws_sdk_organizations::Error> {
    for attempt in 0..max_attempts {
        let resp = client.describe_account().account_id(account_id).send().await?;
        if let Some(acct) = resp.account() {
            let status = acct.status();
            println!(
                "  [wait] account {} status = {:?} (attempt {}/{})",
                account_id,
                status,
                attempt + 1,
                max_attempts
            );
            if status == Some(&AccountStatus::Suspended) {
                return Ok(true);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
    }
    Ok(false)
}

/// Recursively close all accounts and delete all child OUs under the given OU,
/// then delete the OU itself.
///
/// `ou_id` – the OU to delete (e.g. "ou-xxxx-xxxxxxxx")
/// `dry_run` – if true, only print what would be done without making any changes
#[async_recursion::async_recursion]
pub async fn delete_ou_recursive(
    client: &OrgClient,
    ou_id: &str,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Processing OU: {}", ou_id);

    // 1. Recurse into child OUs first (depth-first)
    let child_ous = list_child_ous(client, ou_id).await?;
    for child_ou_id in &child_ous {
        delete_ou_recursive(client, child_ou_id, dry_run).await?;
    }

    // 2. Close all accounts directly under this OU
    let account_ids = list_accounts_for_parent(client, ou_id).await?;
    for account_id in &account_ids {
        println!("  Closing account: {}", account_id);
        if !dry_run {
            client.close_account().account_id(account_id).send().await?;
            // Wait for the account to reach SUSPENDED state before deleting the OU
            let ok = wait_account_suspended(client, account_id, 60, 10).await?;
            if !ok {
                eprintln!(
                    "  WARNING: account {} did not reach SUSPENDED within timeout; proceeding anyway.",
                    account_id
                );
            }
        }
    }

    // 3. Delete this OU (must be empty at this point)
    println!("  Deleting OU: {}", ou_id);
    if !dry_run {
        client
            .delete_organizational_unit()
            .organizational_unit_id(ou_id)
            .send()
            .await?;
        println!("  Deleted OU: {}", ou_id);
    } else {
        println!("  [dry-run] would delete OU: {}", ou_id);
    }

    Ok(())
}
