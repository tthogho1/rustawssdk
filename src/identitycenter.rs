use aws_sdk_ssoadmin::Client as SsoAdminClient;
use aws_sdk_identitystore::Client as IdentityStoreClient;

/// Identity Store ID からすべてのユーザ情報を取得する。
///
/// # Arguments
/// * `client`            - IdentityStoreClient
/// * `identity_store_id` - Identity Store の ID (例: d-xxxxxxxxxx)
///
/// # Returns
/// `(user_id, user_name, display_name)` のタプル一覧。エラー時は `aws_sdk_identitystore::Error` を返す。
pub async fn list_users(
    client: &IdentityStoreClient,
    identity_store_id: &str,
) -> Result<Vec<(String, String, String)>, aws_sdk_identitystore::Error> {
    let mut users: Vec<(String, String, String)> = Vec::new();
    let mut next_token: Option<String> = None;

    loop {
        let mut req = client
            .list_users()
            .identity_store_id(identity_store_id);

        if let Some(token) = next_token {
            req = req.next_token(token);
        }

        let resp = req.send().await?;

        next_token = resp.next_token;

        for user in resp.users {
            let user_id = user.user_id.clone();
            let user_name = user.user_name.unwrap_or_default();
            let display_name = user.display_name.unwrap_or_default();
            users.push((user_id, user_name, display_name));
        }

        if next_token.is_none() {
            break;
        }
    }

    Ok(users)
}

/// Identity Center インスタンス ARN からすべての Permission Set ARN を取得する。
///
/// # Arguments
/// * `client`       - SsoAdminClient
/// * `instance_arn` - Identity Center インスタンスの ARN
///
/// # Returns
/// Permission Set の ARN 一覧。エラー時は `aws_sdk_ssoadmin::Error` を返す。
pub async fn list_permission_sets(
    client: &SsoAdminClient,
    instance_arn: &str,
) -> Result<Vec<String>, aws_sdk_ssoadmin::Error> {
    let mut permission_sets: Vec<String> = Vec::new();
    let mut next_token: Option<String> = None;

    loop {
        let mut req = client
            .list_permission_sets()
            .instance_arn(instance_arn);

        if let Some(token) = next_token {
            req = req.next_token(token);
        }

        let resp = req.send().await?;

        next_token = resp.next_token.map(|t| t.to_string());

        if let Some(arns) = resp.permission_sets {
            for arn in arns {
                permission_sets.push(arn);
            }
        }
        if next_token.is_none() {
            break;
        }
    }

    Ok(permission_sets)
}
