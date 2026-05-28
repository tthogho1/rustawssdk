use aws_sdk_iam::Client as IamClient;

/// Delete a managed IAM policy (best-effort):
/// 1) Detach the policy from any roles, users, and groups.
/// 2) Delete non-default policy versions.
/// 3) Delete the policy itself.
pub async fn delete_policy(client: &IamClient, policy_arn: &str) -> Result<(), aws_sdk_iam::Error> {
    // 1) Detach from entities that reference this policy
    match client
        .list_entities_for_policy()
        .policy_arn(policy_arn)
        .send()
        .await
    {
        Ok(resp) => {
            for r in resp.policy_roles() {
                if let Some(rn) = r.role_name() {
                    if !rn.is_empty() {
                        if let Err(e) = client
                            .detach_role_policy()
                            .role_name(rn)
                            .policy_arn(policy_arn)
                            .send()
                            .await
                        {
                            eprintln!("detach_role_policy {} -> {}", rn, e);
                        }
                    }
                }
            }

            for u in resp.policy_users() {
                if let Some(un) = u.user_name() {
                    if !un.is_empty() {
                        if let Err(e) = client
                            .detach_user_policy()
                            .user_name(un)
                            .policy_arn(policy_arn)
                            .send()
                            .await
                        {
                            eprintln!("detach_user_policy {} -> {}", un, e);
                        }
                    }
                }
            }

            for g in resp.policy_groups() {
                if let Some(gn) = g.group_name() {
                    if !gn.is_empty() {
                        if let Err(e) = client
                            .detach_group_policy()
                            .group_name(gn)
                            .policy_arn(policy_arn)
                            .send()
                            .await
                        {
                            eprintln!("detach_group_policy {} -> {}", gn, e);
                        }
                    }
                }
            }
        }
        Err(e) => eprintln!("list_entities_for_policy failed: {}", e),
    }

    // 2) Delete non-default policy versions
    match client.list_policy_versions().policy_arn(policy_arn).send().await {
        Ok(resp) => {
            for v in resp.versions() {
                let is_default = v.is_default_version();
                if let Some(vid) = v.version_id() {
                    if !vid.is_empty() && !is_default {
                        if let Err(e) = client
                            .delete_policy_version()
                            .policy_arn(policy_arn)
                            .version_id(vid)
                            .send()
                            .await
                        {
                            eprintln!("delete_policy_version {} -> {}", vid, e);
                        }
                    }
                }
            }
        }
        Err(e) => eprintln!("list_policy_versions failed: {}", e),
    }

    // 3) Delete the policy
    match client.delete_policy().policy_arn(policy_arn).send().await {
        Ok(_) => {
            println!("Deleted policy: {}", policy_arn);
            Ok(())
        }
        Err(e) => {
            eprintln!("delete_policy failed: {}", e);
            Err(e.into())
        }
    }
}
