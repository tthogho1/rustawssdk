use aws_sdk_iam::Client as IamClient;

/// Delete an IAM user and best-effort remove common attached resources.
///
/// Steps performed (best-effort, errors are logged but do not abort the
/// flow until the final `delete_user`):
/// 1. Verify the user exists.
/// 2. Delete login profile (console password).
/// 3. Delete access keys.
/// 4. Delete signing certificates.
/// 5. Deactivate MFA devices and delete virtual MFA devices.
/// 6. Delete SSH public keys.
/// 7. Delete service-specific credentials.
/// 8. Delete inline user policies.
/// 9. Detach managed user policies.
/// 10. Remove the user from all groups.
/// 11. Delete the user.
pub async fn delete_user(client: &IamClient, user_name: &str) -> Result<(), aws_sdk_iam::Error> {
    // Existence check
    if let Err(e) = client.get_user().user_name(user_name).send().await {
        eprintln!("get_user failed (user may not exist): {}", e);
        return Err(e.into());
    }

    // 1) Delete login profile (console password)
    if let Err(e) = client
        .delete_login_profile()
        .user_name(user_name)
        .send()
        .await
    {
        eprintln!("delete_login_profile failed (may be missing): {}", e);
    }

    // 2) Delete access keys
    match client
        .list_access_keys()
        .user_name(user_name)
        .send()
        .await
    {
        Ok(resp) => {
            for k in resp.access_key_metadata() {
                if let Some(key_id) = k.access_key_id() {
                    if let Err(e) = client
                        .delete_access_key()
                        .user_name(user_name)
                        .access_key_id(key_id)
                        .send()
                        .await
                    {
                        eprintln!("delete_access_key {} -> {}", key_id, e);
                    }
                }
            }
        }
        Err(e) => eprintln!("list_access_keys failed: {}", e),
    }

    // 3) Delete signing certificates
    match client
        .list_signing_certificates()
        .user_name(user_name)
        .send()
        .await
    {
        Ok(resp) => {
            for c in resp.certificates() {
                let cid = c.certificate_id();
                if !cid.is_empty() {
                    if let Err(e) = client
                        .delete_signing_certificate()
                        .user_name(user_name)
                        .certificate_id(cid)
                        .send()
                        .await
                    {
                        eprintln!("delete_signing_certificate {} -> {}", cid, e);
                    }
                }
            }
        }
        Err(e) => eprintln!("list_signing_certificates failed: {}", e),
    }

    // 4) MFA devices: deactivate then delete virtual MFA device where applicable
    match client.list_mfa_devices().user_name(user_name).send().await {
        Ok(resp) => {
            for m in resp.mfa_devices() {
                let serial = m.serial_number();
                if !serial.is_empty() {
                    if let Err(e) = client
                        .deactivate_mfa_device()
                        .user_name(user_name)
                        .serial_number(serial)
                        .send()
                        .await
                    {
                        eprintln!("deactivate_mfa_device {} -> {}", serial, e);
                    }
                    // Attempt to delete virtual MFA device (best-effort; only valid for virtual MFA ARNs)
                    if let Err(e) = client
                        .delete_virtual_mfa_device()
                        .serial_number(serial)
                        .send()
                        .await
                    {
                        eprintln!("delete_virtual_mfa_device {} -> {}", serial, e);
                    }
                }
            }
        }
        Err(e) => eprintln!("list_mfa_devices failed: {}", e),
    }

    // 5) Delete SSH public keys
    match client
        .list_ssh_public_keys()
        .user_name(user_name)
        .send()
        .await
    {
        Ok(resp) => {
            for k in resp.ssh_public_keys() {
                let id = k.ssh_public_key_id();
                if !id.is_empty() {
                    if let Err(e) = client
                        .delete_ssh_public_key()
                        .user_name(user_name)
                        .ssh_public_key_id(id)
                        .send()
                        .await
                    {
                        eprintln!("delete_ssh_public_key {} -> {}", id, e);
                    }
                }
            }
        }
        Err(e) => eprintln!("list_ssh_public_keys failed: {}", e),
    }

    // 6) Delete service-specific credentials
    match client
        .list_service_specific_credentials()
        .user_name(user_name)
        .send()
        .await
    {
        Ok(resp) => {
            for c in resp.service_specific_credentials() {
                let id = c.service_specific_credential_id();
                if !id.is_empty() {
                    if let Err(e) = client
                        .delete_service_specific_credential()
                        .user_name(user_name)
                        .service_specific_credential_id(id)
                        .send()
                        .await
                    {
                        eprintln!("delete_service_specific_credential {} -> {}", id, e);
                    }
                }
            }
        }
        Err(e) => eprintln!("list_service_specific_credentials failed: {}", e),
    }

    // 7) Delete inline policies
    match client
        .list_user_policies()
        .user_name(user_name)
        .send()
        .await
    {
        Ok(resp) => {
            for name in resp.policy_names() {
                if let Err(e) = client
                    .delete_user_policy()
                    .user_name(user_name)
                    .policy_name(name)
                    .send()
                    .await
                {
                    eprintln!("delete_user_policy {} -> {}", name, e);
                }
            }
        }
        Err(e) => eprintln!("list_user_policies failed: {}", e),
    }

    // 8) Detach managed policies
    match client
        .list_attached_user_policies()
        .user_name(user_name)
        .send()
        .await
    {
        Ok(resp) => {
            for p in resp.attached_policies() {
                if let Some(arn) = p.policy_arn() {
                    if let Err(e) = client
                        .detach_user_policy()
                        .user_name(user_name)
                        .policy_arn(arn)
                        .send()
                        .await
                    {
                        eprintln!("detach_user_policy {} -> {}", arn, e);
                    }
                }
            }
        }
        Err(e) => eprintln!("list_attached_user_policies failed: {}", e),
    }

    // 9) Remove user from groups
    match client
        .list_groups_for_user()
        .user_name(user_name)
        .send()
        .await
    {
        Ok(resp) => {
            for g in resp.groups() {
                let group_name = g.group_name();
                if !group_name.is_empty() {
                    if let Err(e) = client
                        .remove_user_from_group()
                        .group_name(group_name)
                        .user_name(user_name)
                        .send()
                        .await
                    {
                        eprintln!("remove_user_from_group {} -> {}", group_name, e);
                    }
                }
            }
        }
        Err(e) => eprintln!("list_groups_for_user failed: {}", e),
    }

    // 10) Finally attempt to delete the user
    match client.delete_user().user_name(user_name).send().await {
        Ok(_) => {
            println!("Deleted user '{}'.", user_name);
            Ok(())
        }
        Err(e) => {
            eprintln!("delete_user failed: {}", e);
            Err(e.into())
        }
    }
}
