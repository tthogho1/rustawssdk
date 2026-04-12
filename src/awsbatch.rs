use aws_sdk_batch::Client as BatchClient;

pub async fn create_managed_fargate_compute_environment(
    client: &BatchClient,
    compute_environment_name: &str,
    max_vcpus: i32,
    subnets: Vec<String>,
    security_group_ids: Vec<String>,
    service_role: &str,
) -> Result<(), aws_sdk_batch::Error> {
    // Build the ComputeResource model for FARGATE
    let compute_resource = aws_sdk_batch::model::ComputeResource::builder()
        .type_("FARGATE")
        .max_v_cpus(max_vcpus)
        .set_subnets(Some(subnets))
        .set_security_group_ids(Some(security_group_ids))
        .build();

    client
        .create_compute_environment()
        .compute_environment_name(compute_environment_name)
        .type_("MANAGED")
        .service_role(service_role)
        .compute_resources(compute_resource)
        .send()
        .await?;

    Ok(())
}

/// Create a Job Queue that uses the given compute environment.
pub async fn create_job_queue(
    client: &BatchClient,
    job_queue_name: &str,
    compute_environment: &str,
    order: i32,
    priority: i32,
) -> Result<(), aws_sdk_batch::Error> {
    let ce_order = aws_sdk_batch::model::ComputeEnvironmentOrder::builder()
        .order(order)
        .compute_environment(compute_environment)
        .build();

    client
        .create_job_queue()
        .job_queue_name(job_queue_name)
        .compute_environment_order(ce_order)
        .state("ENABLED")
        .priority(priority)
        .send()
        .await?;

    Ok(())
}

/// Register a Fargate-compatible job definition.
///
/// `image` should be the container image (ECR URI), `execution_role_arn` is the
/// task execution role, and `log_group`, `log_region`, `log_stream_prefix` are
/// used to configure awslogs options.
pub async fn register_fargate_job_definition(
    client: &BatchClient,
    job_definition_name: &str,
    image: &str,
    vcpus: i32,
    memory: i32,
    execution_role_arn: &str,
    log_group: &str,
    log_region: &str,
    log_stream_prefix: &str,
) -> Result<(), aws_sdk_batch::Error> {
    // Build log options map
    let mut log_opts = std::collections::HashMap::new();
    log_opts.insert("awslogs-group".to_string(), log_group.to_string());
    log_opts.insert("awslogs-region".to_string(), log_region.to_string());
    log_opts.insert(
        "awslogs-stream-prefix".to_string(),
        log_stream_prefix.to_string(),
    );

    let log_configuration = aws_sdk_batch::model::LogConfiguration::builder()
        .log_driver("awslogs")
        .set_options(Some(log_opts))
        .build();

    let container_properties = aws_sdk_batch::model::ContainerProperties::builder()
        .image(image)
        .vcpus(vcpus)
        .memory(memory)
        .execution_role_arn(execution_role_arn)
        .log_configuration(log_configuration)
        .build();

    client
        .register_job_definition()
        .job_definition_name(job_definition_name)
        .type_("container")
        .container_properties(container_properties)
        .set_platform_capabilities(Some(vec!["FARGATE".to_string()]))
        .send()
        .await?;

    Ok(())
}
