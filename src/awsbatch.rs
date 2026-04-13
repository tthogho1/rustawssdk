use aws_sdk_batch::Client as BatchClient;
use aws_sdk_batch::types::{
    CeType, ComputeEnvironmentOrder, ComputeResource, ContainerProperties, CrType,
    JobDefinitionType, JqState, LogConfiguration, LogDriver, PlatformCapability,
    ResourceRequirement, ResourceType,
};
use std::collections::HashMap;

/// List all compute environments (name, status, ARN).
pub async fn list_compute_environments(
    client: &BatchClient,
) -> Result<usize, aws_sdk_batch::Error> {
    let resp = client.describe_compute_environments().send().await?;
    let envs = resp.compute_environments();
    for env in envs {
        println!(
            "{}\t{}\t{}",
            env.compute_environment_name().unwrap_or("-"),
            env.status().map(|s| s.as_str()).unwrap_or("-"),
            env.compute_environment_arn().unwrap_or("-"),
        );
    }
    Ok(envs.len())
}

/// List all job queues (name, state, priority, ARN).
pub async fn list_job_queues(client: &BatchClient) -> Result<usize, aws_sdk_batch::Error> {
    let resp = client.describe_job_queues().send().await?;
    let queues = resp.job_queues();
    for q in queues {
        println!(
            "{}\t{}\tpriority:{}\t{}",
            q.job_queue_name().unwrap_or("-"),
            q.state().map(|s| s.as_str()).unwrap_or("-"),
            q.priority().unwrap_or(0),
            q.job_queue_arn().unwrap_or("-"),
        );
    }
    Ok(queues.len())
}

/// List all job definitions (name, revision, status).
pub async fn list_job_definitions(client: &BatchClient) -> Result<usize, aws_sdk_batch::Error> {
    let resp = client.describe_job_definitions().send().await?;
    let defs = resp.job_definitions();
    for d in defs {
        println!(
            "{}\trev:{}\t{}",
            d.job_definition_name().unwrap_or("-"),
            d.revision().unwrap_or(0),
            d.status().unwrap_or("-"),
        );
    }
    Ok(defs.len())
}

/// Create a MANAGED Fargate compute environment.
/// subnets and security_group_ids are comma-separated strings split by the caller.
pub async fn create_managed_fargate_compute_environment(
    client: &BatchClient,
    name: &str,
    max_vcpus: i32,
    subnets: Vec<String>,
    security_group_ids: Vec<String>,
    service_role: &str,
) -> Result<(), aws_sdk_batch::Error> {
    let compute_resource = ComputeResource::builder()
        .r#type(CrType::Fargate)
        .maxv_cpus(max_vcpus)
        .set_subnets(Some(subnets))
        .set_security_group_ids(Some(security_group_ids))
        .build();

    let resp = client
        .create_compute_environment()
        .compute_environment_name(name)
        .r#type(CeType::Managed)
        .service_role(service_role)
        .compute_resources(compute_resource)
        .send()
        .await?;

    println!(
        "Created compute environment: {}",
        resp.compute_environment_arn().unwrap_or("-")
    );
    Ok(())
}

/// Create a job queue backed by the given compute environment.
pub async fn create_job_queue(
    client: &BatchClient,
    name: &str,
    compute_environment: &str,
    priority: i32,
) -> Result<(), aws_sdk_batch::Error> {
    let ce_order = ComputeEnvironmentOrder::builder()
        .order(1)
        .compute_environment(compute_environment)
        .build();

    let resp = client
        .create_job_queue()
        .job_queue_name(name)
        .compute_environment_order(ce_order)
        .state(JqState::Enabled)
        .priority(priority)
        .send()
        .await?;

    println!("Created job queue: {}", resp.job_queue_arn().unwrap_or("-"));
    Ok(())
}

/// Register a Fargate job definition using an ECR image.
///
/// - `vcpus`: vCPU count as a string, e.g. "1" or "0.25"
/// - `memory`: memory in MiB as a string, e.g. "2048"
pub async fn register_fargate_job_definition(
    client: &BatchClient,
    name: &str,
    image: &str,
    vcpus: &str,
    memory: &str,
    execution_role_arn: &str,
    log_group: &str,
    log_region: &str,
    log_stream_prefix: &str,
) -> Result<(), aws_sdk_batch::Error> {
    let mut log_opts = HashMap::new();
    log_opts.insert("awslogs-group".to_string(), log_group.to_string());
    log_opts.insert("awslogs-region".to_string(), log_region.to_string());
    log_opts.insert(
        "awslogs-stream-prefix".to_string(),
        log_stream_prefix.to_string(),
    );

    let log_cfg = LogConfiguration::builder()
        .log_driver(LogDriver::Awslogs)
        .set_options(Some(log_opts))
        .build();

    let resource_reqs = vec![
        ResourceRequirement::builder()
            .r#type(ResourceType::Vcpu)
            .value(vcpus)
            .build(),
        ResourceRequirement::builder()
            .r#type(ResourceType::Memory)
            .value(memory)
            .build(),
    ];

    let container_props = ContainerProperties::builder()
        .image(image)
        .set_resource_requirements(Some(resource_reqs))
        .execution_role_arn(execution_role_arn)
        .log_configuration(log_cfg)
        .build();

    let resp = client
        .register_job_definition()
        .job_definition_name(name)
        .r#type(JobDefinitionType::Container)
        .container_properties(container_props)
        .platform_capabilities(PlatformCapability::Fargate)
        .send()
        .await?;

    println!(
        "Registered job definition: {}:{}",
        resp.job_definition_name().unwrap_or("-"),
        resp.revision().unwrap_or(0),
    );
    Ok(())
}

/// Submit a job and print the job ID.
pub async fn submit_job(
    client: &BatchClient,
    job_name: &str,
    job_queue: &str,
    job_definition: &str,
) -> Result<String, aws_sdk_batch::Error> {
    let resp = client
        .submit_job()
        .job_name(job_name)
        .job_queue(job_queue)
        .job_definition(job_definition)
        .send()
        .await?;

    let job_id = resp.job_id().unwrap_or("").to_string();
    println!("Submitted: {} (id: {})", resp.job_name().unwrap_or("-"), job_id);
    Ok(job_id)
}

/// Describe a job by ID and print its status and container details.
pub async fn describe_job(
    client: &BatchClient,
    job_id: &str,
) -> Result<(), aws_sdk_batch::Error> {
    let resp = client.describe_jobs().jobs(job_id).send().await?;
    let jobs = resp.jobs();
    if jobs.is_empty() {
        println!("No job found with id: {}", job_id);
        return Ok(());
    }
    for job in jobs {
        println!("Name:    {}", job.job_name().unwrap_or("-"));
        println!("ID:      {}", job.job_id().unwrap_or("-"));
        println!("Status:  {}", job.status().map(|s| s.as_str()).unwrap_or("-"));
        println!("Queue:   {}", job.job_queue().unwrap_or("-"));
        println!("Def:     {}", job.job_definition().unwrap_or("-"));
        if let Some(ms) = job.created_at() {
            println!("Created: {} (unix ms)", ms);
        }
        if let Some(ms) = job.started_at() {
            println!("Started: {} (unix ms)", ms);
        }
        if let Some(ms) = job.stopped_at() {
            println!("Stopped: {} (unix ms)", ms);
        }
        if let Some(container) = job.container() {
            if let Some(log_stream) = container.log_stream_name() {
                println!("LogStream: {}", log_stream);
            }
            if let Some(reason) = container.reason() {
                println!("ContainerReason: {}", reason);
            }
            if let Some(exit_code) = container.exit_code() {
                println!("ExitCode: {}", exit_code);
            }
        }
        if let Some(reason) = job.status_reason() {
            println!("StatusReason: {}", reason);
        }
    }
    Ok(())
}
