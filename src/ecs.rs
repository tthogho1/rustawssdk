use aws_sdk_ecs::Client as EcsClient;

pub async fn list_ecs_clusters(client: &EcsClient) -> Result<usize, aws_sdk_ecs::Error> {
    let mut paginator = client.list_clusters().into_paginator().send();
    let mut count = 0usize;

    while let Some(page_res) = paginator.next().await {
        let page = page_res?;
        let arns = page.cluster_arns();
        if arns.is_empty() {
            continue;
        }
        for arn in arns {
            println!("{}", arn);
            count += 1;
        }
    }

    Ok(count)
}

pub async fn list_ecs_services(client: &EcsClient, cluster: &str) -> Result<usize, aws_sdk_ecs::Error> {
    let mut paginator = client
        .list_services()
        .cluster(cluster)
        .into_paginator()
        .send();

    let mut count = 0usize;
    while let Some(page_res) = paginator.next().await {
        let page = page_res?;
        let arns = page.service_arns();
        if arns.is_empty() {
            continue;
        }
        for arn in arns {
            println!("{}", arn);
            count += 1;
        }
    }

    Ok(count)
}

pub async fn list_ecs_tasks(client: &EcsClient, cluster: &str) -> Result<usize, aws_sdk_ecs::Error> {
    let mut paginator = client
        .list_tasks()
        .cluster(cluster)
        .into_paginator()
        .send();

    let mut count = 0usize;
    while let Some(page_res) = paginator.next().await {
        let page = page_res?;
        let arns = page.task_arns();
        if arns.is_empty() {
            continue;
        }
        for arn in arns {
            println!("{}", arn);
            count += 1;
        }
    }

    Ok(count)
}

