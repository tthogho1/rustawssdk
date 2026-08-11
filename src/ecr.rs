use aws_sdk_ecr::Client as EcrClient;

pub async fn list_ecr_repositories(client: &EcrClient) -> Result<usize, aws_sdk_ecr::Error> {
    let mut paginator = client.describe_repositories().into_paginator().send();
    let mut count = 0usize;

    while let Some(page_res) = paginator.next().await {
        let page = page_res?;
        let repos = page.repositories();
        if repos.is_empty() {
            continue;
        }
        for repo in repos {
            let name = repo.repository_name().unwrap_or("(no name)");
            let uri = repo.repository_uri().unwrap_or("(no uri)");
            println!("{}\t{}", name, uri);
            count += 1;
        }
    }

    Ok(count)
}
