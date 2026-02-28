use aws_sdk_dynamodb::Client as DdbClient;
use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::{HashMap, HashSet};

pub async fn describe_table_schema(
    client: &DdbClient,
    table: &str,
) -> Result<(), aws_sdk_dynamodb::Error> {
    match client.describe_table().table_name(table).send().await {
        Ok(resp) => {
            if let Some(t) = resp.table() {
                println!("\nDynamoDB table: {}", table);
                let attrs = t.attribute_definitions();
                if !attrs.is_empty() {
                    println!("AttributeDefinitions:");
                    for a in attrs {
                        let name = a.attribute_name();
                        let typ = format!("{:?}", a.attribute_type());
                        println!("  - name: {}, type: {}", name, typ);
                    }
                }
                let keys = t.key_schema();
                if !keys.is_empty() {
                    println!("KeySchema:");
                    for k in keys {
                        let name = k.attribute_name();
                        let key_type = format!("{:?}", k.key_type());
                        println!("  - name: {}, key_type: {}", name, key_type);
                    }
                }
            } else {
                println!("Table {} not found or has no metadata.", table);
            }
            Ok(())
        }
        Err(e) => {
            // Convert SdkError into the SDK Error type then inspect its message/code.
            let sdk_err: aws_sdk_dynamodb::Error = e.into();
            let msg = sdk_err.to_string();
            if msg.contains("ResourceNotFoundException") {
                println!("Table '{}' not found.", table);
                return Ok(());
            }
            Err(sdk_err)
        }
    }
}

pub async fn list_tables(client: &DdbClient) -> Result<(), aws_sdk_dynamodb::Error> {
    let resp = client.list_tables().send().await?;
    let names = resp.table_names();
    if names.is_empty() {
        println!("No DynamoDB tables found.");
    } else {
        println!("DynamoDB tables:");
        for n in names {
            println!("  {}", n);
        }
    }
    Ok(())
}

pub async fn scan_table(client: &DdbClient, table: &str) -> Result<u64, aws_sdk_dynamodb::Error> {
    let mut count: u64 = 0;
    let mut paginator = client.scan().table_name(table).into_paginator().send();

    while let Some(page_res) = paginator.next().await {
        let page = page_res?;
        let items = page.items();
        if items.is_empty() {
            continue;
        }
        for item in items {
            // Print item using debug formatting; AttributeValue supports Debug.
            println!("{:#?}\n", item);
            count += 1;
        }
    }

    Ok(count)
}

pub async fn scan_table_csv(client: &DdbClient, table: &str) -> Result<u64, aws_sdk_dynamodb::Error> {
    let mut items: Vec<HashMap<String, AttributeValue>> = Vec::new();
    let mut paginator = client.scan().table_name(table).into_paginator().send();

    while let Some(page_res) = paginator.next().await {
        let page = page_res?;
        let page_items = page.items();
        if page_items.is_empty() {
            continue;
        }
        for item in page_items {
            items.push(item.clone());
        }
    }

    if items.is_empty() {
        println!("(no items)");
        return Ok(0);
    }

    // Collect all header keys in stable order
    let mut keys_set: HashSet<String> = HashSet::new();
    for it in &items {
        for k in it.keys() {
            keys_set.insert(k.clone());
        }
    }
    let mut headers: Vec<String> = keys_set.into_iter().collect();
    headers.sort();

    // CSV helper to escape values
    fn escape_csv(s: &str) -> String {
        let mut out = s.replace('"', "\"\"");
        out = out.replace('\n', "\\n");
        format!("\"{}\"", out)
    }

    // print header
    let header_line = headers.iter().map(|h| escape_csv(h)).collect::<Vec<_>>().join(",");
    println!("{}", header_line);

    // print rows
    for it in &items {
        let mut row: Vec<String> = Vec::with_capacity(headers.len());
        for h in &headers {
            let val = it.get(h).map(|v| format!("{:?}", v)).unwrap_or_default();
            row.push(escape_csv(&val));
        }
        println!("{}", row.join(","));
    }

    Ok(items.len() as u64)
}

pub async fn scan_table_tsv(client: &DdbClient, table: &str) -> Result<u64, aws_sdk_dynamodb::Error> {
    let mut items: Vec<HashMap<String, AttributeValue>> = Vec::new();
    let mut paginator = client.scan().table_name(table).into_paginator().send();

    while let Some(page_res) = paginator.next().await {
        let page = page_res?;
        let page_items = page.items();
        if page_items.is_empty() {
            continue;
        }
        for item in page_items {
            items.push(item.clone());
        }
    }

    if items.is_empty() {
        println!("(no items)");
        return Ok(0);
    }

    // Collect all header keys in stable order
    let mut keys_set: HashSet<String> = HashSet::new();
    for it in &items {
        for k in it.keys() {
            keys_set.insert(k.clone());
        }
    }
    let mut headers: Vec<String> = keys_set.into_iter().collect();
    headers.sort();

    // TSV helper to escape values (tabs and newlines)
    fn escape_tsv(s: &str) -> String {
        let mut out = s.replace('\t', "\\t");
        out = out.replace('\n', "\\n");
        out
    }

    // print header (tab-separated)
    let header_line = headers.join("\t");
    println!("{}", header_line);

    // print rows
    for it in &items {
        let mut row: Vec<String> = Vec::with_capacity(headers.len());
        for h in &headers {
            let val = it.get(h).map(|v| format!("{:?}", v)).unwrap_or_default();
            row.push(escape_tsv(&val));
        }
        println!("{}", row.join("\t"));
    }

    Ok(items.len() as u64)
}

pub async fn delete_all_items(client: &DdbClient, table: &str) -> Result<u64, aws_sdk_dynamodb::Error> {
    // Describe table to get key schema
    let resp = client.describe_table().table_name(table).send().await?;
    let table_desc = match resp.table() {
        Some(t) => t,
        None => {
            println!("Table '{}' not found.", table);
            return Ok(0);
        }
    };

    let key_schema = table_desc.key_schema();
    if key_schema.is_empty() {
        println!("Table '{}' has no key schema.", table);
        return Ok(0);
    }

    let key_attrs: Vec<String> = key_schema.iter().map(|k| k.attribute_name().to_string()).collect();

    let mut deleted: u64 = 0;
    let mut paginator = client
        .scan()
        .table_name(table)
        .projection_expression(&key_attrs.join(","))
        .into_paginator()
        .send();

    while let Some(page_res) = paginator.next().await {
        let page = page_res?;
        let items = page.items();
        if items.is_empty() {
            continue;
        }
        for item in items {
            let mut key_map: HashMap<String, AttributeValue> = HashMap::new();
            for k in &key_attrs {
                if let Some(v) = item.get(k) {
                    key_map.insert(k.clone(), v.clone());
                }
            }
            if key_map.len() == key_attrs.len() {
                client
                    .delete_item()
                    .table_name(table)
                    .set_key(Some(key_map))
                    .send()
                    .await?;
                deleted += 1;
            } else {
                println!("Skipping item missing full key: {:?}", item);
            }
        }
    }

    Ok(deleted)
}

pub async fn item_exists(
    client: &DdbClient,
    table: &str,
    key: &HashMap<String, AttributeValue>,
) -> Result<bool, aws_sdk_dynamodb::Error> {
    let resp = client
        .get_item()
        .table_name(table)
        .set_key(Some(key.clone()))
        .send()
        .await?;
    Ok(resp.item().is_some())
}

pub async fn set_item_attribute(
    client: &DdbClient,
    table: &str,
    key: &HashMap<String, AttributeValue>,
    attribute_name: &str,
    attribute_value: AttributeValue,
) -> Result<(), aws_sdk_dynamodb::Error> {
    // Use expression attribute names/values to avoid reserved-word issues.
    let name_placeholder = "#attr".to_string();
    let value_placeholder = ":val".to_string();
    let update_expr = format!("SET {} = {}", name_placeholder, value_placeholder);

    let mut expr_names: HashMap<String, String> = HashMap::new();
    expr_names.insert(name_placeholder.clone(), attribute_name.to_string());

    let mut expr_values: HashMap<String, AttributeValue> = HashMap::new();
    expr_values.insert(value_placeholder.clone(), attribute_value);

    client
        .update_item()
        .table_name(table)
        .set_key(Some(key.clone()))
        .update_expression(update_expr)
        .set_expression_attribute_names(Some(expr_names))
        .set_expression_attribute_values(Some(expr_values))
        .send()
        .await?;

    Ok(())
}
