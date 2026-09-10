//! Data-returning S3 browsing API shared by the CLI (`ls-s3`) and the
//! `s3-explorer` GUI.
//!
//! Unlike `s3.rs`, nothing here prints: every function returns plain,
//! `serde`-serializable structs so a caller can render them however it likes.
//!
//! S3 has no real folders. `list_dir` emulates them with ListObjectsV2 and
//! `delimiter("/")`: `common_prefixes` become sub-folders and `contents`
//! become the files directly inside the current prefix.

use std::collections::HashMap;
use std::sync::Mutex;

use aws_config::meta::region::RegionProviderChain;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::DateTimeFormat;
use serde::Serialize;

/// S3 returns at most 1000 keys (folders + files) per ListObjectsV2 call.
const PAGE_SIZE: i32 = 1000;

#[derive(Debug, Clone, Serialize)]
pub struct BucketInfo {
    pub name: String,
    /// Region reported by ListBuckets; `None` if S3 did not include it.
    pub region: Option<String>,
    /// RFC 3339 timestamp.
    pub created: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FolderEntry {
    /// Display name relative to the listed prefix, without the trailing `/`.
    pub name: String,
    /// Full prefix to pass back to `list_dir` to open this folder.
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileEntry {
    /// Display name relative to the listed prefix.
    pub name: String,
    /// Full object key.
    pub key: String,
    pub size: i64,
    /// RFC 3339 timestamp.
    pub last_modified: Option<String>,
    pub storage_class: Option<String>,
}

/// One page of a "directory" listing.
#[derive(Debug, Clone, Serialize)]
pub struct DirPage {
    pub bucket: String,
    pub prefix: String,
    pub folders: Vec<FolderEntry>,
    pub files: Vec<FileEntry>,
    /// Pass back to `list_dir` to fetch the next page; `None` on the last page.
    pub next_token: Option<String>,
}

/// S3 browser holding one client per region.
///
/// A client pinned to one region fails with a redirect error on buckets that
/// live elsewhere, so each bucket is served by a client for its own region.
/// Regions come from ListBuckets and fall back to GetBucketLocation.
pub struct Explorer {
    config: SdkConfig,
    default_client: S3Client,
    regional_clients: Mutex<HashMap<String, S3Client>>,
    bucket_regions: Mutex<HashMap<String, String>>,
}

impl Explorer {
    pub fn new(config: &SdkConfig) -> Self {
        Self {
            config: config.clone(),
            default_client: S3Client::new(config),
            regional_clients: Mutex::new(HashMap::new()),
            bucket_regions: Mutex::new(HashMap::new()),
        }
    }

    /// Build from the default provider chain (AWS_PROFILE, AWS_REGION, ...).
    ///
    /// Falls back to us-east-1 when no region is configured: a GUI launched
    /// from Finder/Explorer does not inherit the shell's AWS_REGION. That is
    /// only the region used for ListBuckets; each bucket is still accessed
    /// through a client for its own region.
    pub async fn from_env() -> Self {
        let region = RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region)
            .load()
            .await;
        Self::new(&config)
    }

    /// List every bucket in the account, sorted by name.
    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>, aws_sdk_s3::Error> {
        let mut buckets = Vec::new();
        let mut paginator = self.default_client.list_buckets().into_paginator().send();
        while let Some(page) = paginator.next().await {
            let page = page?;
            for b in page.buckets() {
                let Some(name) = b.name() else { continue };
                buckets.push(BucketInfo {
                    name: name.to_string(),
                    region: b.bucket_region().map(str::to_string),
                    created: b
                        .creation_date()
                        .and_then(|d| d.fmt(DateTimeFormat::DateTime).ok()),
                });
            }
        }

        let mut regions = self.bucket_regions.lock().unwrap();
        for b in &buckets {
            if let Some(r) = &b.region {
                regions.insert(b.name.clone(), r.clone());
            }
        }
        drop(regions);

        buckets.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(buckets)
    }

    /// List one page of the "folder" `prefix` inside `bucket`.
    ///
    /// `prefix` is `""` for the bucket root, otherwise it should end with `/`
    /// (one is appended if missing). Folders and files are sorted by name.
    pub async fn list_dir(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<&str>,
    ) -> Result<DirPage, aws_sdk_s3::Error> {
        let prefix = normalize_prefix(prefix);
        let client = self.client_for(bucket).await?;

        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(&prefix)
            .delimiter("/")
            .max_keys(PAGE_SIZE)
            .set_continuation_token(continuation_token.map(str::to_string))
            .send()
            .await?;

        let mut folders: Vec<FolderEntry> = resp
            .common_prefixes()
            .iter()
            .filter_map(|cp| cp.prefix())
            .map(|p| FolderEntry {
                name: display_name(p.strip_prefix(prefix.as_str()).unwrap_or(p).trim_end_matches('/')),
                prefix: p.to_string(),
            })
            .collect();

        let mut files: Vec<FileEntry> = resp
            .contents()
            .iter()
            .filter_map(|o| {
                let key = o.key()?;
                // The zero-byte "folder marker" object for the current prefix
                // (created by the console's "Create folder") is not a file.
                if key == prefix {
                    return None;
                }
                Some(FileEntry {
                    name: display_name(key.strip_prefix(prefix.as_str()).unwrap_or(key)),
                    key: key.to_string(),
                    size: o.size().unwrap_or(0),
                    last_modified: o
                        .last_modified()
                        .and_then(|d| d.fmt(DateTimeFormat::DateTime).ok()),
                    storage_class: o.storage_class().map(|s| s.as_str().to_string()),
                })
            })
            .collect();

        folders.sort_by(|a, b| a.name.cmp(&b.name));
        files.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(DirPage {
            bucket: bucket.to_string(),
            prefix,
            folders,
            files,
            next_token: resp.next_continuation_token().map(str::to_string),
        })
    }

    /// Return a client for the bucket's region, resolving and caching it.
    async fn client_for(&self, bucket: &str) -> Result<S3Client, aws_sdk_s3::Error> {
        let cached = self.bucket_regions.lock().unwrap().get(bucket).cloned();
        let region = match cached {
            Some(r) => r,
            None => {
                let resp = self
                    .default_client
                    .get_bucket_location()
                    .bucket(bucket)
                    .send()
                    .await?;
                // An empty LocationConstraint means us-east-1; "EU" is the
                // legacy name for eu-west-1.
                let r = match resp.location_constraint().map(|c| c.as_str()) {
                    None | Some("") => "us-east-1".to_string(),
                    Some("EU") => "eu-west-1".to_string(),
                    Some(other) => other.to_string(),
                };
                self.bucket_regions
                    .lock()
                    .unwrap()
                    .insert(bucket.to_string(), r.clone());
                r
            }
        };

        if self.config.region().map(|r| r.as_ref()) == Some(region.as_str()) {
            return Ok(self.default_client.clone());
        }

        let mut clients = self.regional_clients.lock().unwrap();
        let client = clients.entry(region.clone()).or_insert_with(|| {
            let conf = aws_sdk_s3::config::Builder::from(&self.config)
                .region(Region::new(region))
                .build();
            S3Client::from_conf(conf)
        });
        Ok(client.clone())
    }
}

/// Render an SDK error with its code and message; the plain `Display` of
/// `aws_sdk_s3::Error` often says only "service error".
pub fn error_message(e: aws_sdk_s3::Error) -> String {
    aws_sdk_s3::error::DisplayErrorContext(e).to_string()
}

fn normalize_prefix(prefix: &str) -> String {
    if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{}/", prefix)
    }
}

/// Keys such as `a//b` produce empty path segments; show them as `/`
/// rather than as a blank row.
fn display_name(name: &str) -> String {
    if name.is_empty() {
        "/".to_string()
    } else {
        name.to_string()
    }
}
