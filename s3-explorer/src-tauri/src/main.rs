// Prevents an extra console window on Windows in release builds.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use rustawssdk::explorer::{self, BucketInfo, DirPage, Explorer};
use tauri::State;

#[tauri::command]
async fn list_buckets(explorer: State<'_, Explorer>) -> Result<Vec<BucketInfo>, String> {
    explorer.list_buckets().await.map_err(explorer::error_message)
}

#[tauri::command]
async fn list_dir(
    explorer: State<'_, Explorer>,
    bucket: String,
    prefix: String,
    token: Option<String>,
) -> Result<DirPage, String> {
    explorer
        .list_dir(&bucket, &prefix, token.as_deref())
        .await
        .map_err(explorer::error_message)
}

fn main() {
    let explorer = tauri::async_runtime::block_on(Explorer::from_env());

    tauri::Builder::default()
        .manage(explorer)
        .invoke_handler(tauri::generate_handler![list_buckets, list_dir])
        .run(tauri::generate_context!())
        .expect("error while running S3 Explorer");
}
