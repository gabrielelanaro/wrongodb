pub async fn wait_for_server() {
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}
