//! A shard pin set by `SET LOCAL pgdog.sharding_key` must end with its
//! transaction, also when the `SET` arrives via the extended protocol and the
//! transaction never connects to a server.
//!
//! asyncpg (and so SQLAlchemy) sends every statement as Parse/Describe/Sync
//! followed by Bind/Execute/Sync. The leaked pin made the next transaction's
//! first query fail with "cannot switch shards in a direct-to-shard
//! transaction".

use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test_sharded, set},
};

use super::prelude::*;

/// Sharded client whose `pgdog.sharding_key` resolves via the `sharded` table
/// hash, not as a schema name (same setup as
/// `test_set_sharding_key_pins_transaction_to_one_shard`).
async fn pinning_client() -> TestClient {
    load_test_sharded();
    let mut cfg = (*config()).clone();
    cfg.config.sharded_schemas.clear();
    cfg.config
        .sharded_tables
        .retain(|t| t.name.as_deref() == Some("sharded"));
    set(cfg).unwrap();
    reload_from_existing().unwrap();

    TestClient::new(Parameters::default()).await
}

async fn extended_set_local(client: &mut TestClient, name: &str, sharding_key: i64) {
    client
        .send(Parse::named(
            name,
            &format!("SET LOCAL pgdog.sharding_key TO '{sharding_key}'"),
        ))
        .await;
    client.send(Describe::new_statement(name)).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    client.send(Bind::new_params(name, &[])).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
}

async fn simple(client: &mut TestClient, sql: &str) {
    client.send_simple(Query::new(sql)).await;
    client
        .read_until('Z')
        .await
        .unwrap_or_else(|err| panic!("{sql}: {}", err.message));
}

async fn assert_pin_ends_with_transaction(end: &str) {
    let mut client = pinning_client().await;

    for (pinned, other) in [(0, 1), (1, 0)] {
        let key = client.random_id_for_shard(pinned);
        simple(&mut client, "BEGIN").await;
        extended_set_local(&mut client, &format!("pin_{end}_{pinned}"), key).await;
        simple(&mut client, end).await;

        // Not pinned any more: the next transaction may target any shard. Target
        // the other one explicitly; an unkeyed query is round-robin and could
        // land on the leaked shard by chance.
        simple(&mut client, "BEGIN").await;
        simple(&mut client, &format!("/* pgdog_shard: {other} */ SELECT 1")).await;
        simple(&mut client, "ROLLBACK").await;
    }
}

#[tokio::test]
async fn extended_set_local_pin_does_not_outlive_commit() {
    assert_pin_ends_with_transaction("COMMIT").await;
}

#[tokio::test]
async fn extended_set_local_pin_does_not_outlive_rollback() {
    assert_pin_ends_with_transaction("ROLLBACK").await;
}
