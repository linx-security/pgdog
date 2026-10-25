//! Pool read/write counters must follow the transaction's routing decision,
//! not its first statement's.
//!
//! asyncpg sends `BEGIN` as a simple query and every statement after it via the
//! extended protocol. SQLAlchemy's pre-ping (`BEGIN; ;  ROLLBACK`) is routed to
//! the primary as a write transaction but was counted as a read on the primary,
//! and `BEGIN READ ONLY` transactions starting with `set_config` were counted as
//! writes on the replica.

use crate::{backend::databases::databases, config::Role, net::Parameters};

use super::prelude::*;

/// (reads, writes) checkout counters of the primary and replica pools.
fn counts() -> ((usize, usize), (usize, usize)) {
    let mut primary = (0, 0);
    let mut replica = (0, 0);

    for (role, pool) in
        databases().cluster(("pgdog", "pgdog")).unwrap().shards()[0].pools_with_roles()
    {
        let counts = pool.state().stats.counts;
        let target = match role {
            Role::Primary => &mut primary,
            _ => &mut replica,
        };
        target.0 += counts.reads;
        target.1 += counts.writes;
    }

    (primary, replica)
}

async fn extended(client: &mut TestClient, sql: &str, params: &[&str]) {
    let params: Vec<_> = params
        .iter()
        .map(|p| Parameter::new(p.as_bytes()))
        .collect();
    client.send(Parse::new_anonymous(sql)).await;
    client.send(Bind::new_params("", &params)).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    client
        .read_until('Z')
        .await
        .unwrap_or_else(|err| panic!("{sql}: {}", err.message));
}

async fn simple(client: &mut TestClient, sql: &str) {
    client.send_simple(Query::new(sql)).await;
    client
        .read_until('Z')
        .await
        .unwrap_or_else(|err| panic!("{sql}: {}", err.message));
}

#[tokio::test]
async fn pre_ping_transaction_counts_as_write_on_primary() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;
    let (primary_before, replica_before) = counts();

    simple(&mut client, "BEGIN").await;
    extended(&mut client, ";", &[]).await;
    simple(&mut client, "ROLLBACK").await;

    let (primary, replica) = counts();
    assert_eq!(primary.0, primary_before.0, "read counted on the primary");
    assert_eq!(primary.1, primary_before.1 + 1);
    assert_eq!(replica, replica_before);
}

#[tokio::test]
async fn read_only_transaction_counts_as_read_on_replica() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;
    let (primary_before, replica_before) = counts();

    simple(&mut client, "BEGIN READ ONLY").await;
    // Bound parameters, as SQLAlchemy sends them: PgDog handles a literal
    // `set_config` itself, without a server.
    extended(
        &mut client,
        "SELECT set_config($1, $2, true)",
        &["app.tenant", "1"],
    )
    .await;
    simple(&mut client, "COMMIT").await;

    let (primary, replica) = counts();
    assert_eq!(primary, primary_before);
    assert_eq!(replica.0, replica_before.0 + 1);
    assert_eq!(replica.1, replica_before.1, "write counted on the replica");
}
