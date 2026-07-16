// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders the table snapshot side of the [`MySqlSourceConnection`] dataflow.
//!
//! # Snapshot reading
//!
//! Depending on the `source_outputs resume_upper` parameters this dataflow decides which tables to
//! snapshot and performs a simple `SELECT * FROM table` on them in order to get a snapshot.
//! There are a few subtle points about this operation, described below.
//!
//! It is crucial for correctness that we always perform the snapshot of all tables at a specific
//! point in time. This must be true even in the presence of restarts or partially committed
//! snapshots. The consistent point that the snapshot must happen at is discovered and durably
//! recorded during planning of the source and is exposed to this ingestion dataflow via the
//! `initial_gtid_set` field in `MySqlSourceDetails`.
//!
//! Unfortunately MySQL does not provide an API to perform a transaction at a specific point in
//! time. Instead, MySQL allows us to perform a snapshot of a table and let us know at which point
//! in time the snapshot was taken. Using this information we can take a snapshot at an arbitrary
//! point in time and then rewind it to the desired `initial_gtid_set` by "rewinding" it. These two
//! phases are described in the following section.
//!
//! ## Producing a snapshot at a known point in time.
//!
//! Ideally we would like to start a transaction and ask MySQL to tell us the point in time this
//! transaction is running at. As far as we know there isn't such API so we achieve this using
//! table locks instead.
//!
//! A designated leader worker acquires table locks on all the tables to be snapshotted. By doing
//! so we establish a moment in time where we know no writes are happening to the tables we are
//! interested in. The leader then reads the current upper frontier (`snapshot_upper`) using the
//! `@@gtid_executed` system variable and broadcasts it, along with PK-range bounds (see below), to
//! all workers via a timely feedback loop. This frontier establishes an upper bound on any
//! possible write to the tables of interest until the lock is released.
//!
//! Each worker now starts a transaction via a new connection with 'REPEATABLE READ' and
//! 'CONSISTENT SNAPSHOT' semantics. Due to linearizability we know that this transaction's view of
//! the database must some time `t_snapshot` such that `snapshot_upper <= t_snapshot`. We don't
//! actually know the exact value of `t_snapshot` and it might be strictly greater than
//! `snapshot_upper`. However, because this transaction will only be used to read the locked tables
//! and we know that `snapshot_upper` is an upper bound on all the writes that have happened to
//! them we can safely pretend that the transaction's `t_snapshot` is *equal* to `snapshot_upper`.
//! We have therefore succeeded in starting a transaction at a known point in time!
//!
//! Once all workers have started their transactions the leader unlocks the tables. Each worker
//! then reads the snapshot of the tables (or PK ranges) it is responsible for and publishes it
//! downstream.
//!
//! TODO: Other software products hold the table lock for the duration of the snapshot, and some do
//! not. We should figure out why and if we need to hold the lock longer. This may be because of a
//! difference in how REPEATABLE READ works in some MySQL-compatible systems (e.g. Aurora MySQL).
//!
//! ## Parallel PK-range snapshots
//!
//! For tables with a suitable primary key, the leader computes `worker_count - 1` boundary keys
//! that split the key domain into disjoint half-open ranges, and broadcasts them. Each worker
//! reads only its assigned range. Ranges are assigned round-robin starting from each table's
//! legacy single-worker owner, so the open-ended ranges (which absorb any row-count underestimate)
//! land on a different worker per table rather than always the last worker. Tables without a
//! suitable PK fall back to single-worker-per-table mode. This holds up to `worker_count + 1`
//! upstream connections per source (one per ranged worker plus the leader's lock connection),
//! which on clusters with many workers may approach the server's `max_connections` limit.
//!
//! ## Rewinding the snapshot to a specific point in time.
//!
//! Having obtained a snapshot of a table at some `snapshot_upper` we are now tasked with
//! transforming this snapshot into one at `initial_gtid_set`. In other words we have produced a
//! snapshot containing all updates that happened at `t: !(snapshot_upper <= t)` but what we
//! actually want is a snapshot containing all updates that happened at `t: !(initial_gtid <= t)`.
//!
//! If we assume that `initial_gtid_set <= snapshot_upper`, which is a fair assumption since the
//! former is obtained before the latter, then we can observe that the snapshot we produced
//! contains all updates at `t: !(initial_gtid <= t)` (i.e the snapshot we want) and some additional
//! unwanted updates at `t: initial_gtid <= t && !(snapshot_upper <= t)`. We happen to know exactly
//! what those additional unwanted updates are because those will be obtained by reading the
//! replication stream in the replication operator and so all we need to do to "rewind" our
//! `snapshot_upper` snapshot to `initial_gtid` is to ask the replication operator to "undo" any
//! updates that falls in the undesirable region.
//!
//! This is exactly what `RewindRequest` is about. It informs the replication operator that a
//! particular table has been snapshotted at `snapshot_upper` and would like all the updates
//! discovered during replication that happen at `t: initial_gtid <= t && !(snapshot_upper <= t)`.
//! to be cancelled. In Differential Dataflow this is as simple as flipping the sign of the diff
//! field.
//!
//! The snapshot reader emits updates at the minimum timestamp (by convention) to allow the
//! updates to be potentially negated by the replication operator, which will emit negated
//! updates at the minimum timestamp (by convention) when it encounters rows from a table that
//! occur before the GTID frontier in the Rewind Request for that table.
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::AsCollection;
use futures::{StreamExt as _, TryStreamExt};
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_async::{IsolationLevel, Row as MySqlRow, TxOpts};
use mz_mysql_util::{
    ER_NO_SUCH_TABLE, MySqlConn, MySqlError, pack_mysql_row, query_sys_var, quote_identifier,
};
use mz_ore::cast::CastFrom;
use mz_ore::future::InTask;
use mz_ore::iter::IteratorExt;
use mz_ore::metrics::MetricsFutureExt;
use mz_repr::{Diff, Row, SqlScalarType};
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::MySqlSourceConnection;
use mz_storage_types::sources::mysql::{GtidPartition, gtid_set_frontier};
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use mz_timely_util::containers::stack::FueledBuilder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::core::Map;
use timely::dataflow::operators::vec::Broadcast;
use timely::dataflow::operators::{CapabilitySet, Concat, ConnectLoop, Feedback};
use timely::dataflow::{Scope, StreamVec};
use timely::progress::Timestamp;
use tracing::trace;

use crate::metrics::source::mysql::MySqlSnapshotMetrics;
use crate::source::RawSourceCreationConfig;
use crate::source::types::{FuelSize, SignaledFuture, SourceMessage, StackedCollection};
use crate::statistics::SourceStatistics;

use super::schemas::verify_schemas;
use super::{
    DefiniteError, MySqlTableName, ReplicationError, RewindRequest, SourceOutputInfo,
    TransientError, return_definite_error, validate_mysql_repl_settings,
};

/// How a PK column's values are rendered as SQL literals in range predicates.
#[derive(Debug, Clone, Copy)]
enum PkColKind {
    /// Integer type, rendered and compared as a bare numeric literal.
    Numeric,
    /// Character type, rendered as a quoted string literal via MySQL `QUOTE()`
    /// and compared under the column's own collation.
    Text,
}

/// Classify a scalar type for range splitting, or `None` if unsupported.
fn pk_col_kind(scalar_type: &SqlScalarType) -> Option<PkColKind> {
    match scalar_type {
        SqlScalarType::Int16
        | SqlScalarType::Int32
        | SqlScalarType::Int64
        | SqlScalarType::UInt16
        | SqlScalarType::UInt32
        | SqlScalarType::UInt64 => Some(PkColKind::Numeric),
        SqlScalarType::Char { .. } | SqlScalarType::VarChar { .. } | SqlScalarType::String => {
            Some(PkColKind::Text)
        }
        _ => None,
    }
}

/// If `desc` has a single-column primary key of a supported type, return the
/// quoted PK column and its kind. Used for the sampling path in
/// [`compute_sampled_splits`]. Composite primary keys are not supported and fall
/// back to single-worker snapshotting.
fn formattable_pk(desc: &mz_mysql_util::MySqlTableDesc) -> Option<(String, PkColKind)> {
    let pk = desc.keys.iter().find(|k| k.is_primary)?;
    let [name] = &pk.columns[..] else {
        return None;
    };
    let col = desc.columns.iter().find(|c| &c.name == name)?;
    let kind = pk_col_kind(&col.column_type.as_ref()?.scalar_type)?;
    Some((quote_identifier(name), kind))
}

/// PK-range partition boundaries for a table, computed by the leader.
///
/// `pk_col` is the quoted PK column identifier. `boundaries` holds
/// `partition_count - 1` boundary keys, each one SQL literal. Boundary `i` is the
/// lower bound of partition `i + 1`, so the partitions are the half-open ranges
/// `[boundaries[i-1], boundaries[i])`.
///
/// INVARIANT: boundaries are non-decreasing under the same comparison the range
/// predicates use (the column's collation for text, numeric otherwise). That makes
/// the partitions disjoint and exhaustive over the entire key domain, so every
/// row is read by exactly one worker regardless of the actual data distribution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PkSplits {
    pk_col: String,
    boundaries: Vec<String>,
}

/// Snapshot info broadcast from leader to all workers.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotInfo {
    gtid_set: String,
    /// PK splits per table. None = no suitable PK, use single-worker fallback.
    pk_bounds: BTreeMap<MySqlTableName, Option<PkSplits>>,
}

/// A worker's assigned PK range for a table, as ready-to-splice SQL fragments.
struct PkRange {
    /// Quoted PK column, e.g. `` `id` ``.
    pk_col: String,
    /// Inclusive lower bound literal, or `None` for the first partition (open start).
    lower: Option<String>,
    /// Exclusive upper bound literal, or `None` for the last partition (open end).
    upper: Option<String>,
}

/// What a worker does for one table during the snapshot.
enum ReadPlan {
    /// Partitioned table: read this worker's assigned PK range.
    Range(PkRange),
    /// Unpartitioned table: this worker is responsible for it and reads it whole.
    WholeTable,
}

/// The partition index (0-based) worker `worker_id` reads for a table whose legacy
/// single-worker owner is `owner`. Partition 0 is assigned to `owner` and the rest
/// follow round-robin, so the open-started first range and the open-ended last
/// range land on a different worker for each table.
///
/// The open-ended last range absorbs any rows past the final sampled boundary, so
/// it grows without bound when the row-count estimate is low. Anchoring the
/// rotation on the legacy owner (whose hash spreads tables across workers) keeps a
/// few badly-underestimated tables from piling their surplus onto one worker.
///
/// The returned index may be `>=` the partition count, meaning this worker owns no
/// partition of the table.
fn partition_for_worker(worker_id: usize, owner: usize, worker_count: usize) -> usize {
    (worker_id + worker_count - owner % worker_count) % worker_count
}

/// The PK range for partition `partition` of `splits`, or `None` if `partition` is
/// beyond the partition count (fewer partitions than workers).
fn worker_pk_range(splits: &PkSplits, partition: usize) -> Option<PkRange> {
    let partitions = splits.boundaries.len() + 1;
    if partition >= partitions {
        return None;
    }
    Some(PkRange {
        pk_col: splits.pk_col.clone(),
        lower: (partition > 0).then(|| splits.boundaries[partition - 1].clone()),
        upper: (partition < partitions - 1).then(|| splits.boundaries[partition].clone()),
    })
}

/// Split a single-column non-integer PK into roughly even-sized partitions by
/// sampling `partition_count - 1` boundary keys via keyset pagination: each probe reads
/// the first key one chunk past the previous boundary (`WHERE pk > prev ORDER BY
/// pk LIMIT 1 OFFSET chunk-1`), so together they make a single forward pass of
/// the PK index rather than a full sort.
///
/// Boundaries come back already rendered as SQL literals (`QUOTE()` for text,
/// `CAST(.. AS CHAR)` for numeric) so the `ORDER BY` here and the range
/// predicates compare identically (the column's collation for text). Because each
/// boundary is found with a strict `pk > prev`, boundaries strictly increase.
///
/// Returns `None` if the approximate row count is below `worker_count` (including
/// a stale 0 for a never-analyzed table), or a boundary fails to decode (e.g. a
/// non-UTF-8 collation).
async fn compute_sampled_splits<Q>(
    conn: &mut Q,
    table: &MySqlTableName,
    pk_col: &(String, PkColKind),
    worker_count: usize,
) -> Result<Option<PkSplits>, SnapshotSetupError>
where
    Q: Queryable,
{
    // Approximate row count from table metadata, not an O(rows) `COUNT(*)`. It only
    // sets the sampling stride, so inaccuracy costs partition balance, not correctness.
    let total: Option<Option<u64>> = conn
        .exec_first(
            "SELECT table_rows FROM information_schema.tables \
             WHERE table_schema = ? AND table_name = ?",
            (&table.0, &table.1),
        )
        .await?;
    let total = total.flatten().unwrap_or(0);
    let partitions = std::cmp::min(u64::cast_from(worker_count), total);
    if partitions < 2 {
        return Ok(None);
    }
    let chunk = total / partitions;
    let (col, kind) = pk_col;
    let col_literal = match kind {
        PkColKind::Numeric => format!("CAST({col} AS CHAR)"),
        PkColKind::Text => format!("QUOTE({col})"),
    };

    let mut boundaries: Vec<String> = Vec::with_capacity(usize::cast_from(partitions) - 1);
    for _ in 1..partitions {
        // Skip a chunk of rows past the previous boundary and take the next key.
        // The first probe has no lower bound and skips a full chunk; later probes
        // start just after the previous boundary, so `OFFSET chunk - 1`.
        let (predicate, offset) = match boundaries.last() {
            Some(prev) => (format!(" WHERE {col} > {prev}"), chunk - 1),
            None => (String::new(), chunk),
        };
        // The identifier is quoted via `quote_identifier`, the previous boundary is
        // itself a value MySQL rendered as a literal, `table` via Display, and the
        // offset is an integer, so this interpolation is safe; not parameterizable.
        #[allow(clippy::disallowed_methods)]
        let row: Option<MySqlRow> = conn
            .query_first(format!(
                "SELECT {col_literal} FROM {table}{predicate} \
                 ORDER BY {col} LIMIT 1 OFFSET {offset}"
            ))
            .await
            .map_err(classify_query_error)?;
        // Ran off the end (table smaller than COUNT implied): stop, and use the
        // boundaries found so far. Fewer partitions is still correct.
        let Some(mut row) = row else { break };
        // The column is CAST/QUOTE-ed to text, so it decodes as a String that is
        // already a valid SQL literal. A decode failure (e.g. a non-UTF-8
        // collation) means we can't safely partition: fall back.
        match row.take_opt::<String, usize>(0) {
            Some(Ok(lit)) => boundaries.push(lit),
            _ => return Ok(None),
        }
    }
    if boundaries.is_empty() {
        return Ok(None);
    }
    Ok(Some(PkSplits {
        pk_col: col.clone(),
        boundaries,
    }))
}

/// Renders the snapshot dataflow. See the module documentation for more information.
pub(crate) fn render<'scope>(
    scope: Scope<'scope, GtidPartition>,
    config: RawSourceCreationConfig,
    connection: MySqlSourceConnection,
    source_outputs: Vec<SourceOutputInfo>,
    metrics: MySqlSnapshotMetrics,
) -> (
    StackedCollection<'scope, GtidPartition, (usize, Result<SourceMessage, DataflowError>)>,
    StreamVec<'scope, GtidPartition, RewindRequest>,
    StreamVec<'scope, GtidPartition, ReplicationError>,
    PressOnDropButton,
) {
    let mut builder =
        AsyncOperatorBuilder::new(format!("MySqlSnapshotReader({})", config.id), scope.clone());

    let (feedback_handle, feedback_data) = scope.feedback(Default::default());

    let (raw_handle, raw_data) = builder.new_output::<FueledBuilder<_>>();
    let (rewinds_handle, rewinds) = builder.new_output::<CapacityContainerBuilder<Vec<_>>>();
    // Captures DefiniteErrors that affect the entire source, including all outputs
    let (definite_error_handle, definite_errors) =
        builder.new_output::<CapacityContainerBuilder<Vec<_>>>();
    let (snapshot_handle, snapshot) = builder.new_output::<CapacityContainerBuilder<Vec<_>>>();

    // This operator needs to broadcast data to itself in order to synchronize the transaction
    // snapshot. However, none of the feedback capabilities result in output messages and for the
    // feedback edge specifically having a default connection would result in a loop.
    let mut snapshot_input = builder.new_disconnected_input(feedback_data, Pipeline);

    // The snapshot info must be sent to all workers, so we broadcast the feedback connection
    snapshot.broadcast().connect_loop(feedback_handle);

    let is_snapshot_leader = config.responsible_for("mysql_snapshot_leader");

    // A global view of all outputs that will be snapshot by all workers.
    let mut all_outputs = vec![];
    // The table infos to snapshot. Every worker holds all of them, since parallel
    // PK-range reads split each table across workers.
    let mut reader_snapshot_table_info = BTreeMap::new();
    // Maps MySQL table name to export `SourceStatistics`. Same info exists in reader_snapshot_table_info,
    // but this avoids having to iterate + map each time the statistics are needed.
    let mut export_statistics = BTreeMap::new();
    for output in source_outputs.into_iter() {
        // Determine which outputs need to be snapshot and which already have been.
        if *output.resume_upper != [GtidPartition::minimum()] {
            // Already has been snapshotted.
            continue;
        }
        all_outputs.push(output.output_index);
        let export_stats = config
            .statistics
            .get(&output.export_id)
            .expect("statistics have been intialized")
            .clone();
        export_statistics
            .entry(output.table_name.clone())
            .or_insert_with(Vec::new)
            .push(export_stats);

        reader_snapshot_table_info
            .entry(output.table_name.clone())
            .or_insert_with(Vec::new)
            .push(output);
    }

    let (button, transient_errors): (_, StreamVec<'scope, GtidPartition, Rc<TransientError>>) =
        builder.build_fallible(move |caps| {
            let busy_signal = Arc::clone(&config.busy_signal);
            Box::pin(SignaledFuture::new(busy_signal, async move {
                let [
                    data_cap_set,
                    rewind_cap_set,
                    definite_error_cap_set,
                    snapshot_cap_set,
                ]: &mut [_; 4] = caps.try_into().unwrap();

                let id = config.id;
                let worker_id = config.worker_id;

                if !all_outputs.is_empty() {
                    // A worker *must* emit a count even if not responsible for snapshotting a table
                    // as statistic summarization will return null if any worker hasn't set a value.
                    // This will also reset snapshot stats for any exports not snapshotting.
                    for statistics in config.statistics.values() {
                        statistics.set_snapshot_records_known(0);
                        statistics.set_snapshot_records_staged(0);
                    }
                }

                // If this worker has no tables to snapshot then there is nothing to do.
                if reader_snapshot_table_info.is_empty() {
                    trace!(%id, "timely-{worker_id} initializing table reader \
                                 with no tables to snapshot, exiting");
                    return Ok(());
                } else {
                    trace!(%id, "timely-{worker_id} initializing table reader \
                                 with {} tables to snapshot",
                           reader_snapshot_table_info.len());
                }

                let connection_config = connection
                    .connection
                    .config(
                        &config.config.connection_context.secrets_reader,
                        &config.config,
                        InTask::Yes,
                    )
                    .await?;
                let task_name = format!("timely-{worker_id} MySQL snapshotter");

                // Phase B: Leader acquires locks, reads GTID, queries PK bounds, broadcasts.
                //
                // All fallible leader work is wrapped in an inner async block so that
                // we ALWAYS broadcast a result (success or failure) before returning.
                // Without this, a leader error would drop `snapshot_cap_set` without
                // broadcasting, causing non-leader workers to deadlock waiting for
                // `SnapshotInfo` on the feedback loop.
                let mut lock_conn = if is_snapshot_leader {
                    let leader_result: Result<_, SnapshotSetupError> = async {
                        let lock_clauses = reader_snapshot_table_info
                            .keys()
                            .map(|t| format!("{} READ", t))
                            .collect::<Vec<String>>()
                            .join(", ");
                        let mut lock_conn = connection_config
                            .connect(
                                &task_name,
                                &config.config.connection_context.ssh_tunnel_manager,
                            )
                            .await?;

                        // Compute PK-range split boundaries for each table BEFORE
                        // acquiring the lock. Supported PKs sample boundaries with
                        // one PK-index scan, which is O(rows). Doing this work
                        // before `LOCK TABLES` keeps the lock-hold window O(1) rather
                        // than scaling with table size. Boundaries need not match the
                        // exact snapshot point: they only partition the key domain and
                        // stay correct for any data distribution (see `PkSplits`).
                        // `None` means single-worker fallback. Sampled concurrently across a
                        // pool of `worker_count` connections (leader-local; splits are still
                        // broadcast to all workers).
                        let mut pk_bounds_map: BTreeMap<MySqlTableName, Option<PkSplits>> =
                            BTreeMap::new();
                        // Held (each in a transaction holding a shared metadata lock on its
                        // table) until after `LOCK TABLES`, to bridge DDL between sampling and
                        // the read. Declared here so it outlives the branch below.
                        let mut probe_conns = Vec::new();
                        if config.worker_count < 2 {
                            // A single worker has nothing to split, so skip the probes
                            // and fall back to single-worker mode for every table.
                            for table in reader_snapshot_table_info.keys() {
                                pk_bounds_map.insert(table.clone(), None);
                            }
                        } else {
                            // Tables without a supported PK fall back to single-worker
                            // (`None`); the rest go into a shared queue drained by a pool
                            // of up to `worker_count` connections.
                            let mut queue = Vec::new();
                            for (table, outputs) in &reader_snapshot_table_info {
                                match formattable_pk(&outputs[0].desc) {
                                    Some(pk_col) => queue.push((table.clone(), pk_col)),
                                    None => {
                                        pk_bounds_map.insert(table.clone(), None);
                                    }
                                }
                            }

                            let connection_config = &connection_config;
                            let task_name = task_name.as_str();
                            let ssh_tunnel_manager =
                                &config.config.connection_context.ssh_tunnel_manager;
                            let worker_count = config.worker_count;
                            // Cooperative single-task futures, so `Rc<RefCell<_>>` is safe
                            // (no borrow is held across an `.await`).
                            let queue = Rc::new(RefCell::new(queue));
                            let pool = (0..worker_count).map(|_| {
                                let queue = Rc::clone(&queue);
                                async move {
                                    // Connect lazily so a pool member that draws no work opens
                                    // nothing; the transaction holds the metadata lock (above).
                                    let mut conn = None;
                                    let mut results = Vec::new();
                                    loop {
                                        let Some((table, pk_col)) = queue.borrow_mut().pop() else {
                                            break;
                                        };
                                        if conn.is_none() {
                                            let mut c = connection_config
                                                .connect(task_name, ssh_tunnel_manager)
                                                .await?;
                                            // static SQL string
                                            #[allow(clippy::disallowed_methods)]
                                            c.query_drop("START TRANSACTION READ ONLY").await?;
                                            conn = Some(c);
                                        }
                                        let c = conn.as_mut().expect("connected above");
                                        let splits = compute_sampled_splits(
                                            &mut **c,
                                            &table,
                                            &pk_col,
                                            worker_count,
                                        )
                                        .await?;
                                        results.push((table, splits));
                                    }
                                    Ok::<_, SnapshotSetupError>((results, conn))
                                }
                            });
                            let per_worker = futures::future::try_join_all(pool).await?;
                            for (results, conn) in per_worker {
                                for (table, splits) in results {
                                    pk_bounds_map.insert(table, splits);
                                }
                                // 0 or 1 conn: pool members that drew no work hold none.
                                probe_conns.extend(conn);
                            }
                        }

                        trace!(%id, "timely-{worker_id} acquiring table locks: {lock_clauses}");
                        let snapshot_gtid_set = lock_tables_and_read_gtid_set(
                            &mut lock_conn,
                            &lock_clauses,
                            config
                                .config
                                .parameters
                                .mysql_source_timeouts
                                .snapshot_lock_wait_timeout,
                        )
                        .await?;

                        // Read lock now blocks DDL, so release the probe connections/locks.
                        drop(probe_conns);

                        trace!(%id, "timely-{worker_id} acquired table locks");

                        let snapshot_info = SnapshotInfo {
                            gtid_set: snapshot_gtid_set,
                            pk_bounds: pk_bounds_map,
                        };
                        Ok((snapshot_info, lock_conn))
                    }
                    .await;

                    match leader_result {
                        Ok((info, conn)) => {
                            trace!(%id, "timely-{worker_id} broadcasting snapshot info: {info:?}");
                            snapshot_handle.give(&snapshot_cap_set[0], Some(info));
                            Some(conn)
                        }
                        Err(err) => {
                            // CRITICAL: broadcast None so non-leaders exit cleanly
                            // instead of deadlocking on the feedback loop.
                            trace!(%id, "timely-{worker_id} leader failed, broadcasting \
                                         error sentinel");
                            snapshot_handle.give(&snapshot_cap_set[0], None);
                            match err {
                                SnapshotSetupError::Definite(e) => {
                                    return Ok(return_definite_error(
                                        e,
                                        &all_outputs,
                                        &raw_handle,
                                        data_cap_set,
                                        &definite_error_handle,
                                        definite_error_cap_set,
                                    )
                                    .await);
                                }
                                SnapshotSetupError::Transient(e) => {
                                    return Err(e);
                                }
                            }
                        }
                    }
                } else {
                    None
                };

                // Phase C: All workers receive broadcast.
                // The payload is `Option<SnapshotInfo>`: `Some` on success,
                // `None` if the leader encountered an error.
                let snapshot_info: Option<SnapshotInfo> = 'recv: loop {
                    match snapshot_input.next().await {
                        Some(AsyncEvent::Data(_, mut data)) => {
                            if let Some(msg) = data.pop() {
                                break 'recv msg;
                            }
                        }
                        Some(AsyncEvent::Progress(_)) => continue,
                        None => {
                            // Feedback stream closed without data — the leader
                            // must have failed. Return cleanly; the leader's
                            // operator instance handles error propagation.
                            break 'recv None;
                        }
                    }
                };
                let snapshot_info = match snapshot_info {
                    Some(info) => info,
                    None => {
                        // Leader signaled failure. Bail out — errors are
                        // already propagated by the leader's worker.
                        return Ok(());
                    }
                };

                // Parse GTID frontier from snapshot_info.gtid_set
                let snapshot_gtid_frontier = match gtid_set_frontier(&snapshot_info.gtid_set) {
                    Ok(frontier) => frontier,
                    Err(err) => {
                        let err = DefiniteError::UnsupportedGtidState(err.to_string());
                        return Ok(return_definite_error(
                            err,
                            &all_outputs,
                            &raw_handle,
                            data_cap_set,
                            &definite_error_handle,
                            definite_error_cap_set,
                        )
                        .await);
                    }
                };

                trace!(%id, "timely-{worker_id} received snapshot info at: {}",
                       snapshot_gtid_frontier.pretty());

                // Precompute each table's read plan for this worker. A
                // partitioned table's ranges are assigned round-robin from the
                // table's legacy owner, so each range is read by exactly one
                // worker and no row is read twice. The owner always takes
                // partition 0, so it always has work and emits the rewind below.
                // An unpartitioned table is read whole by its responsible worker.
                let table_ranges: BTreeMap<_, ReadPlan> = reader_snapshot_table_info
                    .keys()
                    .filter_map(|table| {
                        let plan = match snapshot_info.pk_bounds.get(table) {
                            Some(Some(splits)) => {
                                let partition = partition_for_worker(
                                    config.worker_id,
                                    config.responsible_worker(table),
                                    config.worker_count,
                                );
                                worker_pk_range(splits, partition).map(ReadPlan::Range)
                            }
                            _ => config
                                .responsible_for(table)
                                .then_some(ReadPlan::WholeTable),
                        };
                        plan.map(|plan| (table.clone(), plan))
                    })
                    .collect();
                let has_work = !table_ranges.is_empty();

                // Non-leader workers that have nothing to read skip
                // connecting — avoids exhausting MySQL's connection
                // pool in many-sources scenarios (limits test).
                let mut conn = if is_snapshot_leader || has_work {
                    let mut c = connection_config
                        .connect(
                            &task_name,
                            &config.config.connection_context.ssh_tunnel_manager,
                        )
                        .await?;
                    match validate_mysql_repl_settings(&mut c).await {
                        Err(err @ MySqlError::InvalidSystemSetting { .. }) => {
                            return Ok(return_definite_error(
                                DefiniteError::ServerConfigurationError(err.to_string()),
                                &all_outputs,
                                &raw_handle,
                                data_cap_set,
                                &definite_error_handle,
                                definite_error_cap_set,
                            )
                            .await);
                        }
                        Err(err) => Err(err)?,
                        Ok(()) => (),
                    };
                    Some(c)
                } else {
                    trace!(%id, "timely-{worker_id} has no tables to read, \
                                 skipping MySQL connection");
                    None
                };

                let mut tx = if let Some(ref mut conn) = conn {
                    trace!(%id, "timely-{worker_id} starting transaction with \
                                 consistent snapshot at: {}", snapshot_gtid_frontier.pretty());
                    let mut tx_opts = TxOpts::default();
                    tx_opts
                        .with_isolation_level(IsolationLevel::RepeatableRead)
                        .with_consistent_snapshot(true)
                        .with_readonly(true);
                    let mut tx = conn.start_transaction(tx_opts).await?;
                    // Set the session time zone to UTC so we read TIMESTAMP columns as UTC.
                    #[allow(clippy::disallowed_methods)] // static SQL string
                    tx.query_drop("set @@session.time_zone = '+00:00'").await?;
                    if let Some(timeout) = config
                        .config
                        .parameters
                        .mysql_source_timeouts
                        .snapshot_max_execution_time
                    {
                        // Interpolating an integer millis value; not parameterizable in MySQL `SET`.
                        #[allow(clippy::disallowed_methods)]
                        tx.query_drop(format!(
                            "SET @@session.max_execution_time = {}",
                            timeout.as_millis()
                        ))
                        .await?;
                    }
                    Some(tx)
                } else {
                    None
                };

                // Phase E: All workers signal, leader unlocks
                *snapshot_cap_set = CapabilitySet::new();
                if is_snapshot_leader {
                    while snapshot_input.next().await.is_some() {}
                    if let Some(mut lc) = lock_conn.take() {
                        #[allow(clippy::disallowed_methods)] // static SQL string
                        lc.query_drop("UNLOCK TABLES").await?;
                        lc.disconnect().await?;
                    }
                }
                drop(lock_conn);

                trace!(%id, "timely-{worker_id} started transaction (has_work={has_work})");

                // Workers without a transaction have nothing to read.
                let Some(ref mut tx) = tx else {
                    return Ok(());
                };

                // Phase F: Verify schemas for the tables this worker reads. In
                // PK-range mode several workers read the same table, so each
                // verifies independently to learn which outputs to skip; only
                // the responsible worker publishes any resulting error (below).
                let errored_outputs = verify_schemas(
                    tx,
                    reader_snapshot_table_info
                        .iter()
                        .filter(|(t, _)| table_ranges.contains_key(t))
                        .map(|(k, v)| (k, v.as_slice()))
                        .collect(),
                )
                .await?;
                let mut removed_outputs = BTreeSet::new();
                for (output, err) in errored_outputs {
                    // Every worker reading this table must stop ingesting the output, but
                    // only the responsible worker publishes the error so it lands with
                    // multiplicity one — other workers read disjoint PK ranges of the same
                    // table and would otherwise each emit the same error.
                    if config.responsible_for(&output.table_name) {
                        let update = (
                            (output.output_index, Err(err.clone().into())),
                            GtidPartition::minimum(),
                            Diff::ONE,
                        );
                        let size = update.fuel_size();
                        raw_handle.give_fueled(&data_cap_set[0], update, size).await;
                        tracing::warn!(%id, "timely-{worker_id} stopping snapshot of output \
                                    {output:?} due to schema mismatch");
                    }
                    removed_outputs.insert(output.output_index);
                }
                for (_, outputs) in reader_snapshot_table_info.iter_mut() {
                    outputs.retain(|output| !removed_outputs.contains(&output.output_index));
                }
                reader_snapshot_table_info.retain(|_, outputs| !outputs.is_empty());

                // Phase G: The leader publishes the full snapshot size so the summed
                // worker-local gauges reflect the upstream total without double-counting.
                if is_snapshot_leader {
                    fetch_snapshot_size(
                        tx,
                        reader_snapshot_table_info
                            .iter()
                            .map(|(name, outputs)| {
                                let stats = export_statistics
                                    .get(name)
                                    .expect("statistics are initialized for each output");
                                (name.clone(), outputs.len(), stats)
                            })
                            .collect(),
                        metrics,
                    )
                    .await?;
                }

                if reader_snapshot_table_info.is_empty() {
                    return Ok(());
                }

                // Phase H: Read snapshot data
                let mut final_row = Row::default();

                // Yield more frequently when multiple workers are
                // active to keep total in-flight memory bounded.
                // ~130 bytes/row × 10K rows ≈ 1.3 MiB per yield.
                // The trailing `.max(1)` keeps the interval positive even for
                // pathological worker counts (> 10K), avoiding a `% 0` panic.
                let yield_interval = (10_000 / u64::cast_from(config.worker_count).max(1)).max(1);

                let mut snapshot_staged_total = 0;
                for (table, outputs) in &reader_snapshot_table_info {
                    let pk_range = match table_ranges.get(table) {
                        Some(ReadPlan::Range(range)) => Some(range),
                        Some(ReadPlan::WholeTable) => None,
                        // This worker has no work for this table.
                        None => continue,
                    };

                    let mut snapshot_staged = 0;
                    let query = build_snapshot_query(outputs, pk_range);
                    trace!(%id, "timely-{worker_id} reading snapshot query='{}'", query);
                    let mut results = tx.exec_stream(query, ()).await?;
                    while let Some(row) = results.try_next().await? {
                        let row: MySqlRow = row;
                        snapshot_staged += 1;
                        for (output, row_val) in outputs.iter().repeat_clone(row) {
                            // We don't need to verify if binlog_row_metadata matches the expected when snapshotting as
                            // the snapshot query always returns rows with full metadata. If the output is configured
                            // with binlog_full_metadata = false, then we will just ignore the metadata when decoding.
                            let event = match pack_mysql_row(
                                &mut final_row,
                                row_val,
                                &output.desc,
                                None,
                                output.binlog_full_metadata,
                            ) {
                                Ok(row) => Ok(SourceMessage {
                                    key: Row::default(),
                                    value: row,
                                    metadata: Row::default(),
                                }),
                                // Produce a DefiniteError in the stream for any rows that fail to decode
                                Err(err @ MySqlError::ValueDecodeError { .. }) => {
                                    Err(DataflowError::from(DefiniteError::ValueDecodeError(
                                        err.to_string(),
                                    )))
                                }
                                Err(err) => Err(err)?,
                            };
                            let update = (
                                (output.output_index, event),
                                GtidPartition::minimum(),
                                Diff::ONE,
                            );
                            let size = update.fuel_size();
                            raw_handle.give_fueled(&data_cap_set[0], update, size).await;
                        }
                        snapshot_staged_total += u64::cast_from(outputs.len());
                        if snapshot_staged_total % yield_interval == 0 {
                            tokio::task::yield_now().await;
                        }
                        if snapshot_staged_total % 1000 == 0 {
                            if let Some(stats_list) = export_statistics.get(table) {
                                for statistics in stats_list {
                                    statistics.set_snapshot_records_staged(snapshot_staged);
                                }
                            }
                        }
                    }
                    if let Some(stats_list) = export_statistics.get(table) {
                        for statistics in stats_list {
                            statistics.set_snapshot_records_staged(snapshot_staged);
                        }
                    }
                    trace!(%id, "timely-{worker_id} snapshotted {} records from \
                                 table '{table}'", snapshot_staged * u64::cast_from(outputs.len()));
                }

                // Phase I: Emit rewind requests
                // We are done with the snapshot so now we will emit rewind requests. It is
                // important that this happens after the snapshot has finished because this is what
                // unblocks the replication operator and we want this to happen serially.
                //
                // Only the responsible worker emits rewind requests for each
                // table. It is the table's legacy owner, which always owns
                // partition 0 (or reads the whole unpartitioned table), so it is
                // guaranteed to have a transaction (has_work = true).
                for (table, outputs) in &reader_snapshot_table_info {
                    if !config.responsible_for(table) {
                        continue;
                    }
                    for output in outputs {
                        trace!(%id, "timely-{worker_id} producing rewind request for {table}\
                                     output {}", output.output_index);
                        let req = RewindRequest {
                            output_index: output.output_index,
                            snapshot_upper: snapshot_gtid_frontier.clone(),
                        };
                        rewinds_handle.give(&rewind_cap_set[0], req);
                    }
                }
                *rewind_cap_set = CapabilitySet::new();

                Ok(())
            }))
        });

    // TODO: Split row decoding into a separate operator that can be distributed across all workers

    let errors = definite_errors.concat(transient_errors.map(ReplicationError::from));

    (
        raw_data.as_collection(),
        rewinds,
        errors,
        button.press_on_drop(),
    )
}

/// Fetch the size of the snapshot on this worker and emits the appropriate emtrics and statistics
/// for each table.
async fn fetch_snapshot_size<Q>(
    conn: &mut Q,
    tables: Vec<(MySqlTableName, usize, &Vec<SourceStatistics>)>,
    metrics: MySqlSnapshotMetrics,
) -> Result<u64, anyhow::Error>
where
    Q: Queryable,
{
    let mut total = 0;
    for (table, num_outputs, export_statistics) in tables {
        let stats = collect_table_statistics(conn, &table).await?;
        metrics.record_table_count_latency(table.1, table.0, stats.count_latency);
        for export_stat in export_statistics {
            export_stat.set_snapshot_records_known(stats.count);
            export_stat.set_snapshot_records_staged(0);
        }
        total += stats.count * u64::cast_from(num_outputs);
    }
    Ok(total)
}

enum SnapshotSetupError {
    Definite(DefiniteError),
    Transient(TransientError),
}

impl From<mysql_async::Error> for SnapshotSetupError {
    fn from(e: mysql_async::Error) -> Self {
        SnapshotSetupError::Transient(e.into())
    }
}

impl From<MySqlError> for SnapshotSetupError {
    fn from(e: MySqlError) -> Self {
        SnapshotSetupError::Transient(e.into())
    }
}

fn classify_query_error(e: mysql_async::Error) -> SnapshotSetupError {
    match e {
        mysql_async::Error::Server(mysql_async::ServerError { code, message, .. })
            if code == ER_NO_SUCH_TABLE =>
        {
            SnapshotSetupError::Definite(DefiniteError::TableDropped(message))
        }
        e => SnapshotSetupError::Transient(e.into()),
    }
}

async fn lock_tables_and_read_gtid_set(
    lock_conn: &mut MySqlConn,
    lock_clauses: &str,
    lock_wait_timeout: Option<Duration>,
) -> Result<String, SnapshotSetupError> {
    if let Some(timeout) = lock_wait_timeout {
        // Interpolating a `Duration` integer; not parameterizable in MySQL `SET`.
        #[allow(clippy::disallowed_methods)]
        lock_conn
            .query_drop(format!(
                "SET @@session.lock_wait_timeout = {}",
                timeout.as_secs()
            ))
            .await?;
    }

    // `lock_clauses` is built from `MySqlTableName::Display`, which escapes both
    // schema and table via `quote_identifier`.
    #[allow(clippy::disallowed_methods)]
    lock_conn
        .query_drop(format!("LOCK TABLES {lock_clauses}"))
        .await
        .map_err(classify_query_error)?;

    let snapshot_gtid_set = query_sys_var(lock_conn, "global.gtid_executed").await?;
    Ok(snapshot_gtid_set)
}

/// Builds the SQL query to be used for creating the snapshot using the first entry in outputs.
///
/// Expect `outputs` to contain entries for a single table, and to have at least 1 entry.
/// Expect that each MySqlTableDesc entry contains all columns described in information_schema.columns.
///
/// When `pk_range` is provided, a WHERE clause is appended to filter rows by PK range.
#[must_use]
fn build_snapshot_query(outputs: &[SourceOutputInfo], pk_range: Option<&PkRange>) -> String {
    let info = outputs.first().expect("MySQL table info");
    for output in &outputs[1..] {
        // the columns may be decoded based on position, and different outputs may replicate
        // different columns, so we need to ensure that all columns are accounted for.
        assert!(
            info.desc.columns.len() == output.desc.columns.len(),
            "Mismatch in table descriptions for {}",
            info.table_name
        );
    }
    let columns = info
        .desc
        .columns
        .iter()
        .map(|col| quote_identifier(&col.name))
        .join(", ");
    let mut query = format!("SELECT {} FROM {}", columns, info.table_name);
    if let Some(range) = pk_range {
        // Half-open range on the PK column. The first/last partition omits its
        // open bound.
        let col = &range.pk_col;
        if let Some(lower) = &range.lower {
            query.push_str(&format!(" WHERE {col} >= {lower}"));
        }
        if let Some(upper) = &range.upper {
            let kw = if range.lower.is_some() {
                "AND"
            } else {
                "WHERE"
            };
            query.push_str(&format!(" {kw} {col} < {upper}"));
        }
    }
    query
}

#[derive(Default)]
struct TableStatistics {
    count_latency: f64,
    count: u64,
}

async fn collect_table_statistics<Q>(
    conn: &mut Q,
    table: &MySqlTableName,
) -> Result<TableStatistics, anyhow::Error>
where
    Q: Queryable,
{
    let mut stats = TableStatistics::default();

    // `MySqlTableName::Display` escapes both identifier components via
    // `quote_identifier`, so this interpolation is safe; not parameterizable.
    #[allow(clippy::disallowed_methods)]
    let count_row: Option<u64> = conn
        .query_first(format!("SELECT COUNT(*) FROM {}", table))
        .wall_time()
        .set_at(&mut stats.count_latency)
        .await?;
    stats.count = count_row.ok_or_else(|| anyhow::anyhow!("failed to COUNT(*) {table}"))?;

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_mysql_util::{MySqlColumnDesc, MySqlTableDesc};
    use timely::progress::Antichain;

    #[mz_ore::test]
    fn snapshot_query_duplicate_table() {
        let schema_name = "myschema".to_string();
        let table_name = "mytable".to_string();
        let table = MySqlTableName(schema_name.clone(), table_name.clone());
        let columns = ["c1", "c2", "c3"]
            .iter()
            .map(|col| MySqlColumnDesc {
                name: col.to_string(),
                column_type: None,
                meta: None,
            })
            .collect::<Vec<_>>();
        let desc = MySqlTableDesc {
            schema_name: schema_name.clone(),
            name: table_name.clone(),
            columns,
            keys: BTreeSet::default(),
        };
        let info = SourceOutputInfo {
            output_index: 1, // ignored
            table_name: table.clone(),
            desc,
            text_columns: vec![],
            exclude_columns: vec![],
            initial_gtid_set: Antichain::default(),
            resume_upper: Antichain::default(),
            export_id: mz_repr::GlobalId::User(1),
            binlog_full_metadata: false,
        };
        let query = build_snapshot_query(&[info.clone(), info], None);
        assert_eq!(
            format!(
                "SELECT `c1`, `c2`, `c3` FROM `{}`.`{}`",
                &schema_name, &table_name
            ),
            query
        );
    }

    #[mz_ore::test]
    fn snapshot_query_with_pk_range() {
        let schema_name = "myschema".to_string();
        let table_name = "mytable".to_string();
        let table = MySqlTableName(schema_name.clone(), table_name.clone());
        let columns = ["id", "name"]
            .iter()
            .map(|col| MySqlColumnDesc {
                name: col.to_string(),
                column_type: None,
                meta: None,
            })
            .collect::<Vec<_>>();
        let desc = MySqlTableDesc {
            schema_name: schema_name.clone(),
            name: table_name.clone(),
            columns,
            keys: BTreeSet::default(),
        };
        let info = SourceOutputInfo {
            output_index: 1,
            table_name: table.clone(),
            desc,
            text_columns: vec![],
            exclude_columns: vec![],
            initial_gtid_set: Antichain::default(),
            resume_upper: Antichain::default(),
            export_id: mz_repr::GlobalId::User(1),
            binlog_full_metadata: false,
        };

        // Middle worker: both bounds.
        let range = PkRange {
            pk_col: "`id`".to_string(),
            lower: Some("100".to_string()),
            upper: Some("200".to_string()),
        };
        let query = build_snapshot_query(std::slice::from_ref(&info), Some(&range));
        assert_eq!(
            format!(
                "SELECT `id`, `name` FROM `{}`.`{}` WHERE `id` >= 100 AND `id` < 200",
                &schema_name, &table_name
            ),
            query
        );

        // First worker: open start.
        let range = PkRange {
            pk_col: "`id`".to_string(),
            lower: None,
            upper: Some("200".to_string()),
        };
        let query = build_snapshot_query(std::slice::from_ref(&info), Some(&range));
        assert_eq!(
            format!(
                "SELECT `id`, `name` FROM `{}`.`{}` WHERE `id` < 200",
                &schema_name, &table_name
            ),
            query
        );

        // Last worker: open end.
        let range = PkRange {
            pk_col: "`id`".to_string(),
            lower: Some("200".to_string()),
            upper: None,
        };
        let query = build_snapshot_query(std::slice::from_ref(&info), Some(&range));
        assert_eq!(
            format!(
                "SELECT `id`, `name` FROM `{}`.`{}` WHERE `id` >= 200",
                &schema_name, &table_name
            ),
            query
        );
    }

    #[mz_ore::test]
    fn test_worker_pk_range() {
        // Two partitions, boundary at 51.
        let splits = PkSplits {
            pk_col: "`id`".to_string(),
            boundaries: vec!["51".to_string()],
        };
        let r0 = worker_pk_range(&splits, 0).expect("worker 0");
        assert_eq!(r0.pk_col, "`id`");
        assert_eq!(r0.lower, None); // open start
        assert_eq!(r0.upper.as_deref(), Some("51"));
        let r1 = worker_pk_range(&splits, 1).expect("worker 1");
        assert_eq!(r1.lower.as_deref(), Some("51"));
        assert_eq!(r1.upper, None); // open end
        // Beyond the partition count → no work.
        assert!(worker_pk_range(&splits, 2).is_none());

        // Three partitions: the middle worker has both bounds.
        let splits = PkSplits {
            pk_col: "`id`".to_string(),
            boundaries: vec!["34".to_string(), "67".to_string()],
        };
        let r1 = worker_pk_range(&splits, 1).expect("worker 1");
        assert_eq!(r1.lower.as_deref(), Some("34"));
        assert_eq!(r1.upper.as_deref(), Some("67"));
    }

    #[mz_ore::test]
    fn test_partition_for_worker() {
        // The owner takes partition 0; the remaining workers follow round-robin.
        assert_eq!(partition_for_worker(2, 2, 4), 0);
        assert_eq!(partition_for_worker(3, 2, 4), 1);
        assert_eq!(partition_for_worker(0, 2, 4), 2);
        assert_eq!(partition_for_worker(1, 2, 4), 3);
        // Owner 0 is the identity mapping (worker id == partition).
        assert_eq!(partition_for_worker(0, 0, 4), 0);
        assert_eq!(partition_for_worker(3, 0, 4), 3);
        // Every worker maps to a distinct partition, so ranges never overlap.
        let owner = 3;
        let mut seen: Vec<_> = (0..5).map(|w| partition_for_worker(w, owner, 5)).collect();
        seen.sort();
        assert_eq!(seen, vec![0, 1, 2, 3, 4]);
    }

    #[mz_ore::test]
    fn test_formattable_pk() {
        use mz_mysql_util::MySqlKeyDesc;
        use mz_repr::SqlColumnType;

        let col = |name: &str, ty: SqlScalarType| MySqlColumnDesc {
            name: name.to_string(),
            column_type: Some(SqlColumnType {
                scalar_type: ty,
                nullable: false,
            }),
            meta: None,
        };
        let pk = |cols: &[&str]| {
            BTreeSet::from([MySqlKeyDesc {
                name: "PRIMARY".to_string(),
                is_primary: true,
                columns: cols.iter().map(|c| c.to_string()).collect(),
            }])
        };
        let desc = |columns, keys| MySqlTableDesc {
            schema_name: "s".to_string(),
            name: "t".to_string(),
            columns,
            keys,
        };

        // Single char PK.
        let (name, kind) = formattable_pk(&desc(
            vec![col("id", SqlScalarType::Char { length: None })],
            pk(&["id"]),
        ))
        .expect("char pk supported");
        assert_eq!(name, "`id`");
        assert!(matches!(kind, PkColKind::Text));

        // Composite PK → not supported, fall back.
        assert!(
            formattable_pk(&desc(
                vec![
                    col("a", SqlScalarType::Char { length: None }),
                    col("b", SqlScalarType::Int64),
                ],
                pk(&["a", "b"]),
            ))
            .is_none()
        );

        // Unsupported column type in the PK → fall back.
        assert!(
            formattable_pk(&desc(vec![col("id", SqlScalarType::Bytes)], pk(&["id"]))).is_none()
        );

        // No primary key → fall back.
        assert!(
            formattable_pk(&desc(
                vec![col("id", SqlScalarType::Int64)],
                BTreeSet::default()
            ))
            .is_none()
        );
    }
}
