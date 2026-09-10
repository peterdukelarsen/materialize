// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Upstream constraint changes that Materialize cannot follow.

use mz_ore::str::StrExt;
use postgres_protocol::escape;
use serde::{Deserialize, Serialize};

use crate::desc::{SqlServerTableConstraint, SqlServerTableConstraintType};

/// An upstream constraint change that Materialize cannot follow.
///
/// `Display` renders the diagnosis. [`SchemaChangeError::hint`] renders the
/// recovery steps, which are surfaced separately: as the `HINT` of a SQL error
/// and in the source status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("incompatible schema change on {schema_name}.{name}: {change}")]
pub struct SchemaChangeError {
    pub schema_name: String,
    pub name: String,
    pub change: SchemaChange,
}

/// The upstream change behind a [`SchemaChangeError`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum SchemaChange {
    #[error("{key} was dropped upstream")]
    KeyDropped { key: KeyRef },
    #[error("{key} was altered upstream")]
    KeyAltered { key: KeyRef },
}

/// The constraint a [`SchemaChange`] is about, as it was recorded when the
/// export was created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRef {
    pub name: String,
    pub is_primary: bool,
    pub columns: Vec<String>,
}

impl From<&SqlServerTableConstraint> for KeyRef {
    fn from(constraint: &SqlServerTableConstraint) -> Self {
        KeyRef {
            name: constraint.constraint_name.clone(),
            is_primary: matches!(
                constraint.constraint_type,
                SqlServerTableConstraintType::PrimaryKey
            ),
            columns: constraint.column_names.clone(),
        }
    }
}

impl std::fmt::Display for KeyRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = if self.is_primary {
            "PRIMARY KEY"
        } else {
            "UNIQUE"
        };
        write!(
            f,
            "{kind} constraint {} ({})",
            self.name.quoted(),
            self.columns.join(", ")
        )
    }
}

impl SchemaChangeError {
    /// The recovery steps for the dropped constraint, including the statements
    /// to run.
    pub fn hint(&self) -> String {
        let recreate = format!(
            "To keep ingesting without this constraint, recreate the table in a new \
             versioned schema, then swap your views to the new table:\n  CREATE SCHEMA v2;\n  \
             CREATE TABLE v2.{}\n  FROM SOURCE <source> (REFERENCE {}.{});",
            escape::escape_identifier(&self.name),
            escape::escape_identifier(&self.schema_name),
            escape::escape_identifier(&self.name),
        );
        match &self.change {
            SchemaChange::KeyDropped { key } | SchemaChange::KeyAltered { key } => format!(
                "{}\nTo make a planned constraint drop a non-event, create the table with \
                 WITH (EXCLUDE CONSTRAINTS ({})) before the upstream drop.",
                recreate,
                escape::escape_literal(&key.name),
            ),
        }
    }
}
