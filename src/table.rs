use std::fmt::{self, Display};

use crate::error::ParseError;
use serde::{Deserialize, Serialize};
use sqlparser::ast;
use utoipa::{IntoParams, ToSchema};

use super::{internal, unsupported};

use super::support::case_fold_identifier;

pub(crate) struct TableIdentWithAlias(pub TabIdent, pub Option<String>);

impl TableIdentWithAlias {
    pub(crate) fn extract(from: &[ast::TableWithJoins]) -> Result<Self, ParseError> {
        let multi_tables = || {
            Err(unsupported!("the FROM clause has multiple tables \
                         (no JOINs, subqueries or functions allowed)."
                .to_string()))
        };

        let relation = match from {
            [ast::TableWithJoins { relation, joins }] if joins.is_empty() => relation,
            _ => return multi_tables(),
        };

        match relation {
            ast::TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                partitions,
            } => {
                if args.is_some() {
                    return multi_tables();
                }
                if !with_hints.is_empty() {
                    return Err(unsupported!(
                        "table hints (WITH in FROM clauses).".to_string()
                    ));
                }
                if version.is_some() {
                    return Err(unsupported!("version qualifier.".to_string()));
                }
                if !partitions.is_empty() {
                    return Err(unsupported!("table partitions.".to_string()));
                }
                let table = TabIdent::from_object_name(name)?;
                let alias = alias
                    .as_ref()
                    .map(|alias| {
                        let ast::TableAlias { name, columns } = alias;
                        if columns.is_empty() {
                            Ok(case_fold_identifier(name))
                        } else {
                            Err(unsupported!(format!(
                                "table aliases with columns (such as {alias})."
                            )))
                        }
                    })
                    .transpose()?;
                Ok(Self(table, alias))
            }
            ast::TableFactor::Derived { .. }
            | ast::TableFactor::TableFunction { .. }
            | ast::TableFactor::UNNEST { .. }
            | ast::TableFactor::NestedJoin { .. }
            | ast::TableFactor::Pivot { .. }
            | ast::TableFactor::Function { .. }
            | ast::TableFactor::JsonTable { .. }
            | ast::TableFactor::Unpivot { .. } => multi_tables(),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, Default, ToSchema, IntoParams)]
pub struct TabIdent {
    pub db: Option<String>,
    pub schema: Option<String>,
    pub table: String,
}

impl TabIdent {
    fn from_object_name(object_name: &ast::ObjectName) -> Result<Self, ParseError> {
        let ast::ObjectName(name_parts) = object_name;
        match &name_parts[..] {
            [] => Err(internal!(
                "found empty table name (ObjectName) in query AST.".to_string()
            )),
            [table] => Ok(Self {
                db: None,
                schema: None,
                table: case_fold_identifier(table),
            }),
            [schema, table] => Ok(Self {
                db: None,
                schema: Some(case_fold_identifier(schema)),
                table: case_fold_identifier(table),
            }),
            [db, schema, table] => Ok(Self {
                db: Some(case_fold_identifier(db)),
                schema: Some(case_fold_identifier(schema)),
                table: case_fold_identifier(table),
            }),
            [..] => Err(internal!(format!(
                "found too many ident in table name (i.e., {object_name}) in query AST."
            ))),
        }
    }

    #[must_use]
    pub fn into_object_name(&self, quote_style: Option<char>) -> ast::ObjectName {
        let mut objects = vec![];
        if let Some(db) = self.db.clone() {
            objects.push(ast::Ident {
                value: db,
                quote_style,
            });
        }
        if let Some(schema) = self.schema.clone() {
            objects.push(ast::Ident {
                value: schema,
                quote_style,
            });
        }
        objects.push(ast::Ident {
            value: self.table.clone(),
            quote_style,
        });
        ast::ObjectName(objects)
    }
}

impl Display for TabIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.db, &self.schema, &self.table) {
            (Some(db), Some(schema), table_name) => {
                write!(f, "{db}.{schema}.{table_name}")
            }
            (None, Some(schema), table_name) => {
                write!(f, "{schema}.{table_name}")
            }
            (Some(db), None, table_name) => {
                write!(f, "{db}.{table_name}")
            }
            (None, None, table_name) => {
                write!(f, "{table_name}")
            }
        }
    }
}
