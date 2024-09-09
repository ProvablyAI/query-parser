use std::fmt::{self, Display};

use serde::{Deserialize, Serialize};
use sqlparser::{ast, dialect::GenericDialect, parser::Parser};
use utoipa::{IntoParams, ToSchema};

use crate::{
    aggregation::Aggregation,
    destructured_query::DestructuredQuery,
    error::ParseError,
    filter::{Filter, FilterExtractor},
    support::case_fold_identifier,
    table::{TabIdent, TableIdentWithAlias},
    unsupported,
};

/// QueryMetadata extracted from the query.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default, ToSchema, IntoParams)]
pub struct QueryMetadata {
    /// Aggregation performed.
    pub aggregation: Aggregation,
    /// Table subject to query.
    pub table: TabIdent,
    /// Filter applied.
    pub filter: Option<Filter>,
}

impl QueryMetadata {
    /// Generates `QueryMetadata` from a SQL query using [`crate::config::Config`].
    pub fn parse(sql_query: &str) -> Result<Self, ParseError> {
        //extract all the statement from the sql query.
        let statements = Parser::parse_sql(&GenericDialect {}, sql_query)?;
        //check if the sql query is: single, and is a select.
        let statement = Self::extract_select_query(&statements)?;
        //check and extract query clauses from statement
        let DestructuredQuery {
            projection,
            from,
            selection,
        } = DestructuredQuery::destructure(statement)?;
        //check and extract table informations from FROM clause
        let TableIdentWithAlias(table_name, table_alias) = TableIdentWithAlias::extract(from)?;
        //extract table name to be used in the SELECT clause
        let from_clause_identifier = table_alias.as_deref().map_or_else(
            || FromClauseIdentifier::Base(&table_name),
            |x| FromClauseIdentifier::Alias { alias: x },
        );

        //extract analytic functions
        let aggregation = Aggregation::extract(from_clause_identifier, projection)?;

        let filter = selection
            .map(|selection| FilterExtractor::new(from_clause_identifier).extract(selection))
            .transpose()?;

        Ok(Self {
            aggregation,
            table: table_name,
            filter,
        })
    }

    fn extract_select_query(statements: &[ast::Statement]) -> Result<&ast::Query, ParseError> {
        if let [ast::Statement::Query(query)] = statements {
            Ok(query)
        } else {
            Err(unsupported!(
                "statements different from single SELECT statement.".to_string()
            ))
        }
    }

    pub fn create_data_extraction_query(
        &self,
        quote_style: Option<char>, // e.g. "'" for PostgreSQL, "`" for MySQL
    ) -> String {
        let mut projection = Vec::default();
        let aggregation_column_ident =
            ast::SelectItem::UnnamedExpr(ast::Expr::Identifier(ast::Ident {
                value: self.aggregation.column.clone(),
                quote_style,
            }));
        projection.push(aggregation_column_ident);
        if let Some(filter) = &self.filter {
            if filter.column != self.aggregation.column {
                let filter_column_ident =
                    ast::SelectItem::UnnamedExpr(ast::Expr::Identifier(ast::Ident {
                        value: filter.column.clone(),
                        quote_style,
                    }));
                projection.push(filter_column_ident);
            }
        }
        let from = vec![ast::TableWithJoins {
            relation: ast::TableFactor::Table {
                name: self.table.into_object_name(quote_style),
                alias: None,
                args: None,
                with_hints: Vec::default(),
                version: None,
                partitions: Vec::default(),
            },
            joins: Vec::default(),
        }];
        let select_expr = ast::Select {
            distinct: None,
            top: None,
            projection,
            into: None,
            from,
            lateral_views: Vec::default(),
            selection: None,
            group_by: ast::GroupByExpr::Expressions(Vec::default()),
            cluster_by: Vec::default(),
            distribute_by: Vec::default(),
            sort_by: Vec::default(),
            having: None,
            qualify: None,
            named_window: Vec::default(),
        };
        let query_body = ast::SetExpr::Select(Box::new(select_expr));
        let query = ast::Query {
            with: None,
            body: Box::new(query_body),
            order_by: Vec::default(),
            limit: None,
            offset: None,
            fetch: None,
            locks: Vec::default(),
            limit_by: Vec::default(),
            for_clause: None,
        };
        let select_statement = ast::Statement::Query(Box::new(query));
        select_statement.to_string()
    }
}

#[derive(Clone, Copy)]
pub(crate) enum FromClauseIdentifier<'a> {
    Base(&'a TabIdent),
    Alias { alias: &'a str },
}

impl FromClauseIdentifier<'_> {
    pub fn matches(
        self,
        db: Option<&ast::Ident>,
        schema: Option<&ast::Ident>,
        table: &ast::Ident,
    ) -> bool {
        match self {
            FromClauseIdentifier::Base(expected) => {
                let db_matches = if expected.db.is_none() {
                    true
                } else {
                    db.map_or(true, |db| {
                        expected
                            .db
                            .as_ref()
                            .map_or(true, |expected_db| &case_fold_identifier(db) == expected_db)
                    })
                };
                let schema_matches = if expected.schema.is_none() {
                    true
                } else {
                    schema.map_or(true, |schema| {
                        expected.schema.as_ref().map_or(true, |expected_schema| {
                            &case_fold_identifier(schema) == expected_schema
                        })
                    })
                };
                let table_matches = case_fold_identifier(table) == expected.table;
                db_matches && schema_matches && table_matches
            }
            FromClauseIdentifier::Alias { alias, .. } => {
                // An alias name is always unqualified, so it can never match a schema-qualified
                // table name.
                schema.is_none() && case_fold_identifier(table) == alias
            }
        }
    }
}

impl Display for FromClauseIdentifier<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FromClauseIdentifier::Base(table_info) => write!(f, "{table_info}"),
            FromClauseIdentifier::Alias { alias } => {
                write!(f, "{alias}")
            }
        }
    }
}
