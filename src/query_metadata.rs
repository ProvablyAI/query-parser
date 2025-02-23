use std::{
    fmt::{self, Display},
    vec,
};

use serde::{Deserialize, Serialize};
use sqlparser::{
    ast::{
        self, Ident, Query, SelectItem, TableAlias, TableFactor, TableWithJoins,
        WildcardAdditionalOptions,
    },
    dialect::GenericDialect,
    parser::Parser,
};
use utoipa::{IntoParams, ToSchema};

use crate::{
    aggregation,
    destructured_query::DestructuredQuery,
    error::ParseError,
    projection::Projection,
    selection::{FilterExtractor, Selection},
    support::case_fold_identifier,
    table::{TabIdent, TableIdentWithAlias},
    unsupported,
};

/// QueryMetadata extracted from the query.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default, ToSchema, IntoParams)]
pub struct QueryMetadata {
    // pub projection: Vec<String>,
    /// Aggregation performed.
    pub projection: Projection,
    /// Table subject to query.
    pub table: TabIdent,
    /// Filter applied.
    pub selection: Option<Selection>,
    /// Data Extraction Query in SQL
    pub data_extraction_query: String,
    /// Data Aggregation Query in SQL
    pub data_answer_query: String,
    pub data_answer_index_query: String,
}

impl QueryMetadata {
    /// Generates `QueryMetadata` from a SQL query using [`crate::config::Config`].
    pub fn parse(
        sql_query: &str,
        quote_style: Option<char>, /* e.g. "'" for PostgreSQL, "`" for MySQL */
    ) -> Result<Self, ParseError> {
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
        //check and extract table information from FROM clause
        let TableIdentWithAlias(table_name, table_alias) = TableIdentWithAlias::extract(from)?;
        //extract table name to be used in the SELECT clause
        let from_clause_identifier = table_alias.as_deref().map_or_else(
            || FromClauseIdentifier::Base(&table_name),
            |x| FromClauseIdentifier::Alias { alias: x },
        );

        let projection_parsed = Projection::extract(from_clause_identifier, projection)?;
        let selection_parsed = selection
            .map(|selection| FilterExtractor::new(from_clause_identifier).extract(selection))
            .transpose()?;

        let data_extraction_query = Self::create_data_extraction_query(
            &projection_parsed,
            &table_name,
            &selection_parsed,
            quote_style,
        );

        let data_answer_query = Self::create_data_answer_query(projection, from, selection)?;
        let data_answer_index_query =
            Self::create_data_answer_index_query(from, &selection_parsed, selection, quote_style)?;

        Ok(Self {
            projection: projection_parsed,
            table: table_name,
            selection: selection_parsed,
            data_extraction_query,
            data_answer_query,
            data_answer_index_query,
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

    #[must_use]
    pub fn create_data_extraction_query(
        projection: &Projection,
        table: &TabIdent,
        selection: &Option<Selection>,
        quote_style: Option<char>, // e.g. "'" for PostgreSQL, "`" for MySQL
    ) -> String {
        let extraction_query_projection =
            Self::build_projection_of_extraction_query(projection, selection, quote_style);
        let from = vec![ast::TableWithJoins {
            relation: ast::TableFactor::Table {
                name: table.into_object_name(quote_style),
                alias: None,
                args: None,
                with_hints: Vec::default(),
                version: None,
                partitions: Vec::default(),
            },
            joins: Vec::default(),
        }];
        let query = create_query(&extraction_query_projection, &from, None);
        let select_statement = ast::Statement::Query(Box::new(query));
        select_statement.to_string()
    }

    fn build_projection_of_extraction_query(
        projection: &Projection,
        selection: &Option<Selection>,
        quote_style: Option<char>, // e.g. "'" for PostgreSQL, "`" for MySQL
    ) -> Vec<SelectItem> {
        let mut extraction_query_projection = Vec::default();

        match projection {
            Projection::Aggregations(aggregations) => {
                for aggregation in aggregations {
                    match &aggregation.column {
                        aggregation::Column::Name(name) => extraction_query_projection
                            .push(Self::build_select_item(name, &quote_style)),
                        aggregation::Column::Wildcard => {
                            return vec![SelectItem::Wildcard(WildcardAdditionalOptions {
                                opt_exclude: None,
                                opt_except: None,
                                opt_rename: None,
                                opt_replace: None,
                            })]
                        }
                    }
                }
            }
            Projection::PlainColumns(plain_columns) => extraction_query_projection.extend(
                plain_columns
                    .iter()
                    .map(|col| Self::build_select_item(&col.column, &quote_style)),
            ),
            Projection::Wildcard => {
                return vec![SelectItem::Wildcard(WildcardAdditionalOptions {
                    opt_exclude: None,
                    opt_except: None,
                    opt_rename: None,
                    opt_replace: None,
                })]
            }
        };

        if let Some(selection) = selection {
            extraction_query_projection.extend(Self::extract_selection_column_names(
                selection,
                &quote_style,
            ));
        }

        extraction_query_projection
    }

    fn extract_selection_column_names<'a>(
        selection: &'a Selection,
        quote_style: &'a Option<char>,
    ) -> impl Iterator<Item = SelectItem> + 'a {
        selection
            .filters
            .iter()
            .map(|filter| Self::build_select_item(&filter.column, quote_style))
    }

    fn build_select_item(column_name: &str, quote_style: &Option<char>) -> SelectItem {
        ast::SelectItem::UnnamedExpr(ast::Expr::Identifier(ast::Ident {
            value: String::from(column_name),
            quote_style: *quote_style,
        }))
    }

    fn create_data_answer_query(
        projection: &[ast::SelectItem],
        from: &[ast::TableWithJoins],
        selection: Option<&ast::Expr>,
    ) -> Result<String, ParseError> {
        let query = create_query(projection, from, selection);
        let select_statement = ast::Statement::Query(Box::new(query));
        Ok(select_statement.to_string())
    }

    // SELECT row FROM (SELECT row_number() over () as row, list_of_selection_columns FROM table) subquery WHERE selection
    fn create_data_answer_index_query(
        from: &[ast::TableWithJoins],
        selection_parsed: &Option<Selection>,
        selection: Option<&ast::Expr>,
        quote_style: Option<char>,
    ) -> Result<String, ParseError> {
        let mut subquery_selection = Vec::default();
        subquery_selection.push(ast::SelectItem::ExprWithAlias {
            expr: ast::Expr::Identifier(ast::Ident {
                value: String::from("row_number() over ()"),
                quote_style,
            }),
            alias: ast::Ident {
                value: String::from("row"),
                quote_style,
            },
        });

        if let Some(selection) = selection_parsed {
            subquery_selection.extend(Self::extract_selection_column_names(
                selection,
                &quote_style,
            ));
        }

        let subquery = create_query(&subquery_selection, from, None);

        let query_from = vec![TableWithJoins {
            relation: TableFactor::Derived {
                subquery: Box::new(subquery), // Embed subquery as a derived table
                alias: Some(TableAlias {
                    name: Ident::new("subquery"), // Alias for the subquery
                    columns: vec![],
                }),
                lateral: false,
            },
            joins: vec![],
        }];
        let row = Self::build_select_item("row", &quote_style);

        let query = create_query(&vec![row], &query_from, selection);

        let select_statement = ast::Statement::Query(Box::new(query));
        Ok(select_statement.to_string())
    }
}

fn create_query(
    projection: &[ast::SelectItem],
    from: &[ast::TableWithJoins],
    selection: Option<&ast::Expr>,
) -> Query {
    let select_expr = ast::Select {
        distinct: None,
        top: None,
        projection: projection.to_vec(),
        into: None,
        from: from.to_vec(),
        lateral_views: Vec::default(),
        selection: selection.cloned(),
        group_by: ast::GroupByExpr::Expressions(Vec::default()),
        cluster_by: Vec::default(),
        distribute_by: Vec::default(),
        sort_by: Vec::default(),
        having: None,
        qualify: None,
        named_window: Vec::default(),
    };
    let query_body = ast::SetExpr::Select(Box::new(select_expr));
    ast::Query {
        with: None,
        body: Box::new(query_body),
        order_by: Vec::default(),
        limit: None,
        offset: None,
        fetch: None,
        locks: Vec::default(),
        limit_by: Vec::default(),
        for_clause: None,
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
