use sqlparser::ast;

use crate::{error::ParseError, internal, malformed_query, query_metadata::FromClauseIdentifier};

//recursively removes outer parenthesis
pub(crate) fn remove_outer_parens(expr: &ast::Expr) -> &ast::Expr {
    match expr {
        ast::Expr::Nested(inner) => remove_outer_parens(inner),
        _ => expr,
    }
}

//extract column name from name_parts and if table/schema/db identifier are there, checks if it corresponds to FROM clause
pub(crate) fn extract_qualified_column(
    from_clause_identifier: FromClauseIdentifier<'_>,
    compound_identifier: &ast::Expr,
    name_parts: &[ast::Ident],
) -> Result<String, ParseError> {
    let unknown_column = || {
        Err(malformed_query!(format!(
                "the {compound_identifier} column is not part of the table that's listed in the FROM clause ({from_clause_identifier}).",
            )))
    };

    let mut name_parts = name_parts.iter();

    let column = name_parts.next_back().ok_or_else(|| {
        internal!("found empty column name (CompoundIdentifier) in query AST.".to_string())
    })?;
    let column = case_fold_identifier(column);

    if let Some(table) = name_parts.next_back() {
        let schema = name_parts.next_back();
        let db = name_parts.next_back();
        if !from_clause_identifier.matches(db, schema, table) {
            return unknown_column();
        }
    }
    if name_parts.count() > 0 {
        return Err(internal!(format!(
            "found too many ident in column name (i.e., {compound_identifier})."
        )));
    }

    Ok(column)
}

pub(crate) fn case_fold_identifier(ident: &ast::Ident) -> String {
    // Fold unquoted identifiers to lowercase, like PostgreSQL does (see
    // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS).
    let ast::Ident { value, quote_style } = ident;
    if quote_style.is_none() {
        value.to_ascii_lowercase()
    } else {
        value.to_string()
    }
}
