use serde::{Deserialize, Serialize};
use sqlparser::ast;
use utoipa::{IntoParams, ToSchema};

use crate::{
    aggregation::Aggregation,
    error::ParseError,
    query_metadata::FromClauseIdentifier,
    support::{case_fold_identifier, remove_outer_parens},
    unsupported,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default, ToSchema)]
#[serde(tag = "type", content = "value")]
pub enum Projection {
    Aggregations(Vec<Aggregation>),
    PlainColumns(Vec<PlainColumn>),
    #[default]
    Wildcard,
}

impl Projection {
    pub(crate) fn extract(
        from_clause_identifier: FromClauseIdentifier<'_>,
        projection: &[ast::SelectItem],
    ) -> Result<Self, ParseError> {
        let unsupported = || {
            Err(unsupported!("the SELECT clause must contain or only aggregations / analytic functions or column names or the wildcard symbol. Nothing else is accepted.".to_string()))
        };

        let mut exprs: Vec<(&ast::Expr, Option<String>)> = Vec::new();
        for select_item in projection {
            match select_item {
                ast::SelectItem::Wildcard(_) => return Ok(Self::Wildcard),
                ast::SelectItem::UnnamedExpr(expr) => exprs.push((remove_outer_parens(expr), None)),
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    exprs.push((remove_outer_parens(expr), Some(case_fold_identifier(alias))));
                }
                ast::SelectItem::QualifiedWildcard(..) => return unsupported(),
            }
        }

        let (expr_sample, _) = exprs
            .first()
            .ok_or_else(|| unsupported!("Projection cannot be empty.".to_string()))?;
        match expr_sample {
            ast::Expr::Identifier(_) => Ok(Self::PlainColumns(PlainColumn::extract(exprs)?)),
            ast::Expr::Function(_) => Ok(Self::Aggregations(Aggregation::extract(
                from_clause_identifier,
                exprs,
            )?)),
            _ => unsupported(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, ToSchema, IntoParams)]
pub struct PlainColumn {
    pub column: String,
    pub alias: Option<String>,
}

impl PlainColumn {
    #[must_use]
    pub const fn new(column: String, alias: Option<String>) -> Self {
        Self { column, alias }
    }
}

impl PlainColumn {
    pub(crate) fn extract(
        exprs: Vec<(&ast::Expr, Option<String>)>,
    ) -> Result<Vec<Self>, ParseError> {
        let unsupported = || {
            Err(unsupported!(
                "If the SELECT clause contains a column name, it must contains only column names."
                    .to_string()
            ))
        };

        let mut plain_columns = Vec::new();
        for (expr, alias) in exprs {
            let ast::Expr::Identifier(ident) = remove_outer_parens(expr) else {
                return unsupported();
            };
            plain_columns.push(Self::new(case_fold_identifier(ident), alias));
        }

        Ok(plain_columns)
    }
}
