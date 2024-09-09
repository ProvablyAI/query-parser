use std::fmt::{self, Display};

use serde::{Deserialize, Serialize};
use sqlparser::ast;
use utoipa::{IntoParams, ToSchema};

use crate::{
    error::ParseError, malformed_query, query_metadata::FromClauseIdentifier, unsupported,
};

use super::support::{case_fold_identifier, extract_qualified_column, remove_outer_parens};

/// An aggregation that's computed over the values of a column.
///
/// Represents an occurrence of an aggregation such as `function(column)`
/// within the `SELECT` clause of a query.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default, ToSchema, IntoParams)]
pub struct Aggregation {
    /// The function used as aggregator of column's values.
    pub function: KoronFunction,
    /// The name of the column on which the function is executed.
    pub column: String,
    /// The alias that's assigned to the result of the function: `function(column) AS alias`.
    pub alias: Option<String>,
}

impl Aggregation {
    pub(crate) fn extract(
        from_clause_identifier: FromClauseIdentifier<'_>,
        projection: &[ast::SelectItem],
    ) -> Result<Self, ParseError> {
        let multiple_aggregations = || {
            Err(unsupported!("the SELECT clause must contain exactly one aggregation / analytic function. Nothing else is accepted.".to_string()))
        };
        //check if single operation in the projection
        let (expr, alias) = match projection {
            [ast::SelectItem::UnnamedExpr(expr)] => (expr, None),
            [ast::SelectItem::ExprWithAlias { expr, alias }] => {
                (expr, Some(case_fold_identifier(alias)))
            }
            _ => {
                return multiple_aggregations();
            }
        };
        //remove outer parens if any and check if the contained expression is a single function
        let ast::Expr::Function(function) = remove_outer_parens(expr) else {
            return multiple_aggregations();
        };

        //destructure function
        let ast::Function {
            name,
            args,
            over,
            distinct,
            special: _,
            order_by,
            filter,
            null_treatment,
        } = function;
        if over.is_some() {
            return Err(unsupported!("window functions (OVER).".to_string()));
        }
        if *distinct {
            return Err(unsupported!("DISTINCT.".to_string()));
        }
        if !order_by.is_empty() {
            return Err(unsupported!("ORDER BY.".to_string()));
        }
        if filter.is_some() {
            return Err(unsupported!("FILTER.".to_string()));
        }
        if null_treatment.is_some() {
            return Err(unsupported!("IGNORE NULLS.".to_string()));
        }
        //check if it is a supported function
        let (function, column) =
            Self::validate_function_and_arguments(from_clause_identifier, name, args)?;

        Ok(Self {
            function,
            column,
            alias,
        })
    }

    fn validate_function_and_arguments(
        from_clause_identifier: FromClauseIdentifier<'_>,
        function_name: &ast::ObjectName,
        args: &[ast::FunctionArg],
    ) -> Result<(KoronFunction, String), ParseError> {
        //closure that extracts column information from the statement
        let only_column_arg = |function| {
            let column =
                Self::extract_only_column_argument(from_clause_identifier, function_name, args)?;
            Ok((function, column))
        };

        let ast::ObjectName(name_parts) = function_name;
        if let [unqualified_name] = &name_parts[..] {
            //currently only these four functions are supported by Koron
            match &case_fold_identifier(unqualified_name)[..] {
                "sum" => return only_column_arg(KoronFunction::Sum),
                "count" => return only_column_arg(KoronFunction::Count),
                "avg" => return only_column_arg(KoronFunction::Average),
                "median" => return only_column_arg(KoronFunction::Median),
                "variance" => return only_column_arg(KoronFunction::Variance),
                "stddev" => return only_column_arg(KoronFunction::StandardDeviation),
                _ => (),
            }
        }
        Err(unsupported!(format!(
            "unrecognized or unsupported function: {function_name}."
        )))
    }

    fn extract_only_column_argument(
        from_clause_identifier: FromClauseIdentifier<'_>,
        function_name: &ast::ObjectName,
        args: &[ast::FunctionArg],
    ) -> Result<String, ParseError> {
        //currently only functions that takes as input a single column are supported (i.e. a single argument)
        match args {
            [arg] => {
                let arg_expr = Self::extract_unnamed_argument(arg)?;
                Self::extract_aggregated_column(from_clause_identifier, function_name, arg_expr, "")
            }
            _ => Err(malformed_query!(format!(
                "the {function_name} function takes exactly 1 argument, but {} {verb} provided.",
                args.len(),
                verb = if args.len() == 1 { "is" } else { "are" },
            ))),
        }
    }

    fn extract_unnamed_argument(
        arg: &ast::FunctionArg,
    ) -> Result<&ast::FunctionArgExpr, ParseError> {
        match arg {
            ast::FunctionArg::Named { .. } => Err(unsupported!(format!(
                "named function arguments (such as {arg})."
            ))),
            ast::FunctionArg::Unnamed(arg_expr) => Ok(arg_expr),
        }
    }

    fn extract_aggregated_column(
        from_clause_identifier: FromClauseIdentifier<'_>,
        function_name: &ast::ObjectName,
        arg_expr: &ast::FunctionArgExpr,
        which_arg: &str,
    ) -> Result<String, ParseError> {
        if let ast::FunctionArgExpr::Expr(expr) = arg_expr {
            match remove_outer_parens(expr) {
                ast::Expr::Identifier(ident) => return Ok(case_fold_identifier(ident)),
                compound_identifier @ ast::Expr::CompoundIdentifier(name_parts) => {
                    return extract_qualified_column(
                        from_clause_identifier,
                        compound_identifier,
                        name_parts,
                    );
                }
                _ => (),
            }
        }
        Err(unsupported!(format!(
                "only a column name is supported as the {which_arg}{space}argument of the {function_name} function.",
                space = if which_arg.is_empty() { "" } else { " " },
            )))
    }
}

/// Represents a Koron aggregation / analytic function.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default, ToSchema)]
pub enum KoronFunction {
    /// The `sum` aggregation function.
    Sum,
    /// The `count` aggregation function.
    #[default]
    Count,
    /// The `average` aggregation function.
    Average,
    /// The `median` aggregation function.
    Median,
    /// The `variance` aggregation function.
    Variance,
    /// The `stddev` aggregation function.
    StandardDeviation,
}

impl Display for KoronFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sum => write!(f, "Sum"),
            Self::Count => write!(f, "Count"),
            Self::Average => write!(f, "Average"),
            Self::Median => write!(f, "Median"),
            Self::Variance => write!(f, "Variance"),
            Self::StandardDeviation => write!(f, "Standard Deviation"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::KoronFunction;

    #[test]
    fn koron_fn_display() {
        let cases = [
            (KoronFunction::Count, "Count"),
            (KoronFunction::Sum, "Sum"),
            (KoronFunction::Variance, "Variance"),
            (KoronFunction::Median, "Median"),
            (KoronFunction::Average, "Average"),
            (KoronFunction::StandardDeviation, "Standard Deviation"),
        ];
        for (koron_fn, expected) in cases {
            assert_eq!(koron_fn.to_string(), expected.to_string());
        }
    }
}
