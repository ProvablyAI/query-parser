use std::{default, fmt::{self, Display}};

use serde::{Deserialize, Serialize};
use sqlparser::ast;

use crate::{
    error::ParseError, query_metadata::FromClauseIdentifier, support::case_fold_identifier,
    unsupported,
};

use super::support::{extract_qualified_column, remove_outer_parens};

#[must_use]
pub const fn is_binary_operator_supported(op: &ast::BinaryOperator) -> bool {
    matches!(
        op,
        &ast::BinaryOperator::Gt
            | &ast::BinaryOperator::GtEq
            | &ast::BinaryOperator::Lt
            | &ast::BinaryOperator::LtEq
            | &ast::BinaryOperator::Eq
            | &ast::BinaryOperator::NotEq
    )
}

#[must_use]
pub const fn is_expression_supported(op: &ast::Expr) -> bool {
    matches!(
        op,
        &ast::Expr::IsNull(..)
            | &ast::Expr::IsNotNull(..)
            | &ast::Expr::IsTrue(..)
            | &ast::Expr::IsNotTrue(..)
            | &ast::Expr::IsFalse(..)
            | &ast::Expr::IsNotFalse(..)
    )
}

/// The comparison operation between the value of an unspecified column and some constant values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CompareOp {
    /// Check if column's value is less than `value`.
    Lt { value: String },
    /// Check if column's value is less than or equal to `value`.
    LtEq { value: String },
    /// Check if column's value is greater than `value`.
    Gt { value: String },
    /// Check if column's value is greater than or equal to `value`.
    GtEq { value: String },
    /// Check if column's value is equal to `value`.
    Eq { value: String },
    /// Check if column's value is not equal to `value`.
    NotEq { value: String },
    /// Check if column's value is `NULL`.
    #[default]
    IsNull,
    /// Check if column's value is not `NULL`.
    IsNotNull,
    /// Check if column's value is `true`.
    IsTrue,
    /// Check if column's value is not `true`.
    IsNotTrue,
    /// Check if column's value is `false`.
    IsFalse,
    /// Check if column's value is not `false`.
    IsNotFalse,
}

impl Display for CompareOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lt { value: _ } => write!(f, "Less than"),
            Self::LtEq { value: _ } => write!(f, "Less than or equal"),
            Self::Gt { value: _ } => write!(f, "Greater than"),
            Self::GtEq { value: _ } => write!(f, "Greater than or equal"),
            Self::Eq { value: _ } => write!(f, "Equal"),
            Self::NotEq { value: _ } => write!(f, "Not equal"),
            Self::IsNull => write!(f, "Is null"),
            Self::IsNotNull => write!(f, "Is not null"),
            Self::IsTrue => write!(f, "Is true"),
            Self::IsNotTrue => write!(f, "Is not true"),
            Self::IsFalse => write!(f, "Is false"),
            Self::IsNotFalse => write!(f, "Is not false"),
        }
    }
}

impl CompareOp {
    pub(crate) fn from_binary_operator(
        op: &ast::BinaryOperator,
        value: String,
        reverse: bool,
    ) -> Result<Self, ParseError> {
        let comparison = match op {
            ast::BinaryOperator::Lt if reverse => Self::Gt { value },
            ast::BinaryOperator::Lt => Self::Lt { value },
            ast::BinaryOperator::LtEq if reverse => Self::GtEq { value },
            ast::BinaryOperator::LtEq => Self::LtEq { value },
            ast::BinaryOperator::Gt if reverse => Self::Lt { value },
            ast::BinaryOperator::Gt => Self::Gt { value },
            ast::BinaryOperator::GtEq if reverse => Self::LtEq { value },
            ast::BinaryOperator::GtEq => Self::GtEq { value },
            ast::BinaryOperator::Eq => Self::Eq { value },
            ast::BinaryOperator::NotEq => Self::NotEq { value },
            _ => {
                return Err(unsupported!(format!("the {op} operator.")));
            }
        };
        Ok(comparison)
    }

    pub(crate) fn from_expr(op: &ast::Expr) -> Result<Self, ParseError> {
        let comparison = match op {
            ast::Expr::IsNull(_) => Self::IsNull,
            ast::Expr::IsNotNull(_) => Self::IsNotNull,
            ast::Expr::IsTrue(_) => Self::IsTrue,
            ast::Expr::IsNotTrue(_) => Self::IsNotTrue,
            ast::Expr::IsFalse(_) => Self::IsFalse,
            ast::Expr::IsNotFalse(_) => Self::IsNotFalse,
            _ => {
                return Err(unsupported!(format!("the {op} operator.")));
            }
        };
        Ok(comparison)
    }
}

#[derive(Debug)]
pub(crate) enum ComparisonOperand<'a> {
    Column(String),
    // Other can be a static value, or another expression
    Other(&'a ast::Expr),
}

impl<'a> ComparisonOperand<'a> {
    pub(crate) fn from_expression(
        from_clause_identifier: FromClauseIdentifier<'_>,
        expr: &'a ast::Expr,
    ) -> Result<Self, ParseError> {
        let expr = remove_outer_parens(expr);
        match expr {
            ast::Expr::Identifier(ident) => Ok(Self::Column(case_fold_identifier(ident))),
            ast::Expr::CompoundIdentifier(name_parts) => {
                extract_qualified_column(from_clause_identifier, expr, name_parts).map(Self::Column)
            }
            _ => Ok(Self::Other(expr)),
        }
    }
}

pub(crate) fn analyze_comparison_operands<'a>(
    binary_expr: &'a ast::Expr,
    left: ComparisonOperand<'a>,
    right: ComparisonOperand<'a>,
) -> Result<(String, &'a ast::Expr, bool), ParseError> {
    match (left, right) {
        (ComparisonOperand::Column(column), ComparisonOperand::Other(value)) => {
            Ok((column, value, false))
        }
        (ComparisonOperand::Other(value), ComparisonOperand::Column(column)) => {
            // keep on the left the column
            Ok((column, value, true))
        }
        _ => Err(unsupported!(format!(
            "{binary_expr}. Only comparisons between a column and a constant are supported.",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Ident;

    use crate::{
        comparison::{is_binary_operator_supported, is_expression_supported, CompareOp},
        error::ParseError,
    };

    use super::ast;
    #[test]
    fn test_supported_binary_operator() {
        assert!(is_binary_operator_supported(&ast::BinaryOperator::Gt));
        assert!(is_binary_operator_supported(&ast::BinaryOperator::GtEq));
        assert!(is_binary_operator_supported(&ast::BinaryOperator::Lt));
        assert!(is_binary_operator_supported(&ast::BinaryOperator::LtEq));
        assert!(is_binary_operator_supported(&ast::BinaryOperator::Eq));
        assert!(is_binary_operator_supported(&ast::BinaryOperator::NotEq));
    }

    #[test]
    fn test_unsupported_binary_operator() {
        assert!(!is_binary_operator_supported(&ast::BinaryOperator::Plus));
        assert!(!is_binary_operator_supported(&ast::BinaryOperator::Minus));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::Multiply
        ));
        assert!(!is_binary_operator_supported(&ast::BinaryOperator::Divide));
        assert!(!is_binary_operator_supported(&ast::BinaryOperator::Modulo));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::StringConcat
        ));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::Spaceship
        ));
        assert!(!is_binary_operator_supported(&ast::BinaryOperator::And));
        assert!(!is_binary_operator_supported(&ast::BinaryOperator::Or));
        assert!(!is_binary_operator_supported(&ast::BinaryOperator::Xor));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::BitwiseAnd
        ));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::BitwiseOr
        ));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::BitwiseXor
        ));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::PGBitwiseXor
        ));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::PGBitwiseShiftLeft
        ));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::PGBitwiseShiftRight
        ));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::PGRegexIMatch
        ));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::PGRegexMatch
        ));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::PGRegexNotIMatch
        ));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::PGRegexNotMatch
        ));
        assert!(!is_binary_operator_supported(
            &ast::BinaryOperator::PGCustomBinaryOperator(Vec::new())
        ));
    }

    #[test]
    fn test_supported_expression() {
        assert!(is_expression_supported(&ast::Expr::IsNull(Box::new(
            ast::Expr::Identifier(Ident {
                value: String::new(),
                quote_style: None
            })
        ))));
        assert!(is_expression_supported(&ast::Expr::IsNotNull(Box::new(
            ast::Expr::Identifier(Ident {
                value: String::new(),
                quote_style: None
            })
        ))));
    }

    #[test]
    fn test_from_binary_operator() {
        let value: String = "1".to_string();
        let mut reverse = false;
        let expected_lt = CompareOp::Lt {
            value: value.clone(),
        };
        let expected_lt_eq = CompareOp::LtEq {
            value: value.clone(),
        };
        let expected_gt = CompareOp::Gt {
            value: value.clone(),
        };
        let expected_gt_eq = CompareOp::GtEq {
            value: value.clone(),
        };
        let expected_eq = CompareOp::Eq {
            value: value.clone(),
        };
        let expected_not_eq = CompareOp::NotEq {
            value: value.clone(),
        };
        let expected_error = ParseError::Unsupported {
            message: "the AND operator.".to_string(),
        };

        let op = ast::BinaryOperator::Lt;
        let result = CompareOp::from_binary_operator(&op, value.clone(), reverse).unwrap();
        assert_eq!(expected_lt, result);

        let op = ast::BinaryOperator::LtEq;
        let result = CompareOp::from_binary_operator(&op, value.clone(), reverse).unwrap();
        assert_eq!(expected_lt_eq, result);

        let op = ast::BinaryOperator::Gt;
        let result = CompareOp::from_binary_operator(&op, value.clone(), reverse).unwrap();
        assert_eq!(expected_gt, result);

        let op = ast::BinaryOperator::GtEq;
        let result = CompareOp::from_binary_operator(&op, value.clone(), reverse).unwrap();
        assert_eq!(expected_gt_eq, result);

        reverse = true;

        let op = ast::BinaryOperator::Gt;
        let result = CompareOp::from_binary_operator(&op, value.clone(), reverse).unwrap();
        assert_eq!(expected_lt, result);

        let op = ast::BinaryOperator::GtEq;
        let result = CompareOp::from_binary_operator(&op, value.clone(), reverse).unwrap();
        assert_eq!(expected_lt_eq, result);

        let op = ast::BinaryOperator::Lt;
        let result = CompareOp::from_binary_operator(&op, value.clone(), reverse).unwrap();
        assert_eq!(expected_gt, result);

        let op = ast::BinaryOperator::LtEq;
        let result = CompareOp::from_binary_operator(&op, value.clone(), reverse).unwrap();
        assert_eq!(expected_gt_eq, result);

        let op = ast::BinaryOperator::Eq;
        let result = CompareOp::from_binary_operator(&op, value.clone(), reverse).unwrap();
        assert_eq!(expected_eq, result);

        let op = ast::BinaryOperator::NotEq;
        let result = CompareOp::from_binary_operator(&op, value.clone(), reverse).unwrap();
        assert_eq!(expected_not_eq, result);

        let op = ast::BinaryOperator::And;
        let result = CompareOp::from_binary_operator(&op, value, reverse).unwrap_err();
        assert_eq!(expected_error, result);
    }

    #[test]
    fn test_from_expr() {
        let expected_is_null = CompareOp::IsNull;
        let expected_is_not_null = CompareOp::IsNotNull;

        let op = ast::Expr::IsNull(Box::new(ast::Expr::Identifier(Ident {
            value: String::new(),
            quote_style: None,
        })));
        let result = CompareOp::from_expr(&op).unwrap();
        assert_eq!(expected_is_null, result);

        let op = ast::Expr::IsNotNull(Box::new(ast::Expr::Identifier(Ident {
            value: String::new(),
            quote_style: None,
        })));
        let result = CompareOp::from_expr(&op).unwrap();
        assert_eq!(expected_is_not_null, result);
    }
}
