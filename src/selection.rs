use crate::{
    comparison::{
        self, is_binary_operator_supported, is_expression_supported, CompareOp, ComparisonOperand,
    },
    error::ParseError,
    query_metadata::FromClauseIdentifier,
    support::remove_outer_parens,
};

use serde::{Deserialize, Serialize};
use sqlparser::ast::{self, BinaryOperator, Expr};
use utoipa::{IntoParams, ToSchema};

use crate::unsupported;

pub(crate) struct FilterExtractor<'a> {
    from_clause_identifier: FromClauseIdentifier<'a>,
}

impl<'a> FilterExtractor<'a> {
    pub(crate) const fn new(from_clause_identifier: FromClauseIdentifier<'a>) -> Self {
        Self {
            from_clause_identifier,
        }
    }

    pub(crate) fn extract(&self, selection: &ast::Expr) -> Result<Selection, ParseError> {
        let selection = remove_outer_parens(selection);

        let mut filters: Vec<Filter> = Vec::new();
        let mut logical_op: Option<LogicalOperator> = None;

        fn traverse(
            filter_extractor: &FilterExtractor,
            expr: &Expr,
            filters: &mut Vec<Filter>,
            logical_op: &mut Option<LogicalOperator>,
        ) -> Result<(), ParseError> {
            match expr {
                Expr::BinaryOp { left, op, right } => {
                    if let Some(logical) = matches_logical_operator(op) {
                        if logical_op.as_ref().is_some_and(|op| op != &logical) {
                            return Err(unsupported!(format!(
                                "unsupported expression in the WHERE clause, can't use different logical operator."
                            )));
                        }

                        *logical_op = Some(logical);
                        traverse(filter_extractor, left, filters, logical_op)?;
                        traverse(filter_extractor, right, filters, logical_op)?;
                    } else {
                        filters.push(filter_extractor.handle_single(expr)?);
                    }
                }
                _ => filters.push(filter_extractor.handle_single(expr)?),
            }
            Ok(())
        }

        traverse(self, selection, &mut filters, &mut logical_op)?;

        Ok(Selection {
            filters,
            operation: logical_op,
        })
    }

    fn handle_single(&self, expr: &Expr) -> Result<Filter, ParseError> {
        match expr {
            ast::Expr::BinaryOp { left, op, right } => {
                self.extract_binary_comparison(expr, left, op, right)
            }
            ast::Expr::IsNull(op)
            // | ast::Expr::IsNotNull(op)
            | ast::Expr::IsTrue(op)
            | ast::Expr::IsFalse(op) => self.extract_unary_comparison(expr, op),
            _ => Err(unsupported!(format!(
                "unsupported expression in the WHERE clause: {expr}."
            ))),
        }
    }

    // analyze and extract LEFT OP RIGHT
    // where:
    // LEFT has to be a column or a constant value
    // OP has to be one between <, >, <=, >=
    // RIGHT, same as LEFT
    fn extract_binary_comparison(
        &self,
        binary_expr: &ast::Expr,
        left: &ast::Expr,
        op: &ast::BinaryOperator,
        right: &ast::Expr,
    ) -> Result<Filter, ParseError> {
        if !is_binary_operator_supported(op) {
            return Err(unsupported!(format!("the {op} operator.")));
        }
        //extract left operand and identify if it is a column or other
        let left = ComparisonOperand::from_expression(self.from_clause_identifier, left)?;
        //extract right operand and identify if it is a column or other
        let right = ComparisonOperand::from_expression(self.from_clause_identifier, right)?;
        //analyze extracted operand and eventually reverse them
        let (column, value, reverse) =
            comparison::analyze_comparison_operands(binary_expr, left, right)?;

        let comparison =
            CompareOp::from_binary_operator(op, Self::extract_constant_value(value)?, reverse)?;

        Ok(Filter { column, comparison })
    }

    // analyze and extract IS_NULL or IS_NOT_NULL
    fn extract_unary_comparison(
        &self,
        single_filter_expr: &ast::Expr,
        applied_on: &ast::Expr,
    ) -> Result<Filter, ParseError> {
        if !is_expression_supported(single_filter_expr) {
            return Err(unsupported!(format!("the {single_filter_expr} operator.")));
        }

        let column: ComparisonOperand<'_> =
            ComparisonOperand::from_expression(self.from_clause_identifier, applied_on)?;

        let ComparisonOperand::Column(column) = column else {
            return Err(unsupported!(format!(
                "{single_filter_expr}. Column must be specified.",
            )));
        };

        let comparison = CompareOp::from_expr(single_filter_expr)?;

        Ok(Filter { column, comparison })
    }

    fn extract_constant_value(expr: &ast::Expr) -> Result<String, ParseError> {
        let value = match expr {
            ast::Expr::UnaryOp {
                op,
                expr: unary_op_expr,
            } => {
                let sign = match op {
                    ast::UnaryOperator::Plus => None,
                    ast::UnaryOperator::Minus => Some("-"),
                    _ => return Err(unsupported!(format!("Expected a value, got {expr}"))),
                };
                let ast::Expr::Value(ast::Value::Number(val, _)) = unary_op_expr.as_ref() else {
                    return Err(unsupported!(format!("Expected a value, got {expr}")));
                };
                return Ok(format!("{}{val}", sign.unwrap_or_default()));
            }
            ast::Expr::Value(val) => val,
            _ => return Err(unsupported!(format!("Expected a value, got {expr}"))),
        };

        match value {
            ast::Value::Number(val, _)
            | ast::Value::SingleQuotedString(val)
            | ast::Value::EscapedStringLiteral(val)
            | ast::Value::SingleQuotedByteStringLiteral(val)
            | ast::Value::DoubleQuotedByteStringLiteral(val)
            | ast::Value::RawStringLiteral(val)
            | ast::Value::NationalStringLiteral(val)
            | ast::Value::HexStringLiteral(val)
            | ast::Value::DoubleQuotedString(val)
            | ast::Value::UnQuotedString(val) => Ok(val.clone()),
            ast::Value::Boolean(val) => Ok(val.to_string()),
            ast::Value::Null => Ok("Null".to_string()),
            ast::Value::Placeholder(val) => {
                Err(unsupported!(format!("Expected a value, got {val}")))
            }
            ast::Value::DollarQuotedString(val) => Ok(val.value.clone()),
        }
    }
}

/// Contains information related to the filters applied in the query parsed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default, ToSchema, IntoParams)]
pub struct Selection {
    /// Filter applied contained in the query.
    pub filters: Vec<Filter>,
    /// Operator applied to the filters.
    pub operation: Option<LogicalOperator>,
}

/// Contains information related to the filter applied in the query parsed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default, ToSchema, IntoParams)]
pub struct Filter {
    /// Column on which the filter is applied.
    pub column: String,
    /// Operation applied to the column.
    pub comparison: CompareOp,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default, ToSchema)]
#[serde(tag = "type")]
pub enum LogicalOperator {
    And,
    #[default]
    Or,
}

const fn matches_logical_operator(op: &BinaryOperator) -> Option<LogicalOperator> {
    match op {
        BinaryOperator::And => Some(LogicalOperator::And),
        BinaryOperator::Or => Some(LogicalOperator::Or),
        _ => None,
    }
}
