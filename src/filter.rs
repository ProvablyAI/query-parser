use crate::{
    comparison::{
        self, is_binary_operator_supported, is_expression_supported, CompareOp, ComparisonOperand,
    },
    error::ParseError,
    query_metadata::FromClauseIdentifier,
    support::remove_outer_parens,
};

use sqlparser::ast;

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

    pub(crate) fn extract(&self, selection: &ast::Expr) -> Result<Filter, ParseError> {
        let selection = remove_outer_parens(selection);
        match selection {
            ast::Expr::BinaryOp { left, op, right } => {
                self.extract_binary_comparison(selection, left, op, right)
            }
            ast::Expr::IsNull(op)
            | ast::Expr::IsNotNull(op)
            | ast::Expr::IsTrue(op)
            | ast::Expr::IsNotTrue(op)
            | ast::Expr::IsFalse(op)
            | ast::Expr::IsNotFalse(op) => self.extract_unary_comparison(selection, op),
            _ => Err(unsupported!(format!(
                "unsupported expression in the WHERE clause: {selection}."
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

/// Contains information related to the filter applied in the query parsed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Filter {
    /// Column on which the filter is applied.
    pub column: String,
    /// Operation applied to the column.
    pub comparison: CompareOp,
}
