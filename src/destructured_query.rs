use sqlparser::ast;

use crate::{error::ParseError, unsupported};

pub(crate) struct DestructuredQuery<'a> {
    pub projection: &'a [ast::SelectItem], //i.e. select clause
    pub from: &'a [ast::TableWithJoins],   //i.e. from clause
    pub selection: Option<&'a ast::Expr>,  //i.e. where clause
}

impl<'a> DestructuredQuery<'a> {
    pub fn destructure(query: &'a ast::Query) -> Result<Self, ParseError> {
        let ast::Query {
            with,
            body,
            order_by,
            limit,
            offset,
            fetch,
            locks,
            limit_by,
            for_clause,
        } = query;

        if with.is_some() {
            return Err(unsupported!("CTEs (i.e., WITH clause).".to_string()));
        }
        if !order_by.is_empty() {
            return Err(unsupported!("ORDER BY.".to_string()));
        }
        if limit.is_some() {
            return Err(unsupported!("LIMIT.".to_string()));
        }
        if offset.is_some() {
            return Err(unsupported!("OFFSET.".to_string()));
        }
        if fetch.is_some() {
            return Err(unsupported!("FETCH.".to_string()));
        }
        if !locks.is_empty() {
            return Err(unsupported!(format!(
                "locking clauses (i.e., {}).",
                locks
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<String>>()
                    .join(", ")
            )));
        }
        if !limit_by.is_empty() {
            return Err(unsupported!(format!(
                "limit by clauses (i.e., {}).",
                limit_by
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<String>>()
                    .join(", ")
            )));
        }
        if for_clause.is_some() {
            return Err(unsupported!("FOR clause.".to_string()));
        }

        Self::destructure_set_expr(body)
    }

    fn destructure_set_expr(set_expr: &'a ast::SetExpr) -> Result<Self, ParseError> {
        match set_expr {
            ast::SetExpr::Select(select) => Self::destructure_select(select),
            ast::SetExpr::Query(query) => Self::destructure(query),
            ast::SetExpr::SetOperation { op, .. } => {
                Err(unsupported!(format!("set operations (i.e., {op}).")))
            }
            ast::SetExpr::Values(_) => Err(unsupported!("VALUES.".to_string())),
            ast::SetExpr::Insert(_) | ast::SetExpr::Update(_) => Err(unsupported!(
                "statements different from single SELECT statement.".to_string()
            )),
            ast::SetExpr::Table(_) => Err(unsupported!(
                "TABLE (i.e., SELECT * FROM table_name).".to_string()
            )),
        }
    }

    fn destructure_select(select: &'a ast::Select) -> Result<Self, ParseError> {
        let ast::Select {
            distinct,
            top,
            projection,
            into,
            from,
            lateral_views,
            selection,
            group_by,
            cluster_by,    //Used in HIVE
            distribute_by, //Used in HIVE
            sort_by,       //Used in HIVE
            having,
            qualify, //Used in Snowflake
            named_window,
        } = select;

        if distinct.is_some() {
            return Err(unsupported!("DISTINCT.".to_string()));
        }
        if top.is_some() {
            return Err(unsupported!("TOP.".to_string()));
        }
        if into.is_some() {
            return Err(unsupported!("SELECT INTO.".to_string()));
        }
        if !lateral_views.is_empty() {
            return Err(unsupported!("LATERAL VIEW.".to_string()));
        }
        match group_by {
            ast::GroupByExpr::All => return Err(unsupported!("ALL.".to_string())),
            ast::GroupByExpr::Expressions(exp) => {
                if !exp.is_empty() {
                    return Err(unsupported!("GROUP BY.".to_string()));
                }
            }
        }
        if !cluster_by.is_empty() {
            return Err(unsupported!("CLUSTER BY.".to_string()));
        }
        if !distribute_by.is_empty() {
            return Err(unsupported!("DISTRIBUTE BY.".to_string()));
        }
        if !sort_by.is_empty() {
            return Err(unsupported!("SORT BY.".to_string()));
        }
        if having.is_some() {
            return Err(unsupported!("HAVING.".to_string()));
        }
        if qualify.is_some() {
            return Err(unsupported!("QUALIFY.".to_string()));
        }
        if !named_window.is_empty() {
            return Err(unsupported!(
                "AS (OVER (PARTITION BY .. ORDER BY .. etc.)).".to_string()
            ));
        }

        Ok(Self {
            projection,
            from,
            selection: selection.as_ref(),
        })
    }
}
