#![allow(clippy::missing_errors_doc, clippy::doc_markdown)]
pub mod aggregation;
pub mod comparison;
pub mod destructured_query;
pub mod error;
pub mod filter;
pub mod query_metadata;
pub mod support;
pub mod table;

#[cfg(test)]
mod tests {

    use crate::query_metadata::QueryMetadata;
    use crate::table::TabIdent;
    use crate::{internal, malformed_query, unsupported};

    use super::aggregation::{Aggregation, KoronFunction};
    use super::comparison::CompareOp;
    use super::error::ParseError;
    use super::filter::Filter;

    fn sample_sum() -> Aggregation {
        Aggregation {
            function: KoronFunction::Sum,
            column: "test_column_2".to_string(),
            alias: None,
        }
    }

    fn sample_tab_ident() -> TabIdent {
        TabIdent {
            db: Some("test_db".to_string()),
            schema: Some("test_schema".to_string()),
            table: "test_table_1".to_string(),
        }
    }

    #[test]
    fn basic_aggregation() {
        let cases = [
            ("SUM(test_column_2)", KoronFunction::Sum),
            ("COUNT(test_column_2)", KoronFunction::Count),
            ("AVG(test_column_2)", KoronFunction::Average),
            ("MEDIAN(test_column_2)", KoronFunction::Median),
            ("VARIANCE(test_column_2)", KoronFunction::Variance),
            ("STDDEV(test_column_2)", KoronFunction::StandardDeviation),
        ];

        for (projection, function) in cases {
            let query = &format!("SELECT {projection} FROM test_db.test_schema.test_table_1");

            let data_aggregation_query = if function == KoronFunction::Median {
                None
            } else {
                Some(format!(
                    "SELECT CAST({projection} AS TEXT) FROM test_db.test_schema.test_table_1"
                ))
            };

            let expected = Ok(QueryMetadata {
                table: sample_tab_ident(),
                aggregation: Aggregation {
                    function,
                    column: "test_column_2".to_string(),
                    alias: None,
                },
                filter: None,
                data_extraction_query: String::from(
                    "SELECT test_column_2 FROM test_db.test_schema.test_table_1",
                ),
                data_aggregation_query,
            });
            assert_eq!(
                QueryMetadata::parse(query, None),
                expected,
                "\nfailed for aggregation {projection}",
            );
        }
    }

    #[test]
    fn parenthesized_query() {
        let query = "(((SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1)))";
        let expected = Ok(QueryMetadata {
            table: sample_tab_ident(),
            aggregation: sample_sum(),
            filter: None,
            data_extraction_query: String::from(
                "SELECT test_column_2 FROM test_db.test_schema.test_table_1",
            ),
            data_aggregation_query: Some(String::from(
                "SELECT CAST(SUM(test_column_2) AS TEXT) FROM test_db.test_schema.test_table_1",
            )),
        });
        assert_eq!(QueryMetadata::parse(query, None), expected);
    }

    #[test]
    fn parenthesized_function() {
        let query = "SELECT (((SUM(test_column_2)))) FROM test_db.test_schema.test_table_1";
        let expected = Ok(QueryMetadata {
            table: sample_tab_ident(),
            aggregation: sample_sum(),
            filter: None,
            data_extraction_query:String::from("SELECT test_column_2 FROM test_db.test_schema.test_table_1"),
            data_aggregation_query: Some(String::from("SELECT CAST((((SUM(test_column_2)))) AS TEXT) FROM test_db.test_schema.test_table_1")),
        });
        assert_eq!(QueryMetadata::parse(query, None), expected);
    }

    #[test]
    fn parenthesized_column() {
        let query = "SELECT SUM((((test_column_2)))) FROM test_db.test_schema.test_table_1";
        let expected = Ok(QueryMetadata {
            table: sample_tab_ident(),
            aggregation: sample_sum(),
            filter: None,
            data_extraction_query:String::from("SELECT test_column_2 FROM test_db.test_schema.test_table_1"),
            data_aggregation_query: Some(String::from("SELECT CAST(SUM((((test_column_2)))) AS TEXT) FROM test_db.test_schema.test_table_1")),
        });
        assert_eq!(QueryMetadata::parse(query, None), expected);
    }

    #[test]
    fn result_alias() {
        let query = "SELECT SUM(test_column_2) AS s FROM test_db.test_schema.test_table_1";
        let expected = Ok(QueryMetadata {
            table: sample_tab_ident(),
            aggregation: Aggregation {
                function: KoronFunction::Sum,
                column: "test_column_2".to_string(),
                alias: Some("s".to_string()),
            },
            filter: None,
            data_extraction_query:String::from("SELECT test_column_2 FROM test_db.test_schema.test_table_1"),
            data_aggregation_query: Some(String::from("SELECT CAST(SUM(test_column_2) AS TEXT) AS s FROM test_db.test_schema.test_table_1")),
        });
        assert_eq!(QueryMetadata::parse(query, None), expected);
    }

    #[test]
    fn table_alias() {
        let query = "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 AS t";
        let expected = Ok(QueryMetadata {
            table: sample_tab_ident(),
            aggregation: sample_sum(),
            filter: None,
            data_extraction_query:String::from("SELECT test_column_2 FROM test_db.test_schema.test_table_1"),
            data_aggregation_query: Some(String::from("SELECT CAST(SUM(test_column_2) AS TEXT) FROM test_db.test_schema.test_table_1 AS t")),
        });
        assert_eq!(QueryMetadata::parse(query, None), expected);
    }

    #[test]
    fn unquoted_function_case_insensitive() {
        let query = "SELECT sum(test_column_2) FROM test_db.test_schema.test_table_1";
        let expected = Ok(QueryMetadata {
            table: sample_tab_ident(),
            aggregation: sample_sum(),
            filter: None,
            data_extraction_query: String::from(
                "SELECT test_column_2 FROM test_db.test_schema.test_table_1",
            ),
            data_aggregation_query: Some(String::from(
                "SELECT CAST(sum(test_column_2) AS TEXT) FROM test_db.test_schema.test_table_1",
            )),
        });
        assert_eq!(QueryMetadata::parse(query, None), expected);
    }

    #[test]
    fn quoted_function_case_sensitive() {
        let query = "SELECT \"SUM\"(test_column_2) FROM test_db.test_schema.test_table_1";
        let expected = Err(unsupported!(
            "unrecognized or unsupported function: \"SUM\".".to_string()
        ));
        assert_eq!(QueryMetadata::parse(query, None), expected);
    }

    #[test]
    fn unquoted_result_alias_case_insensitive() {
        let query = "SELECT SUM(test_column_2) AS S FROM test_db.test_schema.test_table_1";
        let expected = Ok(QueryMetadata {
            table: sample_tab_ident(),
            aggregation: Aggregation {
                function: KoronFunction::Sum,
                column: "test_column_2".to_string(),
                alias: Some("s".to_string()),
            },
            filter: None,
            data_extraction_query:String::from("SELECT test_column_2 FROM test_db.test_schema.test_table_1"),
            data_aggregation_query: Some(String::from("SELECT CAST(SUM(test_column_2) AS TEXT) AS S FROM test_db.test_schema.test_table_1")),
        });
        assert_eq!(QueryMetadata::parse(query, None), expected);
    }

    #[test]
    fn quoted_result_alias_case_sensitive() {
        let query = "SELECT SUM(test_column_2) AS \"S\" FROM test_db.test_schema.test_table_1";
        let expected = Ok(QueryMetadata {
            table: sample_tab_ident(),
            aggregation: Aggregation {
                function: KoronFunction::Sum,
                column: "test_column_2".to_string(),
                alias: Some("S".to_string()),
            },
            filter: None,
            data_extraction_query:String::from("SELECT test_column_2 FROM test_db.test_schema.test_table_1"),
            data_aggregation_query: Some(String::from("SELECT CAST(SUM(test_column_2) AS TEXT) AS \"S\" FROM test_db.test_schema.test_table_1")),
        });
        assert_eq!(QueryMetadata::parse(query, None), expected);
    }

    #[test]
    fn quoted_table_alias_case_sensitive() {
        for (column, alias, extracted_alias) in [
            ("t.test_column_2", "\"T\"", "T"),
            ("\"T\".test_column_2", "\"t\"", "t"),
        ] {
            let query =
                &format!("SELECT SUM({column}) FROM test_db.test_schema.test_table_1 AS {alias}");

            let expected = Err(malformed_query!(format!(
                "the {column} column is not part of \
                     the table that's listed in the FROM clause ({extracted_alias}).",
            )));
            assert_eq!(
                QueryMetadata::parse(query, None),
                expected,
                "\nfailed for query {query:?}",
            );
        }
    }

    #[test]
    fn qualified_column_from_different_table() {
        for column in [
            "\"test_table_2\".test_column_2",
            "\"test_schema\".test_table_2.test_column_2",
        ] {
            let query = &format!("SELECT SUM({column}) FROM test_db.test_schema.test_table_1");

            let expected = Err(malformed_query!(format!(
                    "the {column} column is not part of \
                     the table that's listed in the FROM clause (test_db.test_schema.test_table_1).",
                )));
            assert_eq!(
                QueryMetadata::parse(query, None),
                expected,
                "\nfailed for query {query:?}",
            );
        }
    }

    #[test]
    fn qualified_column_not_from_table_alias() {
        for column in [
            "test_table_1.test_column_2",
            "test_schema.test_table_1.test_column_2",
        ] {
            let query = &format!("SELECT SUM({column}) FROM test_db.test_schema.test_table_1 AS t");
            let expected = Err(malformed_query!(format!(
                "the {column} column is not part of \
                     the table that's listed in the FROM clause (t).",
            )));
            assert_eq!(
                QueryMetadata::parse(query, None),
                expected,
                "\nfailed for query {query:?}",
            );
        }
    }

    #[test]
    fn sql_syntax_error() {
        let query = "SELECT * FROM";
        let expected = Err(malformed_query!(
            "sql parser error: Expected identifier, found: EOF".to_string()
        ));
        assert_eq!(QueryMetadata::parse(query, None), expected);
    }

    #[test]
    fn table_name_too_many_name_parts() {
        let query = "SELECT SUM(test_column_2) FROM x.test_db.test_schema.test_table_1";
        let expected = Err(internal!("found too many ident in table name (i.e., x.test_db.test_schema.test_table_1) in query AST.".to_string()));
        assert_eq!(QueryMetadata::parse(query, None), expected);
    }

    #[test]
    fn column_name_too_many_name_parts() {
        let query = "SELECT SUM(x.test_db.test_schema.test_table_1.test_column_2) FROM test_db.test_schema.test_table_1";
        let expected = Err(internal!("found too many ident in column name (i.e., x.test_db.test_schema.test_table_1.test_column_2)."
                .to_string()));
        assert_eq!(QueryMetadata::parse(query, None), expected);
    }

    #[test]
    fn wrong_number_of_arguments() {
        let cases = [
            (
                "SUM()",
                "the SUM function takes exactly 1 argument, but 0 are provided.",
            ),
            (
                "SUM(test_column_2, test_column_2)",
                "the SUM function takes exactly 1 argument, but 2 are provided.",
            ),
        ];

        for (projection, reason) in cases {
            let query = &format!("SELECT {projection} FROM test_db.test_schema.test_table_1");
            let expected = Err(malformed_query!(reason.to_string()));
            assert_eq!(
                QueryMetadata::parse(query, None),
                expected,
                "\nfailed for aggregation {projection}",
            );
        }
    }

    #[test]
    fn unsupported_sql_features() {
        let cases = [
            (
                "SELECT * FROM test_db.test_schema.test_table_1; SELECT * FROM test_db.test_schema.test_table_1",
                "statements different from single SELECT statement.",
            ),
            (
                "DELETE FROM test_db.test_schema.test_table_1",
                "statements different from single SELECT statement.",
            ),
            (
                "WITH t AS (SELECT 1) SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1",
                "CTEs (i.e., WITH clause).",
            ),
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 ORDER BY SUM",
                "ORDER BY.",
            ),
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 LIMIT 1",
                "LIMIT.",
            ),
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 OFFSET 1",
                "OFFSET.",
            ),
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 FETCH FIRST 1 ROW ONLY",
                "FETCH.",
            ),
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 drda FOR UPDATE",
                "locking clauses (i.e., FOR UPDATE).",
            ),
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 \
                UNION \
                SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1",
                "set operations (i.e., UNION).",
            ),
            ("VALUES (1)", "VALUES."),
            (
                "INSERT INTO test_table_1(test_column_2) VALUES(1)",
                "statements different from single SELECT statement."
            ),
            (
                "SELECT DISTINCT SUM(test_column_2) FROM test_db.test_schema.test_table_1",
                "DISTINCT.",
            ),
            // TOP is MSSQL syntax.
            (
                "SELECT TOP 1 SUM(test_column_2) FROM test_db.test_schema.test_table_1",
                "TOP.",
            ),
            (
                "SELECT SUM(test_column_2) INTO t FROM test_db.test_schema.test_table_1",
                "SELECT INTO.",
            ),
            // LATERAL VIEW is HiveQL syntax.
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 LATERAL VIEW (SELECT 1) t",
                "LATERAL VIEW.",
            ),
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 GROUP BY SUM",
                "GROUP BY.",
            ),
            // CLUSTER BY is HiveQL syntax.
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 CLUSTER BY SUM",
                "CLUSTER BY.",
            ),
            // DISTRIBUTE BY is HiveQL syntax.
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 DISTRIBUTE BY SUM",
                "DISTRIBUTE BY.",
            ),
            // SORT BY is HiveQL syntax.
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 SORT BY SUM",
                "SORT BY.",
            ),
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 HAVING sum > 0",
                "HAVING.",
            ),
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1, treasury.attachment",
                "the FROM clause has multiple tables (no JOINs, subqueries or functions allowed).",
            ),
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 CROSS JOIN treasury.attachment",
                "the FROM clause has multiple tables (no JOINs, subqueries or functions allowed).",
            ),
            (
                "SELECT SUM(test_column_2) FROM f('arg')",
                "the FROM clause has multiple tables (no JOINs, subqueries or functions allowed).",
            ),
            // table hints are MSSQL syntax.
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 WITH (NOLOCK)",
                "table hints (WITH in FROM clauses).",
            ),
            (
                "SELECT SUM(test_column_2) FROM (SELECT * FROM test_db.test_schema.test_table_1)",
                "the FROM clause has multiple tables (no JOINs, subqueries or functions allowed).",
            ),
            (
                "SELECT SUM(test_column_2) FROM TABLE(f())",
                "the FROM clause has multiple tables (no JOINs, subqueries or functions allowed).",
            ),
            (
                "SELECT SUM(test_column_2) FROM (test_schema.test_table_1 CROSS JOIN treasury.attachment)",
                "the FROM clause has multiple tables (no JOINs, subqueries or functions allowed).",
            ),
            (
                "SELECT SUM(f) FROM test_db.test_schema.test_table_1 AS d (f, g)",
                "table aliases with columns (such as d (f, g)).",
            ),
            (
                "SELECT SUM(test_column_2), AVG(test_column_2) FROM test_db.test_schema.test_table_1",
                "the SELECT clause must contain exactly one aggregation / analytic function. Nothing else is accepted.",
            ),
            (
                "SELECT drda.* FROM test_db.test_schema.test_table_1",
                "the SELECT clause must contain exactly one aggregation / analytic function. Nothing else is accepted.",
            ),
            (
                "SELECT * FROM test_db.test_schema.test_table_1",
                "the SELECT clause must contain exactly one aggregation / analytic function. Nothing else is accepted.",
            ),
            (
                "SELECT id FROM test_db.test_schema.test_table_1",
                "the SELECT clause must contain exactly one aggregation / analytic function. Nothing else is accepted.",
            ),
            (
                "SELECT SUM(test_column_2) OVER (PARTITION BY id) FROM test_db.test_schema.test_table_1",
                "window functions (OVER).",
            ),
            (
                "SELECT SUM(DISTINCT test_column_2) FROM test_db.test_schema.test_table_1",
                "DISTINCT.",
            ),
            (
                "SELECT custom.aggregation(test_column_2) FROM test_db.test_schema.test_table_1",
                "unrecognized or unsupported function: custom.aggregation.",
            ),
            (
                "SELECT SUM(x => test_column_2) FROM test_db.test_schema.test_table_1",
                "named function arguments (such as x => test_column_2).",
            ),
            (
                "SELECT SUM(1) FROM test_db.test_schema.test_table_1",
                "only a column name is supported as the argument of the SUM function.",
            ),
            (
                "SELECT SUM(test_table_1.*) FROM test_db.test_schema.test_table_1",
                "only a column name is supported as the argument of the SUM function.",
            ),
            (
                "SELECT SUM(*) FROM test_db.test_schema.test_table_1",
                "only a column name is supported as the argument of the SUM function.",
            ),
            (
                "INSERT INTO test_table_1 SELECT * FROM test_db.test_schema.test_table_1",
                "statements different from single SELECT statement.",
            ),
            (
                "CREATE TABLE test_table_1 AS SELECT * FROM test_db.test_schema.test_table_1",
                "statements different from single SELECT statement.",
            ),
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 WHERE test_column_2 BETWEEN 1 AND 2",
                "unsupported expression in the WHERE clause: test_column_2 BETWEEN 1 AND 2.",
            ),
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 WHERE 2 < 1",
                "2 < 1. Only comparisons between a column and a constant are supported.",
            ),
            (
                "SELECT SUM(test_column_2) FROM test_db.test_schema.test_table_1 WHERE test_column_2 < test_column_3",
                "test_column_2 < test_column_3. Only comparisons between a column and a constant are supported.",
            ),
            // Unsupported functions
            (
                "SELECT MIN(test_column_2) FROM test_db.test_schema.test_table_1;",
                "unrecognized or unsupported function: MIN."
            ),
            (
                "SELECT MAX(test_column_2) FROM test_db.test_schema.test_table_1;",
                "unrecognized or unsupported function: MAX."
            ),
            (
                "SELECT KTHELEMENT(test_column_2, 3) FROM test_db.test_schema.test_table_1;",
                "unrecognized or unsupported function: KTHELEMENT."
            )
        ];

        for (query, reason) in cases {
            let expected = Err(unsupported!(reason.to_string()));
            assert_eq!(
                QueryMetadata::parse(query, None),
                expected,
                "\nfailed for query {query:?}",
            );
        }
    }

    #[test]
    fn aggregation_with_single_where() {
        let cases = [
            (
                "test_column_2 < 1",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::Lt {
                        value: "1".to_string(),
                    },
                },
            ),
            (
                "1 < test_column_2",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::Gt {
                        value: "1".to_string(),
                    },
                },
            ),
            (
                "test_column_2 <= 1",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::LtEq {
                        value: "1".to_string(),
                    },
                },
            ),
            (
                "1 <= test_column_2",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::GtEq {
                        value: "1".to_string(),
                    },
                },
            ),
            (
                "test_column_2 > 1",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::Gt {
                        value: "1".to_string(),
                    },
                },
            ),
            (
                "1 > test_column_2",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::Lt {
                        value: "1".to_string(),
                    },
                },
            ),
            (
                "test_column_2 >= 1",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::GtEq {
                        value: "1".to_string(),
                    },
                },
            ),
            (
                "1 >= test_column_2",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::LtEq {
                        value: "1".to_string(),
                    },
                },
            ),
            (
                "test_column_3 > '2021-04-02T05:02:16.04+03:00'",
                Filter {
                    column: "test_column_3".to_string(),
                    comparison: CompareOp::Gt {
                        value: "2021-04-02T05:02:16.04+03:00".to_string(),
                    },
                },
            ),
            (
                "-1 >= test_column_4",
                Filter {
                    column: "test_column_4".to_string(),
                    comparison: CompareOp::LtEq {
                        value: "-1".to_string(),
                    },
                },
            ),
            (
                "+1 >= test_column_2",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::LtEq {
                        value: "1".to_string(),
                    },
                },
            ),
            (
                "+1 = test_column_2",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::Eq {
                        value: "1".to_string(),
                    },
                },
            ),
            (
                "+1 <> test_column_2",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::NotEq {
                        value: "1".to_string(),
                    },
                },
            ),
            (
                "test_column_2 IS NULL",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::IsNull,
                },
            ),
            (
                "test_column_2 IS NOT NULL",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::IsNotNull,
                },
            ),
            (
                "test_column_1 = NULL",
                Filter {
                    column: "test_column_1".to_string(),
                    comparison: CompareOp::Eq {
                        value: "Null".to_string(),
                    },
                },
            ),
            (
                "test_column_2 = NULL",
                Filter {
                    column: "test_column_2".to_string(),
                    comparison: CompareOp::Eq {
                        value: "Null".to_string(),
                    },
                },
            ),
            (
                "test_column_3 = NULL",
                Filter {
                    column: "test_column_3".to_string(),
                    comparison: CompareOp::Eq {
                        value: "Null".to_string(),
                    },
                },
            ),
            (
                "test_column_4 = NULL",
                Filter {
                    column: "test_column_4".to_string(),
                    comparison: CompareOp::Eq {
                        value: "Null".to_string(),
                    },
                },
            ),
            (
                "test_column_5 IS TRUE",
                Filter {
                    column: "test_column_5".to_string(),
                    comparison: CompareOp::IsTrue,
                },
            ),
            (
                "test_column_5 IS NOT TRUE",
                Filter {
                    column: "test_column_5".to_string(),
                    comparison: CompareOp::IsNotTrue,
                },
            ),
            (
                "test_column_5 = true",
                Filter {
                    column: "test_column_5".to_string(),
                    comparison: CompareOp::Eq {
                        value: "true".to_string(),
                    },
                },
            ),
            (
                "test_column_5 <> true",
                Filter {
                    column: "test_column_5".to_string(),
                    comparison: CompareOp::NotEq {
                        value: "true".to_string(),
                    },
                },
            ),
            (
                "test_column_5 IS FALSE",
                Filter {
                    column: "test_column_5".to_string(),
                    comparison: CompareOp::IsFalse,
                },
            ),
            (
                "test_column_5 IS NOT FALSE",
                Filter {
                    column: "test_column_5".to_string(),
                    comparison: CompareOp::IsNotFalse,
                },
            ),
            (
                "test_column_5 = false",
                Filter {
                    column: "test_column_5".to_string(),
                    comparison: CompareOp::Eq {
                        value: "false".to_string(),
                    },
                },
            ),
            (
                "test_column_5 <> false",
                Filter {
                    column: "test_column_5".to_string(),
                    comparison: CompareOp::NotEq {
                        value: "false".to_string(),
                    },
                },
            ),
        ];

        let analytical_functions = [("SUM", KoronFunction::Sum), ("COUNT", KoronFunction::Count)];

        let test_cases = |enum_fn: KoronFunction, query: &String| {
            for (selection, filter) in cases.clone() {
                let query = &format!("{query} WHERE {selection}");
                let mut aggregation = sample_sum();
                aggregation.function = enum_fn;
                let expected_query = if &filter.column == "test_column_2" {
                    "SELECT test_column_2 FROM test_db.test_schema.test_table_1".to_string()
                } else {
                    format!(
                        "SELECT test_column_2, {} FROM test_db.test_schema.test_table_1",
                        filter.column
                    )
                };
                let expected = QueryMetadata {
                    table: sample_tab_ident(),
                    aggregation,
                    filter: Some(filter.clone()),
                    data_extraction_query: expected_query,
                    data_aggregation_query: None,
                };
                let result = QueryMetadata::parse(query, None).unwrap();
                assert_eq!(
                    result.aggregation, expected.aggregation,
                    "\nfailed for selection {selection:?}",
                );
                assert_eq!(
                    result.table, expected.table,
                    "\nfailed for selection {selection:?}",
                );
                assert_eq!(
                    result.filter, expected.filter,
                    "\nfailed for selection {selection:?}",
                );
                assert_eq!(
                    result.data_extraction_query, expected.data_extraction_query,
                    "\nfailed for selection {selection:?}",
                );
            }
        };

        for (function, enum_fn) in analytical_functions {
            let query =
                format!("SELECT {function}(test_column_2) FROM test_db.test_schema.test_table_1");
            test_cases(enum_fn, &query);
        }
    }
}
