use thiserror::Error;

/// Koron errors.
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("malformed query: {message}")]
    MalformedQuery { message: String },
    #[error("statement not supported: {message}")]
    Unsupported { message: String },
    #[error("internal: {message}")]
    Internal { message: String },
}

macro_rules! impl_malformed_from {
    ($err:ty) => {
        impl From<$err> for ParseError {
            fn from(e: $err) -> Self {
                Self::MalformedQuery {
                    message: e.to_string(),
                }
            }
        }
    };
}

impl_malformed_from!(sqlparser::parser::ParserError);

impl From<String> for ParseError {
    fn from(e: String) -> Self {
        Self::Internal { message: e }
    }
}

/// Constructs a `ParseError::Unsupported{message: $msg}`.
#[macro_export]
macro_rules! unsupported {
    ($msg:literal) => {{
        ParseError::Unsupported { message: $msg }
    }};
    ($msg:expr) => {{
        ParseError::Unsupported { message: $msg }
    }};
}

/// Constructs a `ParseError::Internal{message: $msg}`.
#[macro_export]
macro_rules! internal {
    ($msg:literal) => {{
        ParseError::Internal { message: $msg }
    }};
    ($msg:expr) => {{
        ParseError::Internal { message: $msg }
    }};
}

/// Constructs a `ParseError::MalformedQuery{message: $msg}`.
#[macro_export]
macro_rules! malformed_query {
    ($msg:literal) => {{
        ParseError::MalformedQuery { message: $msg }
    }};
    ($msg:expr) => {{
        ParseError::MalformedQuery { message: $msg }
    }};
}

#[cfg(test)]
mod tests {
    use super::ParseError;

    #[test]
    fn to_string() {
        let mut error = internal!("test.".to_string());
        assert_eq!(error.to_string(), "internal: test.".to_string());

        error = malformed_query!("test.".to_string());
        assert_eq!(error.to_string(), "malformed query: test.".to_string());

        error = unsupported!("test.".to_string());
        assert_eq!(
            error.to_string(),
            "statement not supported: test.".to_string()
        );
    }
}
