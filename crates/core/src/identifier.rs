use regex::Regex;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::ops::Deref;
use std::sync::LazyLock;
use thiserror::Error;

static IDENT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]{0,63}$").unwrap());

// Common dangerous SQL keywords that cannot be bare table/column identifiers
static SQL_RESERVED: LazyLock<std::collections::HashSet<&'static str>> = LazyLock::new(|| {
    let mut s = std::collections::HashSet::new();
    s.insert("select");
    s.insert("insert");
    s.insert("update");
    s.insert("delete");
    s.insert("drop");
    s.insert("alter");
    s.insert("truncate");
    s.insert("where");
    s.insert("from");
    s.insert("table");
    s.insert("database");
    s.insert("union");
    s.insert("join");
    s.insert("having");
    s.insert("group");
    s.insert("order");
    s.insert("grant");
    s.insert("revoke");
    s
});

#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum IdentifierError {
    #[error("Identifier '{0}' is invalid: must match ^[a-zA-Z_][a-zA-Z0-9_]{{0,63}}$ and contain no spaces, hyphens, or SQL punctuation")]
    InvalidPattern(String),
    #[error("Identifier '{0}' is a reserved SQL keyword and cannot be used as an unquoted schema identifier")]
    ReservedKeyword(String),
}

/// A validated schema/column/table identifier that is mathematically guaranteed
/// to be safe from SQL injection when building queries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ValidatedIdentifier(String);

impl ValidatedIdentifier {
    pub fn new(s: impl Into<String>) -> Result<Self, IdentifierError> {
        let s = s.into();
        if !IDENT_RE.is_match(&s) {
            return Err(IdentifierError::InvalidPattern(s));
        }
        if SQL_RESERVED.contains(s.to_lowercase().as_str()) {
            return Err(IdentifierError::ReservedKeyword(s));
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for ValidatedIdentifier {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for ValidatedIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for ValidatedIdentifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ValidatedIdentifier::new(s).map_err(de::Error::custom)
    }
}

impl Serialize for ValidatedIdentifier {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_identifiers() {
        assert!(ValidatedIdentifier::new("orders").is_ok());
        assert!(ValidatedIdentifier::new("sale_id").is_ok());
        assert!(ValidatedIdentifier::new("_tenant_id_123").is_ok());
        assert!(ValidatedIdentifier::new("amountPaise").is_ok());
    }

    #[test]
    fn test_invalid_identifiers() {
        assert!(ValidatedIdentifier::new("orders; DROP TABLE").is_err());
        assert!(ValidatedIdentifier::new("sale-id").is_err()); // hyphen not allowed
        assert!(ValidatedIdentifier::new("123orders").is_err()); // cannot start with digit
        assert!(ValidatedIdentifier::new("orders' OR '1'='1").is_err());
        assert!(ValidatedIdentifier::new("order status").is_err()); // space not allowed
    }

    #[test]
    fn test_reserved_keywords() {
        assert_eq!(
            ValidatedIdentifier::new("SELECT").unwrap_err(),
            IdentifierError::ReservedKeyword("SELECT".to_string())
        );
        assert_eq!(
            ValidatedIdentifier::new("drop").unwrap_err(),
            IdentifierError::ReservedKeyword("drop".to_string())
        );
    }
}

