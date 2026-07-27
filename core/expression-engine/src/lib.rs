//! A constrained, deterministic expression language.
//!
//! The language deliberately has no user-defined functions, I/O, mutation, or
//! arbitrary code execution. It supports boolean composition, comparisons,
//! existence checks, literals, and bounded JSON-style path traversal.

use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use thiserror::Error;

/// Parser resource limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpressionLimits {
    /// Maximum UTF-8 source size.
    pub max_input_bytes: usize,
    /// Maximum AST depth.
    pub max_depth: usize,
    /// Maximum AST node count.
    pub max_nodes: usize,
    /// Maximum decoded string literal size.
    pub max_string_bytes: usize,
    /// Maximum number of segments in one environment path.
    pub max_path_segments: usize,
}

impl Default for ExpressionLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 16 * 1024,
            max_depth: 64,
            max_nodes: 1_024,
            max_string_bytes: 4 * 1024,
            max_path_segments: 64,
        }
    }
}

/// A parsed expression.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "expression", content = "value", rename_all = "snake_case")]
pub enum Expression {
    /// A boolean-producing operand.
    Value(Operand),
    /// Boolean negation.
    Not(Box<Self>),
    /// Short-circuiting conjunction.
    And(Box<Self>, Box<Self>),
    /// Short-circuiting disjunction.
    Or(Box<Self>, Box<Self>),
    /// Typed comparison.
    Compare {
        /// Left operand.
        left: Operand,
        /// Comparison operator.
        operator: ComparisonOperator,
        /// Right operand.
        right: Operand,
    },
    /// Returns whether a path resolves.
    Exists(EnvironmentPath),
}

impl Expression {
    /// Parses a bounded expression with explicit limits.
    ///
    /// # Errors
    ///
    /// Returns [`ParseError`] for invalid syntax or any exceeded resource limit.
    pub fn parse(source: &str, limits: ExpressionLimits) -> Result<Self, ParseError> {
        if source.len() > limits.max_input_bytes {
            return Err(ParseError::InputTooLong {
                actual: source.len(),
                maximum: limits.max_input_bytes,
            });
        }
        if limits.max_nodes == 0 || limits.max_depth == 0 || limits.max_path_segments == 0 {
            return Err(ParseError::InvalidLimits);
        }
        let tokens = Lexer::new(source, limits).tokenize()?;
        let mut parser = Parser::new(tokens, limits);
        let expression = parser.parse_expression()?;
        parser.expect_end()?;
        let depth = expression_depth(&expression);
        if depth > limits.max_depth {
            return Err(ParseError::DepthExceeded {
                maximum: limits.max_depth,
            });
        }
        Ok(expression)
    }

    /// Evaluates the expression against a JSON-like immutable environment.
    ///
    /// # Errors
    ///
    /// Returns [`EvaluationError`] when a referenced path is missing, an operand
    /// is not boolean where required, or comparison types are incompatible.
    pub fn evaluate(&self, environment: &Value) -> Result<bool, EvaluationError> {
        evaluate_expression(self, environment)
    }
}

/// An expression operand.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operand", content = "value", rename_all = "snake_case")]
pub enum Operand {
    /// A literal value.
    Literal(Literal),
    /// A value selected from the evaluation environment.
    Path(EnvironmentPath),
}

/// A deterministic literal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Literal {
    /// JSON null.
    Null,
    /// Boolean.
    Boolean(bool),
    /// JSON number.
    Number(Number),
    /// UTF-8 string.
    String(String),
}

/// Comparison operators.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOperator {
    /// Equal.
    Equal,
    /// Not equal.
    NotEqual,
    /// Less than.
    Less,
    /// Less than or equal.
    LessOrEqual,
    /// Greater than.
    Greater,
    /// Greater than or equal.
    GreaterOrEqual,
}

/// A bounded path into a JSON-like environment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentPath {
    segments: Vec<PathSegment>,
}

impl EnvironmentPath {
    /// Returns the path segments.
    #[must_use]
    pub fn segments(&self) -> &[PathSegment] {
        &self.segments
    }
}

impl fmt::Display for EnvironmentPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, segment) in self.segments.iter().enumerate() {
            match segment {
                PathSegment::Key(key) if index == 0 => formatter.write_str(key)?,
                PathSegment::Key(key) => write!(formatter, ".{key}")?,
                PathSegment::Index(value) => write!(formatter, "[{value}]")?,
            }
        }
        Ok(())
    }
}

/// One environment path segment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "segment", content = "value", rename_all = "snake_case")]
pub enum PathSegment {
    /// Object key.
    Key(String),
    /// Array index.
    Index(usize),
}

/// Deterministic parse failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ParseError {
    /// Source exceeds the configured byte limit.
    #[error("expression input is {actual} bytes; maximum is {maximum}")]
    InputTooLong {
        /// Actual source bytes.
        actual: usize,
        /// Configured maximum.
        maximum: usize,
    },
    /// A configured limit cannot admit any valid expression.
    #[error("expression limits must allow at least one node, depth, and path segment")]
    InvalidLimits,
    /// Decoded string exceeds the configured limit.
    #[error("string at byte {position} exceeds {maximum} bytes")]
    StringTooLong {
        /// Start byte.
        position: usize,
        /// Configured maximum.
        maximum: usize,
    },
    /// String is not terminated.
    #[error("unterminated string at byte {position}")]
    UnterminatedString {
        /// Start byte.
        position: usize,
    },
    /// String escaping is invalid JSON.
    #[error("invalid string at byte {position}")]
    InvalidString {
        /// Start byte.
        position: usize,
    },
    /// Numeric syntax is invalid JSON.
    #[error("invalid number at byte {position}")]
    InvalidNumber {
        /// Start byte.
        position: usize,
    },
    /// Lexer encountered an unsupported character.
    #[error("unexpected character `{character}` at byte {position}")]
    UnexpectedCharacter {
        /// Source byte.
        position: usize,
        /// Unsupported character.
        character: char,
    },
    /// Parser encountered a different token than required.
    #[error("expected {expected} at byte {position}, found {found}")]
    UnexpectedToken {
        /// Token byte.
        position: usize,
        /// Deterministic expectation.
        expected: &'static str,
        /// Deterministic token description.
        found: String,
    },
    /// Source contains tokens after a complete expression.
    #[error("trailing input at byte {position}")]
    TrailingInput {
        /// First trailing token byte.
        position: usize,
    },
    /// AST node limit was exceeded.
    #[error("expression exceeds {maximum} AST nodes")]
    NodeLimitExceeded {
        /// Configured maximum.
        maximum: usize,
    },
    /// AST depth limit was exceeded.
    #[error("expression exceeds maximum depth {maximum}")]
    DepthExceeded {
        /// Configured maximum.
        maximum: usize,
    },
    /// A path contains too many segments.
    #[error("path at byte {position} exceeds {maximum} segments")]
    PathSegmentsExceeded {
        /// First path byte.
        position: usize,
        /// Configured maximum.
        maximum: usize,
    },
    /// Array index cannot fit in the platform-independent parser bound.
    #[error("array index at byte {position} is invalid")]
    InvalidArrayIndex {
        /// Index byte.
        position: usize,
    },
}

/// Deterministic evaluation failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EvaluationError {
    /// A non-existence operand references a missing path.
    #[error("environment path `{path}` does not exist")]
    MissingPath {
        /// Stable path text.
        path: String,
    },
    /// Boolean context received another value type.
    #[error("expected boolean, found {actual}")]
    ExpectedBoolean {
        /// Actual type.
        actual: &'static str,
    },
    /// Comparison operands have different types.
    #[error("cannot compare {left} with {right}")]
    TypeMismatch {
        /// Left type.
        left: &'static str,
        /// Right type.
        right: &'static str,
    },
    /// Ordering is undefined for the operand type.
    #[error("ordering comparison is not supported for {actual}")]
    UnsupportedOrdering {
        /// Operand type.
        actual: &'static str,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Token {
    kind: TokenKind,
    position: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TokenKind {
    Identifier(String),
    String(String),
    Number(Number),
    True,
    False,
    Null,
    Exists,
    And,
    Or,
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Bang,
    LeftParen,
    RightParen,
    Dot,
    LeftBracket,
    RightBracket,
    End,
}

impl TokenKind {
    fn description(&self) -> String {
        match self {
            Self::Identifier(value) => format!("identifier `{value}`"),
            Self::String(_) => "string".into(),
            Self::Number(_) => "number".into(),
            Self::True => "`true`".into(),
            Self::False => "`false`".into(),
            Self::Null => "`null`".into(),
            Self::Exists => "`exists`".into(),
            Self::And => "`&&`".into(),
            Self::Or => "`||`".into(),
            Self::Equal => "`==`".into(),
            Self::NotEqual => "`!=`".into(),
            Self::Less => "`<`".into(),
            Self::LessOrEqual => "`<=`".into(),
            Self::Greater => "`>`".into(),
            Self::GreaterOrEqual => "`>=`".into(),
            Self::Bang => "`!`".into(),
            Self::LeftParen => "`(`".into(),
            Self::RightParen => "`)`".into(),
            Self::Dot => "`.`".into(),
            Self::LeftBracket => "`[`".into(),
            Self::RightBracket => "`]`".into(),
            Self::End => "end of input".into(),
        }
    }
}

struct Lexer<'a> {
    source: &'a str,
    offset: usize,
    limits: ExpressionLimits,
}

impl<'a> Lexer<'a> {
    const fn new(source: &'a str, limits: ExpressionLimits) -> Self {
        Self {
            source,
            offset: 0,
            limits,
        }
    }

    fn tokenize(mut self) -> Result<Vec<Token>, ParseError> {
        let mut tokens = Vec::new();
        while self.offset < self.source.len() {
            self.skip_whitespace();
            if self.offset == self.source.len() {
                break;
            }
            tokens.push(self.next_token()?);
        }
        tokens.push(Token {
            kind: TokenKind::End,
            position: self.source.len(),
        });
        Ok(tokens)
    }

    fn skip_whitespace(&mut self) {
        while let Some(character) = self.source[self.offset..].chars().next() {
            if !character.is_whitespace() {
                break;
            }
            self.offset += character.len_utf8();
        }
    }

    fn next_token(&mut self) -> Result<Token, ParseError> {
        let position = self.offset;
        let remaining = &self.source[position..];
        for (text, kind) in [
            ("&&", TokenKind::And),
            ("||", TokenKind::Or),
            ("==", TokenKind::Equal),
            ("!=", TokenKind::NotEqual),
            ("<=", TokenKind::LessOrEqual),
            (">=", TokenKind::GreaterOrEqual),
        ] {
            if remaining.starts_with(text) {
                self.offset += text.len();
                return Ok(Token { kind, position });
            }
        }
        let character = remaining.chars().next().expect("offset is in source");
        let kind = match character {
            '!' => TokenKind::Bang,
            '<' => TokenKind::Less,
            '>' => TokenKind::Greater,
            '(' => TokenKind::LeftParen,
            ')' => TokenKind::RightParen,
            '.' => TokenKind::Dot,
            '[' => TokenKind::LeftBracket,
            ']' => TokenKind::RightBracket,
            '"' => return self.string_token(position),
            '-' | '0'..='9' => return self.number_token(position),
            value if value.is_ascii_alphabetic() || value == '_' => {
                return Ok(self.identifier_token(position));
            }
            _ => {
                return Err(ParseError::UnexpectedCharacter {
                    position,
                    character,
                });
            }
        };
        self.offset += character.len_utf8();
        Ok(Token { kind, position })
    }

    fn identifier_token(&mut self, position: usize) -> Token {
        while let Some(character) = self.source[self.offset..].chars().next() {
            if !(character.is_ascii_alphanumeric() || matches!(character, '_' | '-')) {
                break;
            }
            self.offset += character.len_utf8();
        }
        let value = &self.source[position..self.offset];
        let kind = match value {
            "true" => TokenKind::True,
            "false" => TokenKind::False,
            "null" => TokenKind::Null,
            "exists" => TokenKind::Exists,
            _ => TokenKind::Identifier(value.to_owned()),
        };
        Token { kind, position }
    }

    fn string_token(&mut self, position: usize) -> Result<Token, ParseError> {
        self.offset += 1;
        let bytes = self.source.as_bytes();
        let mut escaped = false;
        while self.offset < bytes.len() {
            let byte = bytes[self.offset];
            self.offset += 1;
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                let encoded = &self.source[position..self.offset];
                let decoded: String = serde_json::from_str(encoded)
                    .map_err(|_| ParseError::InvalidString { position })?;
                if decoded.len() > self.limits.max_string_bytes {
                    return Err(ParseError::StringTooLong {
                        position,
                        maximum: self.limits.max_string_bytes,
                    });
                }
                return Ok(Token {
                    kind: TokenKind::String(decoded),
                    position,
                });
            }
        }
        Err(ParseError::UnterminatedString { position })
    }

    fn number_token(&mut self, position: usize) -> Result<Token, ParseError> {
        while let Some(character) = self.source[self.offset..].chars().next() {
            if !(character.is_ascii_digit() || matches!(character, '-' | '+' | '.' | 'e' | 'E')) {
                break;
            }
            self.offset += character.len_utf8();
        }
        let encoded = &self.source[position..self.offset];
        let number: Number =
            serde_json::from_str(encoded).map_err(|_| ParseError::InvalidNumber { position })?;
        Ok(Token {
            kind: TokenKind::Number(number),
            position,
        })
    }
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
    limits: ExpressionLimits,
    nodes: usize,
}

impl Parser {
    const fn new(tokens: Vec<Token>, limits: ExpressionLimits) -> Self {
        Self {
            tokens,
            index: 0,
            limits,
            nodes: 0,
        }
    }

    fn parse_expression(&mut self) -> Result<Expression, ParseError> {
        self.parse_or(1)
    }

    fn parse_or(&mut self, depth: usize) -> Result<Expression, ParseError> {
        self.ensure_depth(depth)?;
        let mut expression = self.parse_and(depth)?;
        while matches!(self.current().kind, TokenKind::Or) {
            self.advance();
            let right = self.parse_and(depth)?;
            expression = self.node(Expression::Or(Box::new(expression), Box::new(right)))?;
        }
        Ok(expression)
    }

    fn parse_and(&mut self, depth: usize) -> Result<Expression, ParseError> {
        let mut expression = self.parse_unary(depth)?;
        while matches!(self.current().kind, TokenKind::And) {
            self.advance();
            let right = self.parse_unary(depth)?;
            expression = self.node(Expression::And(Box::new(expression), Box::new(right)))?;
        }
        Ok(expression)
    }

    fn parse_unary(&mut self, depth: usize) -> Result<Expression, ParseError> {
        self.ensure_depth(depth)?;
        if matches!(self.current().kind, TokenKind::Bang) {
            self.advance();
            let expression = self.parse_unary(depth + 1)?;
            return self.node(Expression::Not(Box::new(expression)));
        }
        self.parse_primary(depth)
    }

    fn parse_primary(&mut self, depth: usize) -> Result<Expression, ParseError> {
        if matches!(self.current().kind, TokenKind::LeftParen) {
            self.advance();
            let expression = self.parse_or(depth + 1)?;
            self.expect(TokenDiscriminant::RightParen, "`)`")?;
            return Ok(expression);
        }
        if matches!(self.current().kind, TokenKind::Exists) {
            self.advance();
            self.expect(TokenDiscriminant::LeftParen, "`(`")?;
            let path = self.parse_path()?;
            self.expect(TokenDiscriminant::RightParen, "`)`")?;
            return self.node(Expression::Exists(path));
        }

        let left = self.parse_operand()?;
        let operator = match self.current().kind {
            TokenKind::Equal => Some(ComparisonOperator::Equal),
            TokenKind::NotEqual => Some(ComparisonOperator::NotEqual),
            TokenKind::Less => Some(ComparisonOperator::Less),
            TokenKind::LessOrEqual => Some(ComparisonOperator::LessOrEqual),
            TokenKind::Greater => Some(ComparisonOperator::Greater),
            TokenKind::GreaterOrEqual => Some(ComparisonOperator::GreaterOrEqual),
            _ => None,
        };
        if let Some(operator) = operator {
            self.advance();
            let right = self.parse_operand()?;
            self.node(Expression::Compare {
                left,
                operator,
                right,
            })
        } else {
            self.node(Expression::Value(left))
        }
    }

    fn parse_operand(&mut self) -> Result<Operand, ParseError> {
        let token = self.current().clone();
        match token.kind {
            TokenKind::True => {
                self.advance();
                Ok(Operand::Literal(Literal::Boolean(true)))
            }
            TokenKind::False => {
                self.advance();
                Ok(Operand::Literal(Literal::Boolean(false)))
            }
            TokenKind::Null => {
                self.advance();
                Ok(Operand::Literal(Literal::Null))
            }
            TokenKind::String(value) => {
                self.advance();
                Ok(Operand::Literal(Literal::String(value)))
            }
            TokenKind::Number(value) => {
                self.advance();
                Ok(Operand::Literal(Literal::Number(value)))
            }
            TokenKind::Identifier(_) => self.parse_path().map(Operand::Path),
            _ => Err(self.unexpected("an operand")),
        }
    }

    fn parse_path(&mut self) -> Result<EnvironmentPath, ParseError> {
        let start = self.current().position;
        let TokenKind::Identifier(key) = self.current().kind.clone() else {
            return Err(self.unexpected("a path"));
        };
        self.advance();
        let mut segments = vec![PathSegment::Key(key)];
        loop {
            match self.current().kind {
                TokenKind::Dot => {
                    self.advance();
                    let TokenKind::Identifier(key) = self.current().kind.clone() else {
                        return Err(self.unexpected("a path key"));
                    };
                    self.advance();
                    segments.push(PathSegment::Key(key));
                }
                TokenKind::LeftBracket => {
                    self.advance();
                    let token = self.current().clone();
                    let TokenKind::Number(number) = token.kind else {
                        return Err(self.unexpected("an array index"));
                    };
                    let index = number
                        .as_u64()
                        .and_then(|value| usize::try_from(value).ok())
                        .ok_or(ParseError::InvalidArrayIndex {
                            position: token.position,
                        })?;
                    self.advance();
                    self.expect(TokenDiscriminant::RightBracket, "`]`")?;
                    segments.push(PathSegment::Index(index));
                }
                _ => break,
            }
            if segments.len() > self.limits.max_path_segments {
                return Err(ParseError::PathSegmentsExceeded {
                    position: start,
                    maximum: self.limits.max_path_segments,
                });
            }
        }
        Ok(EnvironmentPath { segments })
    }

    fn node(&mut self, expression: Expression) -> Result<Expression, ParseError> {
        self.nodes += 1;
        if self.nodes > self.limits.max_nodes {
            Err(ParseError::NodeLimitExceeded {
                maximum: self.limits.max_nodes,
            })
        } else {
            Ok(expression)
        }
    }

    fn ensure_depth(&self, depth: usize) -> Result<(), ParseError> {
        if depth > self.limits.max_depth {
            Err(ParseError::DepthExceeded {
                maximum: self.limits.max_depth,
            })
        } else {
            Ok(())
        }
    }

    fn expect(
        &mut self,
        expected: TokenDiscriminant,
        description: &'static str,
    ) -> Result<(), ParseError> {
        if expected.matches(&self.current().kind) {
            self.advance();
            Ok(())
        } else {
            Err(self.unexpected(description))
        }
    }

    fn expect_end(&self) -> Result<(), ParseError> {
        if matches!(self.current().kind, TokenKind::End) {
            Ok(())
        } else {
            Err(ParseError::TrailingInput {
                position: self.current().position,
            })
        }
    }

    fn unexpected(&self, expected: &'static str) -> ParseError {
        ParseError::UnexpectedToken {
            position: self.current().position,
            expected,
            found: self.current().kind.description(),
        }
    }

    fn current(&self) -> &Token {
        &self.tokens[self.index]
    }

    fn advance(&mut self) {
        self.index += 1;
    }
}

#[derive(Clone, Copy)]
enum TokenDiscriminant {
    LeftParen,
    RightParen,
    RightBracket,
}

impl TokenDiscriminant {
    const fn matches(self, token: &TokenKind) -> bool {
        matches!(
            (self, token),
            (Self::LeftParen, TokenKind::LeftParen)
                | (Self::RightParen, TokenKind::RightParen)
                | (Self::RightBracket, TokenKind::RightBracket)
        )
    }
}

fn expression_depth(expression: &Expression) -> usize {
    match expression {
        Expression::Value(_) | Expression::Compare { .. } | Expression::Exists(_) => 1,
        Expression::Not(inner) => 1 + expression_depth(inner),
        Expression::And(left, right) | Expression::Or(left, right) => {
            1 + expression_depth(left).max(expression_depth(right))
        }
    }
}

fn evaluate_expression(
    expression: &Expression,
    environment: &Value,
) -> Result<bool, EvaluationError> {
    match expression {
        Expression::Value(operand) => match resolve_operand(operand, environment)? {
            Resolved::Null => Err(EvaluationError::ExpectedBoolean { actual: "null" }),
            Resolved::Boolean(value) => Ok(value),
            Resolved::Number(_) => Err(EvaluationError::ExpectedBoolean { actual: "number" }),
            Resolved::String(_) => Err(EvaluationError::ExpectedBoolean { actual: "string" }),
        },
        Expression::Not(inner) => Ok(!evaluate_expression(inner, environment)?),
        Expression::And(left, right) => {
            Ok(evaluate_expression(left, environment)? && evaluate_expression(right, environment)?)
        }
        Expression::Or(left, right) => {
            Ok(evaluate_expression(left, environment)? || evaluate_expression(right, environment)?)
        }
        Expression::Compare {
            left,
            operator,
            right,
        } => compare(
            resolve_operand(left, environment)?,
            *operator,
            resolve_operand(right, environment)?,
        ),
        Expression::Exists(path) => Ok(lookup(environment, path).is_some()),
    }
}

#[derive(Clone, Copy)]
enum Resolved<'a> {
    Null,
    Boolean(bool),
    Number(&'a Number),
    String(&'a str),
}

impl Resolved<'_> {
    const fn kind(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Boolean(_) => "boolean",
            Self::Number(_) => "number",
            Self::String(_) => "string",
        }
    }
}

fn resolve_operand<'a>(
    operand: &'a Operand,
    environment: &'a Value,
) -> Result<Resolved<'a>, EvaluationError> {
    match operand {
        Operand::Literal(literal) => Ok(match literal {
            Literal::Null => Resolved::Null,
            Literal::Boolean(value) => Resolved::Boolean(*value),
            Literal::Number(value) => Resolved::Number(value),
            Literal::String(value) => Resolved::String(value),
        }),
        Operand::Path(path) => {
            let value = lookup(environment, path).ok_or_else(|| EvaluationError::MissingPath {
                path: path.to_string(),
            })?;
            Ok(match value {
                Value::Null => Resolved::Null,
                Value::Bool(value) => Resolved::Boolean(*value),
                Value::Number(value) => Resolved::Number(value),
                Value::String(value) => Resolved::String(value),
                Value::Array(_) => {
                    return Err(EvaluationError::ExpectedBoolean { actual: "array" });
                }
                Value::Object(_) => {
                    return Err(EvaluationError::ExpectedBoolean { actual: "object" });
                }
            })
        }
    }
}

fn lookup<'a>(environment: &'a Value, path: &EnvironmentPath) -> Option<&'a Value> {
    let mut current = environment;
    for segment in &path.segments {
        current = match segment {
            PathSegment::Key(key) => current.as_object()?.get(key)?,
            PathSegment::Index(index) => current.as_array()?.get(*index)?,
        };
    }
    Some(current)
}

fn compare(
    left: Resolved<'_>,
    operator: ComparisonOperator,
    right: Resolved<'_>,
) -> Result<bool, EvaluationError> {
    let left_kind = left.kind();
    let right_kind = right.kind();
    if left_kind != right_kind {
        return Err(EvaluationError::TypeMismatch {
            left: left_kind,
            right: right_kind,
        });
    }
    let ordering = match (&left, &right) {
        (Resolved::Number(left), Resolved::Number(right)) => compare_numbers(left, right),
        (Resolved::String(left), Resolved::String(right)) => Some(left.cmp(right)),
        _ => None,
    };
    match operator {
        ComparisonOperator::Equal => Ok(equal_values(&left, &right)),
        ComparisonOperator::NotEqual => Ok(!equal_values(&left, &right)),
        ComparisonOperator::Less => ordering
            .map(|value| value == Ordering::Less)
            .ok_or(EvaluationError::UnsupportedOrdering { actual: left_kind }),
        ComparisonOperator::LessOrEqual => ordering
            .map(|value| value != Ordering::Greater)
            .ok_or(EvaluationError::UnsupportedOrdering { actual: left_kind }),
        ComparisonOperator::Greater => ordering
            .map(|value| value == Ordering::Greater)
            .ok_or(EvaluationError::UnsupportedOrdering { actual: left_kind }),
        ComparisonOperator::GreaterOrEqual => ordering
            .map(|value| value != Ordering::Less)
            .ok_or(EvaluationError::UnsupportedOrdering { actual: left_kind }),
    }
}

fn equal_values(left: &Resolved<'_>, right: &Resolved<'_>) -> bool {
    match (left, right) {
        (Resolved::Null, Resolved::Null) => true,
        (Resolved::Boolean(left), Resolved::Boolean(right)) => left == right,
        (Resolved::Number(left), Resolved::Number(right)) => {
            compare_numbers(left, right).is_some_and(|value| value == Ordering::Equal)
        }
        (Resolved::String(left), Resolved::String(right)) => left == right,
        _ => false,
    }
}

fn compare_numbers(left: &Number, right: &Number) -> Option<Ordering> {
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return Some(left.cmp(&right));
    }
    if let (Some(left), Some(right)) = (left.as_u64(), right.as_u64()) {
        return Some(left.cmp(&right));
    }
    let left = left.as_f64()?;
    let right = right.as_f64()?;
    Some(left.total_cmp(&right))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_and_evaluates_boolean_comparison_and_existence() {
        let expression = Expression::parse(
            r#"session.ready && retries < 3 && exists(tasks[0].name) && tasks[0].name == "build""#,
            ExpressionLimits::default(),
        )
        .expect("valid expression");
        let environment = json!({
            "session": {"ready": true},
            "retries": 2,
            "tasks": [{"name": "build"}],
        });
        assert!(expression.evaluate(&environment).expect("evaluation"));
    }

    #[test]
    fn boolean_operators_short_circuit_missing_paths() {
        let expression = Expression::parse("true || missing.value", ExpressionLimits::default())
            .expect("valid expression");
        assert!(expression.evaluate(&json!({})).expect("short circuit"));
    }

    #[test]
    fn existence_returns_false_for_missing_and_wrong_container() {
        let missing =
            Expression::parse("exists(items[2])", ExpressionLimits::default()).expect("parse");
        assert!(!missing.evaluate(&json!({"items": []})).expect("exists"));
        let wrong =
            Expression::parse("exists(items.name)", ExpressionLimits::default()).expect("parse");
        assert!(!wrong.evaluate(&json!({"items": []})).expect("exists"));
    }

    #[test]
    fn type_errors_are_explicit() {
        let mismatch = Expression::parse("value == 1", ExpressionLimits::default()).expect("parse");
        assert_eq!(
            mismatch.evaluate(&json!({"value": "1"})),
            Err(EvaluationError::TypeMismatch {
                left: "string",
                right: "number",
            })
        );
        let ordering =
            Expression::parse("value < true", ExpressionLimits::default()).expect("parse");
        assert_eq!(
            ordering.evaluate(&json!({"value": false})),
            Err(EvaluationError::UnsupportedOrdering { actual: "boolean" })
        );
    }

    #[test]
    fn malformed_input_has_stable_position() {
        assert_eq!(
            Expression::parse("true && )", ExpressionLimits::default()),
            Err(ParseError::UnexpectedToken {
                position: 8,
                expected: "an operand",
                found: "`)`".into(),
            })
        );
    }

    #[test]
    fn every_resource_bound_is_enforced() {
        let limits = ExpressionLimits {
            max_input_bytes: 4,
            ..ExpressionLimits::default()
        };
        assert!(matches!(
            Expression::parse("true ", limits),
            Err(ParseError::InputTooLong { .. })
        ));

        let limits = ExpressionLimits {
            max_string_bytes: 2,
            ..ExpressionLimits::default()
        };
        assert!(matches!(
            Expression::parse(r#""abc" == "x""#, limits),
            Err(ParseError::StringTooLong { .. })
        ));

        let limits = ExpressionLimits {
            max_nodes: 2,
            ..ExpressionLimits::default()
        };
        assert!(matches!(
            Expression::parse("true && false", limits),
            Err(ParseError::NodeLimitExceeded { .. })
        ));

        let limits = ExpressionLimits {
            max_depth: 2,
            ..ExpressionLimits::default()
        };
        assert!(matches!(
            Expression::parse("!!!true", limits),
            Err(ParseError::DepthExceeded { .. })
        ));

        let limits = ExpressionLimits {
            max_path_segments: 2,
            ..ExpressionLimits::default()
        };
        assert!(matches!(
            Expression::parse("a.b.c == 1", limits),
            Err(ParseError::PathSegmentsExceeded { .. })
        ));
    }

    #[test]
    fn ast_serialization_is_deterministic() {
        let expression =
            Expression::parse("state.count >= 2", ExpressionLimits::default()).expect("parse");
        assert_eq!(
            serde_json::to_string(&expression).expect("serialize"),
            r#"{"expression":"compare","value":{"left":{"operand":"path","value":{"segments":[{"segment":"key","value":"state"},{"segment":"key","value":"count"}]}},"operator":"greater_or_equal","right":{"operand":"literal","value":{"type":"number","value":2}}}}"#
        );
    }
}
