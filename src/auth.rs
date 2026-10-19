//! Bearer-token authorization for the HTTP API.
//!
//! The token file has one `<role> <token>` pair per line, where role is
//! `read` or `write` (write implies read). The token may be given in plain
//! text or as `blake3:<hex>` of the token, so the file needn't hold secrets.
//! Blank lines and `#` comments are ignored.
//!
//! ```text
//! read  blake3:6a09e667f3bcc908b2fb1366ea957d3e3adec17512775099da2f590b066f6f2e
//! write s3cret-deploy-token
//! ```

use anyhow::{Context, bail};
use std::collections::HashMap;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Access {
    Read,
    Write,
}

#[derive(Debug, Default)]
pub struct Tokens {
    // Keyed by hash, so lookups never compare secrets directly.
    by_hash: HashMap<[u8; 32], Access>,
}

fn parse_hex(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

impl Tokens {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("in {}", path.display()))
    }

    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let mut by_hash = HashMap::new();
        for (n, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((role, token)) = line.split_once(char::is_whitespace) else {
                bail!("line {}: expected `<read|write> <token>`", n + 1);
            };
            let access = match role {
                "read" => Access::Read,
                "write" => Access::Write,
                other => bail!("line {}: unknown role {other:?}", n + 1),
            };
            let token = token.trim();
            let hash = match token.strip_prefix("blake3:") {
                Some(hex) => {
                    parse_hex(hex).with_context(|| format!("line {}: bad blake3 hash", n + 1))?
                }
                None => *blake3::hash(token.as_bytes()).as_bytes(),
            };
            by_hash.insert(hash, access);
        }
        if by_hash.is_empty() {
            bail!("no tokens defined");
        }
        Ok(Self { by_hash })
    }

    pub fn check(&self, token: &str) -> Option<Access> {
        self.by_hash
            .get(blake3::hash(token.as_bytes()).as_bytes())
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_and_hashed_tokens() {
        let hashed = blake3::hash(b"reader").to_hex();
        let t = Tokens::parse(&format!(
            "# comment\nread blake3:{hashed}\n\nwrite  deployer\n"
        ))
        .unwrap();
        assert_eq!(t.check("reader"), Some(Access::Read));
        assert_eq!(t.check("deployer"), Some(Access::Write));
        assert_eq!(t.check("nope"), None);
        assert!(Access::Write > Access::Read);
        assert!(Tokens::parse("admin x").is_err());
        assert!(Tokens::parse("read blake3:zz").is_err());
        assert!(Tokens::parse("").is_err());
    }
}
