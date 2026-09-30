//! Minimal pgoutput (protocol v1) decoder — the part `pgwire-replication` leaves to the caller.
//! Probe-grade: decodes every message the CDC design consumes, text-format tuples only.
//! Reference: PostgreSQL docs, "Logical Replication Message Formats".

#![allow(dead_code)]

use anyhow::{bail, ensure, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Null,
    /// Unchanged TOASTed value — not sent; the consumer must keep / re-query it.
    UnchangedToast,
    Text(String),
    Binary(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Column {
    pub key: bool,
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
}

#[derive(Debug, Clone)]
pub enum Msg {
    Relation { oid: u32, namespace: String, name: String, replica_identity: char, columns: Vec<Column> },
    Type { oid: u32, namespace: String, name: String },
    Origin { commit_lsn: u64, name: String },
    Insert { rel: u32, new: Vec<Datum> },
    /// `old_kind`: `Some('K')` key-only old tuple, `Some('O')` full old tuple, `None` none sent.
    Update { rel: u32, old_kind: Option<char>, old: Option<Vec<Datum>>, new: Vec<Datum> },
    Delete { rel: u32, old_kind: char, old: Vec<Datum> },
    Truncate { options: u8, rels: Vec<u32> },
    /// Begin / Commit / Message are surfaced by pgwire-replication itself.
    Other(char),
}

struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Cur<'a> {
    fn u8(&mut self) -> Result<u8> {
        ensure!(self.p < self.b.len(), "eof");
        self.p += 1;
        Ok(self.b[self.p - 1])
    }
    fn n<const N: usize>(&mut self) -> Result<[u8; N]> {
        ensure!(self.p + N <= self.b.len(), "eof");
        let mut a = [0u8; N];
        a.copy_from_slice(&self.b[self.p..self.p + N]);
        self.p += N;
        Ok(a)
    }
    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.n()?))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.n()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.n()?))
    }
    fn cstr(&mut self) -> Result<String> {
        let end = self.b[self.p..].iter().position(|&c| c == 0).ok_or_else(|| anyhow::anyhow!("no NUL"))?;
        let s = String::from_utf8(self.b[self.p..self.p + end].to_vec())?;
        self.p += end + 1;
        Ok(s)
    }
    fn bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        ensure!(self.p + len <= self.b.len(), "eof");
        self.p += len;
        Ok(self.b[self.p - len..self.p].to_vec())
    }
    fn tuple(&mut self) -> Result<Vec<Datum>> {
        let n = self.i16()?;
        let mut out = Vec::with_capacity(n as usize);
        for _ in 0..n {
            out.push(match self.u8()? {
                b'n' => Datum::Null,
                b'u' => Datum::UnchangedToast,
                b't' => {
                    let len = self.i32()? as usize;
                    Datum::Text(String::from_utf8(self.bytes(len)?)?)
                }
                b'b' => {
                    let len = self.i32()? as usize;
                    Datum::Binary(self.bytes(len)?)
                }
                k => bail!("unknown tuple kind {:?}", k as char),
            });
        }
        Ok(out)
    }
}

pub fn decode(data: &[u8]) -> Result<Msg> {
    let mut c = Cur { b: data, p: 0 };
    let tag = c.u8()? as char;
    Ok(match tag {
        'R' => {
            let oid = c.i32()? as u32;
            let namespace = c.cstr()?;
            let name = c.cstr()?;
            let replica_identity = c.u8()? as char;
            let n = c.i16()?;
            let mut columns = vec![];
            for _ in 0..n {
                let flags = c.u8()?;
                columns.push(Column { key: flags & 1 == 1, name: c.cstr()?, type_oid: c.i32()? as u32, typmod: c.i32()? });
            }
            Msg::Relation { oid, namespace, name, replica_identity, columns }
        }
        'Y' => Msg::Type { oid: c.i32()? as u32, namespace: c.cstr()?, name: c.cstr()? },
        'O' => Msg::Origin { commit_lsn: c.u64()?, name: c.cstr()? },
        'I' => {
            let rel = c.i32()? as u32;
            ensure!(c.u8()? == b'N', "Insert without N");
            Msg::Insert { rel, new: c.tuple()? }
        }
        'U' => {
            let rel = c.i32()? as u32;
            let mut k = c.u8()?;
            let (old_kind, old) = if k == b'K' || k == b'O' {
                let t = c.tuple()?;
                let kind = k as char;
                k = c.u8()?;
                (Some(kind), Some(t))
            } else {
                (None, None)
            };
            ensure!(k == b'N', "Update without N");
            Msg::Update { rel, old_kind, old, new: c.tuple()? }
        }
        'D' => {
            let rel = c.i32()? as u32;
            let kind = c.u8()? as char;
            Msg::Delete { rel, old_kind: kind, old: c.tuple()? }
        }
        'T' => {
            let n = c.i32()?;
            let options = c.u8()?;
            let mut rels = vec![];
            for _ in 0..n {
                rels.push(c.i32()? as u32);
            }
            Msg::Truncate { options, rels }
        }
        other => Msg::Other(other),
    })
}
