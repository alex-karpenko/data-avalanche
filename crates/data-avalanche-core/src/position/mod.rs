//! Source positions per engine dialect (`Delivery_Guarantee_and_Position_Guard.md` §2).

pub mod lsn;

pub use lsn::{Lsn, LsnParseError};

pub mod gtid;

pub use gtid::{GtidParseError, GtidSet, GtidSource};
