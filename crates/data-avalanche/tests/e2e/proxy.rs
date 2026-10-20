//! A TCP proxy in front of a MySQL server (Q-M7b-3), for the faults no server can be asked for.
//! It passes the protocol's packets through untouched. On request it corrupts the next
//! compressed transaction a binlog dump sends (`BINLOG_PAYLOAD_INVALID`), or sends new
//! connections to another server and cuts the open ones (a failover behind one address,
//! `BINLOG_SOURCE_CHANGED`). Without TLS only (`ssl_mode: disable`): it reads the packets.

use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::AbortHandle;

/// `COM_BINLOG_DUMP`, `COM_BINLOG_DUMP_GTID`: the commands that start a dump.
const DUMP_COMMANDS: [u8; 2] = [0x12, 0x1e];
/// MySQL's `TRANSACTION_PAYLOAD_EVENT` (a transaction compressed by
/// `binlog_transaction_compression`).
const TRANSACTION_PAYLOAD: u8 = 40;

#[derive(Debug, Default)]
struct Faults {
    /// Corrupt the next payload a dump sends.
    corrupt: AtomicBool,
    /// A payload was corrupted.
    corrupted: AtomicBool,
}

/// The proxy, listening until dropped.
#[derive(Debug)]
pub struct Proxy {
    port: u16,
    upstream: Arc<AtomicU16>,
    faults: Arc<Faults>,
    /// The listener's task, then each connection's pumps.
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
}

/// One protocol packet: a 3-byte length, a sequence id, the payload.
async fn packet(r: &mut (impl AsyncRead + Unpin)) -> std::io::Result<Vec<u8>> {
    let mut head = [0u8; 4];
    r.read_exact(&mut head).await?;
    let len = u32::from_le_bytes([head[0], head[1], head[2], 0]) as usize;
    let mut p = head.to_vec();
    p.resize(4 + len, 0);
    r.read_exact(&mut p[4..]).await?;
    Ok(p)
}

/// Corrupts `p` when it is a dump's `TRANSACTION_PAYLOAD` event (an OK byte, then the event's
/// 19-byte header, type at offset 4): one byte of the compressed body flipped and the event's
/// CRC32 recomputed, so only the decompression can find it. It assumes one packet holds the
/// whole event (under 16 MiB).
fn corrupt(p: &mut [u8]) -> bool {
    if p.len() < 4 + 1 + 19 + 4 || p[4] != 0 || p[4 + 1 + 4] != TRANSACTION_PAYLOAD {
        return false;
    }
    let ev = &mut p[5..];
    let n = ev.len();
    ev[n - 4 - 2] ^= 0xff;
    let mut crc = flate2::Crc::new();
    crc.update(&ev[..n - 4]);
    ev[n - 4..].copy_from_slice(&crc.sum().to_le_bytes());
    true
}

impl Proxy {
    /// A proxy to the MySQL server on `upstream`.
    pub async fn start(upstream: u16) -> Self {
        let l = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a proxy port");
        let port = l.local_addr().expect("the proxy's address").port();
        let upstream = Arc::new(AtomicU16::new(upstream));
        let faults = Arc::new(Faults::default());
        let tasks = Arc::new(Mutex::new(vec![]));
        let accept = {
            let (upstream, faults, tasks) = (upstream.clone(), faults.clone(), tasks.clone());
            tokio::spawn(async move {
                while let Ok((client, _)) = l.accept().await {
                    let to = upstream.load(Ordering::SeqCst);
                    let Ok(server) = TcpStream::connect(("127.0.0.1", to)).await else {
                        continue;
                    };
                    let (mut cr, mut cw) = client.into_split();
                    let (mut sr, mut sw) = server.into_split();
                    let dump = Arc::new(AtomicBool::new(false));
                    let up = {
                        let dump = dump.clone();
                        tokio::spawn(async move {
                            while let Ok(p) = packet(&mut cr).await {
                                if p[3] == 0 && p.len() > 4 && DUMP_COMMANDS.contains(&p[4]) {
                                    dump.store(true, Ordering::SeqCst);
                                }
                                if sw.write_all(&p).await.is_err() {
                                    break;
                                }
                            }
                        })
                    };
                    let down = {
                        let faults = faults.clone();
                        tokio::spawn(async move {
                            while let Ok(mut p) = packet(&mut sr).await {
                                if dump.load(Ordering::SeqCst)
                                    && faults.corrupt.load(Ordering::SeqCst)
                                    && corrupt(&mut p)
                                {
                                    faults.corrupt.store(false, Ordering::SeqCst);
                                    faults.corrupted.store(true, Ordering::SeqCst);
                                }
                                if cw.write_all(&p).await.is_err() {
                                    break;
                                }
                            }
                        })
                    };
                    let mut t = tasks.lock().expect("no panic holds the lock");
                    t.push(up.abort_handle());
                    t.push(down.abort_handle());
                }
            })
        };
        tasks
            .lock()
            .expect("no panic holds the lock")
            .push(accept.abort_handle());
        Self {
            port,
            upstream,
            faults,
            tasks,
        }
    }

    /// The port a pipeline connects to.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Corrupts the next compressed transaction a dump sends, once.
    pub fn corrupt_next_payload(&self) {
        self.faults.corrupt.store(true, Ordering::SeqCst);
    }

    /// Whether a payload was corrupted.
    pub fn corrupted(&self) -> bool {
        self.faults.corrupted.load(Ordering::SeqCst)
    }

    /// Sends new connections to the server on `port` and cuts the open ones, as a failover
    /// behind one address does.
    pub fn switch(&self, port: u16) {
        self.upstream.store(port, Ordering::SeqCst);
        let mut tasks = self.tasks.lock().expect("no panic holds the lock");
        // The listener's task stays; every connection's pumps go.
        for t in tasks.drain(1..) {
            t.abort();
        }
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        for t in self
            .tasks
            .lock()
            .expect("no panic holds the lock")
            .drain(..)
        {
            t.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A dump's event packet: header, OK byte, a 19-byte event header and `body`, then a CRC.
    fn event(kind: u8, body: &[u8]) -> Vec<u8> {
        let mut ev = vec![0u8; 19];
        ev[4] = kind;
        ev.extend_from_slice(body);
        let mut crc = flate2::Crc::new();
        crc.update(&ev);
        ev.extend_from_slice(&crc.sum().to_le_bytes());
        let len = (ev.len() + 1) as u32;
        let mut p = len.to_le_bytes()[..3].to_vec();
        p.push(1);
        p.push(0);
        p.extend(ev);
        p
    }

    #[test]
    fn only_a_transaction_payload_is_corrupted_and_its_checksum_still_holds() {
        let mut p = event(TRANSACTION_PAYLOAD, b"compressed body");
        let before = p.clone();
        assert!(corrupt(&mut p));
        assert_ne!(p, before);
        let ev = &p[5..];
        let n = ev.len();
        let mut crc = flate2::Crc::new();
        crc.update(&ev[..n - 4]);
        assert_eq!(crc.sum().to_le_bytes(), ev[n - 4..], "the CRC matches");
        let mut rows = event(30, b"rows");
        assert!(!corrupt(&mut rows), "a WRITE_ROWS event passes");
    }
}
