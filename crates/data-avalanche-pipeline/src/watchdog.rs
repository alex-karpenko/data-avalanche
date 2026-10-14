//! The stream watchdog (DA-SRC-16): a stream that stays silent while the source stops answering
//! ends the session, and the worker reconnects. Silence alone proves nothing (an idle server with
//! `wal_sender_timeout = 0` sends nothing), so the heartbeat's read of the source's log decides: it
//! failed (a frozen server times it out), or it found no stream on the slot (the server dropped the
//! connection without the client hearing of it).

/// Heartbeats the stream may stay silent while the heartbeat's reads go unanswered.
pub(crate) const SILENT_HEARTBEATS: u32 = 3;

#[derive(Debug, Default)]
pub(crate) struct Watchdog {
    /// A stream item since the last heartbeat.
    heard: bool,
    /// Consecutive heartbeats without a stream item while the session read the stream.
    silent: u32,
    /// Consecutive heartbeat reads that failed or found no stream, and the last one's finding.
    unanswered: u32,
    why: String,
}

impl Watchdog {
    /// A stream item arrived.
    pub(crate) fn heard(&mut self) {
        self.heard = true;
        self.silent = 0;
    }

    /// A heartbeat read completed: `failure` is what was wrong with it, `None` when the source
    /// answered and a stream reads the slot. A read still out counts neither way: it may wait
    /// behind a chunk read on a shared connection, which `timeouts.command` bounds.
    pub(crate) fn answered(&mut self, failure: Option<String>) {
        match failure {
            None => self.unanswered = 0,
            Some(why) => {
                self.unanswered += 1;
                self.why = why;
            }
        }
    }

    /// A heartbeat. `reading`: the session reads the stream (not paused, no flush waiting for
    /// room), so a silent stream is the source's doing. `Some(reason)` when the session must end.
    pub(crate) fn beat(&mut self, reading: bool) -> Option<String> {
        if reading && !self.heard {
            self.silent += 1;
        } else {
            self.silent = 0;
        }
        self.heard = false;
        (self.silent >= SILENT_HEARTBEATS && self.unanswered >= SILENT_HEARTBEATS).then(|| {
            format!(
                "the source isn't answering: no stream message for {} heartbeats, and the last {} reads of its log failed ({})",
                self.silent, self.unanswered, self.why
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failed(w: &mut Watchdog) {
        w.answered(Some("timed out".into()));
    }

    #[test]
    fn a_silent_stream_while_the_reads_fail_ends_the_session() {
        let mut w = Watchdog::default();
        for _ in 0..SILENT_HEARTBEATS - 1 {
            failed(&mut w);
            assert_eq!(w.beat(true), None);
        }
        failed(&mut w);
        let why = w.beat(true).unwrap();
        assert!(why.contains("no stream message for 3 heartbeats"), "{why}");
        assert!(why.contains("timed out"), "{why}");
    }

    #[test]
    fn silence_alone_is_an_idle_source() {
        let mut w = Watchdog::default();
        for _ in 0..10 {
            w.answered(None);
            assert_eq!(w.beat(true), None);
        }
    }

    #[test]
    fn failing_reads_beside_a_live_stream_are_the_catalog_s_trouble() {
        let mut w = Watchdog::default();
        for _ in 0..10 {
            failed(&mut w);
            w.heard();
            assert_eq!(w.beat(true), None);
        }
    }

    #[test]
    fn a_session_that_doesn_t_read_isn_t_silent() {
        let mut w = Watchdog::default();
        for _ in 0..10 {
            failed(&mut w);
            assert_eq!(w.beat(false), None, "paused, or waiting for room");
        }
        failed(&mut w);
        assert_eq!(w.beat(true), None, "the count starts again");
    }

    #[test]
    fn an_answer_or_a_message_starts_the_count_again() {
        let mut w = Watchdog::default();
        for _ in 0..SILENT_HEARTBEATS - 1 {
            failed(&mut w);
            w.beat(true);
        }
        w.answered(None);
        failed(&mut w);
        assert_eq!(w.beat(true), None, "one unanswered read since the answer");
        w.heard();
        failed(&mut w);
        failed(&mut w);
        assert_eq!(w.beat(true), None, "the message reset the silence");
    }
}
