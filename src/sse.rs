//! Bounded incremental SSE parsing used to *validate and observe* the
//! passthrough stream. Raw upstream bytes are forwarded verbatim; this parser
//! never rewrites them.

const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
}

#[derive(Default)]
pub struct SseDecoder {
    buffer: Vec<u8>,
    event: Option<String>,
    data_lines: Vec<String>,
    data_bytes: usize,
    overflow: bool,
}

impl SseDecoder {
    /// Feed one network chunk; returns the events completed by this chunk.
    /// `Err(())` means the size limit was exceeded and the decoder was reset.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, ()> {
        if self.overflow || self.buffer.len().saturating_add(bytes.len()) > MAX_EVENT_BYTES {
            self.overflow = true;
            return Err(());
        }
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        while let Some(position) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let mut line: Vec<u8> = self.buffer.drain(..=position).collect();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            self.process_line(&String::from_utf8_lossy(&line), &mut events);
            if self.data_bytes > MAX_EVENT_BYTES {
                self.overflow = true;
                return Err(());
            }
        }
        Ok(events)
    }

    /// Flush a stream that ended without a final newline.
    pub fn finish(&mut self) -> Result<Vec<SseEvent>, ()> {
        if self.overflow {
            return Err(());
        }
        let mut events = Vec::new();
        if !self.buffer.is_empty() {
            let line = String::from_utf8_lossy(&std::mem::take(&mut self.buffer)).into_owned();
            self.process_line(&line, &mut events);
        }
        self.dispatch(&mut events);
        Ok(events)
    }

    fn process_line(&mut self, line: &str, events: &mut Vec<SseEvent>) {
        if line.is_empty() {
            self.dispatch(events);
        } else if line.starts_with(':') {
            // Comment / keepalive frame.
        } else if let Some(value) = line.strip_prefix("data:") {
            let value = value.strip_prefix(' ').unwrap_or(value);
            self.data_bytes = self.data_bytes.saturating_add(value.len());
            self.data_lines.push(value.to_owned());
        } else if let Some(value) = line.strip_prefix("event:") {
            self.event = Some(value.strip_prefix(' ').unwrap_or(value).to_owned());
        }
        // Unknown SSE fields (`id:`, `retry:`) are legal and ignored.
    }

    fn dispatch(&mut self, events: &mut Vec<SseEvent>) {
        if self.data_lines.is_empty() {
            self.event = None;
            return;
        }
        events.push(SseEvent {
            event: self.event.take(),
            data: self.data_lines.join("\n"),
        });
        self.data_lines.clear();
        self.data_bytes = 0;
    }
}

/// Extract protocol facts from one Anthropic SSE event's data payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnthropicEventKind {
    MessageStart,
    ContentBlockStart { block_type: &'static str },
    ContentBlockStop,
    MessageDelta,
    MessageStop,
    Ping,
    Error,
    Other,
}

pub fn classify_event(event: &SseEvent) -> AnthropicEventKind {
    let value = match serde_json::from_str::<serde_json::Value>(&event.data) {
        Ok(value) => value,
        Err(_) => return AnthropicEventKind::Other,
    };
    match value.get("type").and_then(|kind| kind.as_str()) {
        Some("message_start") => AnthropicEventKind::MessageStart,
        Some("content_block_start") => {
            let block_type = value
                .get("content_block")
                .and_then(|block| block.get("type"))
                .and_then(|kind| kind.as_str())
                .unwrap_or("other");
            let block_type = match block_type {
                "text" => "text",
                "thinking" => "thinking",
                "tool_use" => "tool_use",
                _ => "other",
            };
            AnthropicEventKind::ContentBlockStart { block_type }
        }
        Some("content_block_stop") => AnthropicEventKind::ContentBlockStop,
        Some("message_delta") => AnthropicEventKind::MessageDelta,
        Some("message_stop") => AnthropicEventKind::MessageStop,
        Some("ping") => AnthropicEventKind::Ping,
        Some("error") => AnthropicEventKind::Error,
        _ => AnthropicEventKind::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn handles_arbitrary_splits_crlf_comments_and_multiline_data() {
        let mut decoder = SseDecoder::default();
        assert!(
            decoder
                .push(b": keepalive\r\nevent: x\r\ndata: {\"a\":")
                .unwrap()
                .is_empty()
        );
        let events = decoder.push(b"1}\r\ndata: tail\r\n\r\n").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.as_deref(), Some("x"));
        assert_eq!(events[0].data, "{\"a\":1}\ntail");
    }

    #[test]
    fn one_event_split_over_many_chunks_is_assembled() {
        let mut decoder = SseDecoder::default();
        let mut events = Vec::new();
        for fragment in ["data: {\"type".as_bytes(), b"\":\"message_stop\"}", b"\n\n"] {
            events.extend(decoder.push(fragment).unwrap());
        }
        events.extend(decoder.finish().unwrap());
        assert_eq!(events.len(), 1);
        assert_eq!(classify_event(&events[0]), AnthropicEventKind::MessageStop);
    }

    #[test]
    fn multiple_events_in_one_chunk_are_delivered_together() {
        let mut decoder = SseDecoder::default();
        let events = decoder
            .push(
                b"event: ping\ndata: {\"type\":\"ping\"}\n\ndata: {\"type\":\"message_stop\"}\n\n",
            )
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(classify_event(&events[0]), AnthropicEventKind::Ping);
        assert_eq!(classify_event(&events[1]), AnthropicEventKind::MessageStop);
    }

    #[test]
    fn oversized_events_are_rejected_without_panicking() {
        let mut decoder = SseDecoder::default();
        let big = vec![b'x'; MAX_EVENT_BYTES + 1];
        assert!(decoder.push(&big).is_err());
        // A rejected decoder stays rejected.
        assert!(decoder.push(b"data: 1\n\n").is_err());
    }

    #[test]
    fn abrupt_eof_flushes_pending_line() {
        let mut decoder = SseDecoder::default();
        assert!(
            decoder
                .push(b"data: {\"type\":\"message_stop\"}")
                .unwrap()
                .is_empty()
        );
        let events = decoder.finish().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(classify_event(&events[0]), AnthropicEventKind::MessageStop);
    }

    proptest! {
        /// Chunk-boundary placement must never change the parsed event stream.
        #[test]
        fn chunking_does_not_change_events(
            prefix in proptest::string::string_regex("[a-z ]{0,20}").unwrap(),
            cuts in proptest::collection::vec(1usize..40, 1..12),
        ) {
            let payload = format!("event: ping\ndata: {{\"type\":\"ping\",\"note\":\"{prefix}\"}}\n\n");
            let bytes = payload.as_bytes();
            let mut decoder = SseDecoder::default();
            let mut collected = Vec::new();
            let mut offset = 0usize;
            for cut in cuts {
                let end = (offset + cut).min(bytes.len());
                collected.extend(decoder.push(&bytes[offset..end]).unwrap_or_default());
                offset = end;
                if offset == bytes.len() { break; }
            }
            if offset < bytes.len() {
                collected.extend(decoder.push(&bytes[offset..]).unwrap_or_default());
            }
            collected.extend(decoder.finish().unwrap_or_default());
            assert_eq!(collected.len(), 1);
            assert!(collected[0].data.contains(&format!("\"note\":\"{prefix}\"")));
        }
    }
}
