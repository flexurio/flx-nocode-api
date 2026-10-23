//! Redis Streams work queue shared by the write queue and the email queue.
//!
//! Replaces the hand-rolled `LPUSH` / `BLMOVE` / per-worker processing lists:
//!
//! * `XADD` publishes; `XREADGROUP … >` delivers each entry to exactly one
//!   consumer of the group; `XACK` acknowledges after the work is durable.
//! * Entries a crashed consumer never acknowledged stay in the group's Pending
//!   Entries List and are re-delivered to a live consumer by `XAUTOCLAIM`
//!   (at-least-once, no custom recovery code, safe with many replicas).
//! * Lag is observable: `XPENDING` / `XLEN` feed the `flx_queue_*` gauges.
//!
//! Only raw `redis::cmd` is used, so the module does not depend on the typed
//! stream API of a particular `redis` crate version.

use anyhow::{anyhow, Result};
use redis::aio::ConnectionLike;
use redis::Value;

/// Field name under which the JSON payload is stored in every stream entry.
const FIELD: &str = "payload";

#[derive(Debug, Clone)]
pub struct StreamQueue {
    /// Stream key, e.g. `flx:wq:stream`.
    pub key: String,
    /// Consumer group, e.g. `flx-writers`.
    pub group: String,
    /// Dead-letter stream key.
    pub dlq_key: String,
}

/// One delivered entry: the stream id (needed for `XACK`) and the payload.
#[derive(Debug, Clone)]
pub struct Entry {
    pub id: String,
    pub payload: String,
}

fn to_string(v: &Value) -> Option<String> {
    match v {
        Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        Value::SimpleString(s) => Some(s.clone()),
        Value::VerbatimString { text, .. } => Some(text.clone()),
        Value::Int(i) => Some(i.to_string()),
        _ => None,
    }
}

/// Parse `[[id, [field, value, ...]], ...]` into entries.
fn parse_entries(list: &Value) -> Vec<Entry> {
    let mut out = Vec::new();
    let Value::Array(items) = list else { return out };
    for item in items {
        let Value::Array(parts) = item else { continue };
        if parts.len() < 2 {
            continue;
        }
        let Some(id) = to_string(&parts[0]) else { continue };
        let mut payload = None;
        match &parts[1] {
            Value::Array(fv) => {
                let mut i = 0;
                while i + 1 < fv.len() {
                    if to_string(&fv[i]).as_deref() == Some(FIELD) {
                        payload = to_string(&fv[i + 1]);
                        break;
                    }
                    i += 2;
                }
            }
            Value::Map(pairs) => {
                for (k, v) in pairs {
                    if to_string(k).as_deref() == Some(FIELD) {
                        payload = to_string(v);
                        break;
                    }
                }
            }
            _ => {}
        }
        if let Some(payload) = payload {
            out.push(Entry { id, payload });
        }
    }
    out
}

impl StreamQueue {
    pub fn new(key: impl Into<String>, group: impl Into<String>, dlq_key: impl Into<String>) -> Self {
        Self { key: key.into(), group: group.into(), dlq_key: dlq_key.into() }
    }

    /// `XGROUP CREATE key group $ MKSTREAM`; an existing group is not an error.
    pub async fn ensure_group(&self, conn: &mut (impl ConnectionLike + Send)) -> Result<()> {
        let res: redis::RedisResult<Value> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.key)
            .arg(&self.group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(conn)
            .await;
        match res {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()),
            Err(e) => Err(anyhow!("XGROUP CREATE {} failed: {}", self.key, e)),
        }
    }

    /// Number of entries in the stream (`XLEN`), used for backpressure.
    pub async fn len(&self, conn: &mut (impl ConnectionLike + Send)) -> Result<i64> {
        let n: i64 = redis::cmd("XLEN").arg(&self.key).query_async(conn).await?;
        Ok(n)
    }

    /// `XADD key * payload <json>`; returns the new entry id.
    pub async fn publish(&self, conn: &mut (impl ConnectionLike + Send), payload: &str) -> Result<String> {
        let id: String = redis::cmd("XADD")
            .arg(&self.key)
            .arg("*")
            .arg(FIELD)
            .arg(payload)
            .query_async(conn)
            .await
            .map_err(|e| anyhow!("XADD {} failed: {}", self.key, e))?;
        Ok(id)
    }

    /// Deliver up to `count` new entries to `consumer`, blocking up to `block_ms`.
    pub async fn read_batch(
        &self,
        conn: &mut (impl ConnectionLike + Send),
        consumer: &str,
        count: usize,
        block_ms: u64,
    ) -> Result<Vec<Entry>> {
        let v: Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.group)
            .arg(consumer)
            .arg("COUNT")
            .arg(count)
            .arg("BLOCK")
            .arg(block_ms)
            .arg("STREAMS")
            .arg(&self.key)
            .arg(">")
            .query_async(conn)
            .await?;
        // Reply: Nil on timeout, else [[key, [entries...]]] (or a map in RESP3).
        let mut out = Vec::new();
        match &v {
            Value::Nil => {}
            Value::Array(streams) => {
                for s in streams {
                    if let Value::Array(kv) = s
                        && kv.len() >= 2
                    {
                        out.extend(parse_entries(&kv[1]));
                    }
                }
            }
            Value::Map(pairs) => {
                for (_k, entries) in pairs {
                    out.extend(parse_entries(entries));
                }
            }
            _ => {}
        }
        Ok(out)
    }

    /// Claim entries another consumer left pending for more than `min_idle_ms`
    /// (crashed or hung worker) and deliver them to `consumer`.
    pub async fn claim_stale(
        &self,
        conn: &mut (impl ConnectionLike + Send),
        consumer: &str,
        min_idle_ms: u64,
        count: usize,
    ) -> Result<Vec<Entry>> {
        let v: Value = redis::cmd("XAUTOCLAIM")
            .arg(&self.key)
            .arg(&self.group)
            .arg(consumer)
            .arg(min_idle_ms)
            .arg("0-0")
            .arg("COUNT")
            .arg(count)
            .query_async(conn)
            .await?;
        // Reply: [next-start-id, [entries...], [deleted-ids...]] (Redis >= 7).
        if let Value::Array(parts) = &v
            && parts.len() >= 2
        {
            return Ok(parse_entries(&parts[1]));
        }
        Ok(Vec::new())
    }

    pub async fn ack(&self, conn: &mut (impl ConnectionLike + Send), id: &str) -> Result<()> {
        let _: i64 = redis::cmd("XACK").arg(&self.key).arg(&self.group).arg(id).query_async(conn).await?;
        Ok(())
    }

    /// Append a failure record to the dead-letter stream.
    pub async fn dead_letter(&self, conn: &mut (impl ConnectionLike + Send), record: &str) -> Result<i64> {
        let _: String = redis::cmd("XADD").arg(&self.dlq_key).arg("*").arg(FIELD).arg(record).query_async(conn).await?;
        let n: i64 = redis::cmd("XLEN").arg(&self.dlq_key).query_async(conn).await?;
        Ok(n)
    }

    /// Pending (delivered, not yet acknowledged) entries of the group.
    pub async fn pending(&self, conn: &mut (impl ConnectionLike + Send)) -> Result<i64> {
        let v: Value = redis::cmd("XPENDING").arg(&self.key).arg(&self.group).query_async(conn).await?;
        if let Value::Array(parts) = &v
            && let Some(Value::Int(n)) = parts.first()
        {
            return Ok(*n);
        }
        Ok(0)
    }

    /// One-off migration: move every job still sitting in the legacy Redis
    /// lists (`list_key` and the per-worker processing lists matching
    /// `processing_pattern`) into the stream so nothing is lost on upgrade.
    pub async fn migrate_legacy_lists(
        &self,
        conn: &mut (impl ConnectionLike + Send),
        list_key: &str,
        processing_pattern: Option<&str>,
    ) -> Result<usize> {
        let mut moved = 0usize;
        let mut sources: Vec<String> = vec![list_key.to_string()];
        if let Some(pattern) = processing_pattern {
            let mut cursor: u64 = 0;
            loop {
                let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(pattern)
                    .arg("COUNT")
                    .arg(100)
                    .query_async(&mut *conn)
                    .await?;
                sources.extend(keys);
                cursor = next;
                if cursor == 0 {
                    break;
                }
            }
        }
        for src in sources {
            loop {
                let item: Option<String> = redis::cmd("RPOP").arg(&src).query_async(&mut *conn).await?;
                let Some(payload) = item else { break };
                self.publish(conn, &payload).await?;
                moved += 1;
            }
        }
        Ok(moved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> Value {
        Value::BulkString(s.as_bytes().to_vec())
    }

    #[test]
    fn parses_resp2_entries() {
        let list = Value::Array(vec![
            Value::Array(vec![bs("1-0"), Value::Array(vec![bs("payload"), bs("{\"a\":1}")])]),
            Value::Array(vec![bs("2-0"), Value::Array(vec![bs("other"), bs("x"), bs("payload"), bs("{\"b\":2}")])]),
            Value::Array(vec![bs("3-0"), Value::Array(vec![bs("nopayload"), bs("x")])]),
        ]);
        let e = parse_entries(&list);
        assert_eq!(e.len(), 2);
        assert_eq!(e[0].id, "1-0");
        assert_eq!(e[1].payload, "{\"b\":2}");
    }

    #[test]
    fn parses_resp3_map_entries() {
        let list = Value::Array(vec![Value::Array(vec![bs("9-1"), Value::Map(vec![(bs("payload"), bs("p"))])])]);
        let e = parse_entries(&list);
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].payload, "p");
    }
}
