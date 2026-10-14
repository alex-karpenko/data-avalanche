//! Each pipeline's source and sink, built for every worker run.

use std::sync::Arc;

use data_avalanche_cluster::{Parts, WorkerFactory};
use data_avalanche_config::{Globals, Pipeline, SinkType, TimestampNtzAs};
use data_avalanche_pipeline::{Hooks, NoHooks};
use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig, NtzAs, TableDefaults};
use data_avalanche_source_pg::PgConnector;

use crate::startup::backend;

/// The Delta sink of `p` (`sink.delta`, config §9.4.3): tables at `<bucket>/<prefix>/…`.
///
/// # Errors
/// The pipeline's sink isn't Delta.
pub fn delta_config(globals: &Globals, p: &Pipeline) -> Result<DeltaSinkConfig, String> {
    let Some(d) = &p.sink.delta else {
        return Err(format!("pipeline {}: the sink isn't Delta", p.name));
    };
    Ok(DeltaSinkConfig {
        pipeline_id: p.id.clone(),
        location: DeltaLocation::Bucket {
            backend: backend(globals),
            bucket: p.sink.bucket.clone(),
            prefix: p.sink.prefix.clone(),
            options: vec![],
        },
        table: TableDefaults {
            checkpoint_interval: d.checkpoint_interval,
            log_retention: d.log_retention,
            timestamp_ntz_as: match d.timestamp_ntz_as {
                TimestampNtzAs::TimestampNtz => NtzAs::TimestampNtz,
                TimestampNtzAs::Timestamp => NtzAs::Timestamp,
                TimestampNtzAs::String => NtzAs::String,
            },
            table_properties: d.table_properties.clone(),
        },
        merge_memory_limit: d
            .merge_memory_limit
            .map(|b| usize::try_from(b).unwrap_or(usize::MAX)),
    })
}

/// Builds the PostgreSQL source and the Delta sink.
#[derive(Debug)]
pub struct Factory {
    globals: Globals,
    hooks: Arc<dyn Hooks>,
}

impl Factory {
    /// # Errors
    /// With the `e2e-hooks` feature: a `DATA_AVALANCHE_E2E_CRASH` that names no crash point.
    pub fn new(globals: &Globals) -> Result<Self, String> {
        Ok(Self {
            globals: globals.clone(),
            hooks: hooks()?,
        })
    }
}

#[cfg(feature = "e2e-hooks")]
fn hooks() -> Result<Arc<dyn Hooks>, String> {
    match std::env::var(crate::crash::CRASH_VAR) {
        Ok(v) if !v.is_empty() => Ok(Arc::new(crate::crash::CrashAt::parse(&v)?)),
        _ => Ok(Arc::new(NoHooks)),
    }
}

#[cfg(not(feature = "e2e-hooks"))]
#[allow(clippy::unnecessary_wraps)]
fn hooks() -> Result<Arc<dyn Hooks>, String> {
    Ok(Arc::new(NoHooks))
}

impl WorkerFactory for Factory {
    fn build(&self, p: &Pipeline) -> Result<Parts, String> {
        if p.sink.kind == SinkType::Iceberg {
            return Err("the Iceberg sink ships in v2 (D-V1.4); other pipelines run".into());
        }
        let source = PgConnector::new(p).map_err(|e| e.to_string())?;
        let sink = DeltaSink::new(delta_config(&self.globals, p)?);
        Ok(Parts {
            source: Arc::new(source),
            sink: Arc::new(sink),
            hooks: Arc::clone(&self.hooks),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use data_avalanche_storage::Backend;

    use super::*;
    use crate::run::tests::config;

    #[test]
    fn the_delta_sink_takes_the_pipelines_sink_settings() {
        let c = config();
        let p = &c.pipelines["orders"];
        let d = delta_config(&c.globals, p).unwrap();
        assert_eq!(d.pipeline_id, "orders");
        assert_eq!(
            d.location,
            DeltaLocation::Bucket {
                backend: Backend::S3,
                bucket: "lake".into(),
                prefix: "shop".into(),
                options: vec![],
            }
        );
        assert_eq!(d.table.checkpoint_interval, 20);
        assert_eq!(d.table.log_retention, Duration::from_secs(30 * 24 * 3600));
        assert_eq!(d.table.timestamp_ntz_as, NtzAs::String);
        assert_eq!(d.merge_memory_limit, Some(64 << 20));
    }

    #[test]
    fn an_iceberg_pipeline_gets_no_parts() {
        let c = config();
        let mut p = c.pipelines["orders"].clone();
        p.sink.kind = SinkType::Iceberg;
        let e = Factory::new(&c.globals).unwrap().build(&p).unwrap_err();
        assert!(e.contains("v2"), "{e}");
    }
}
