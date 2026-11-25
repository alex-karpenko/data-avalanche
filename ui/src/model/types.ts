// What the console reads from `/v1` and the live stream: the fields it uses, as the server sends
// them (the API's reference names the rest).

export type Actual = 'starting' | 'running' | 'paused' | 'stopped' | 'dumped' | 'retrying' | 'error';
export type Desired = 'running' | 'paused' | 'stopped';

/** Where a pipeline reads: its config's `source`. */
export interface SourceAt {
  type: 'postgres' | 'mysql';
  host: string;
  port: number;
  db: string;
}

/** Where a pipeline writes: its config's `sink`. */
export interface SinkAt {
  type: 'delta' | 'iceberg';
  bucket: string;
  prefix: string;
}

/** `GET /v1/pipelines`'s rows. */
export interface PipelineItem {
  id: string;
  name: string;
  desired_state: Desired;
  actual_state: Actual | null;
  holder: string | null;
  holder_stale: boolean;
  source: SourceAt;
  sink: SinkAt;
}

/** `GET /v1/cluster/status`. */
export interface ClusterStatus {
  node: string;
  members: { node: string; phase: string; suspect: boolean; config_generation: number | null }[];
  pipelines: {
    id: string;
    holder: string | null;
    holder_stale: boolean;
    owner_epoch: number;
    /** When the lease ends unless renewed (RFC 3339). */
    expires_at: string | null;
    /** The node the ring gives it to: a holder that isn't it holds it away from its owner. */
    ring_owner: string | null;
  }[];
}

/** `GET /v1/config/nodes`. */
export interface ConfigNodes {
  generation: number;
  nodes: {
    node: string;
    generation: number | null;
    file: string;
    /** `null`, with an `error`, for a node that doesn't answer or gossips no API address. */
    restart_pending: string[] | null;
    error?: string;
  }[];
}

export interface Halt {
  code: string;
  detail: string;
  since: string | null;
}

/** A table as `GET /v1/tables` lists it, and as a pipeline's `figures` show it. */
export interface TableItem {
  pipeline: string;
  table: string;
  state: string;
  halt: Halt | null;
  last_commit: string | null;
}

/** `GET /v1/tables`. */
export interface Tables {
  as_of: string;
  tables: TableItem[];
  unreachable: { pipeline: string; error?: string }[];
}

/** A snapshot job, as `GET /v1/snapshots` lists it. */
export interface Job {
  /** `<pipeline>~<uuid>`. */
  job_id: string;
  pipeline: string;
  status: 'queued' | 'running' | 'cancelling' | 'completed' | 'failed' | 'cancelled';
  rows_done: number;
  estimate: number | null;
  requested_at: string;
  finished_at: string | null;
  error: string | null;
}

/** PostgreSQL's source detail; MySQL's has no slot. */
export interface SourceDetail {
  engine: 'postgres' | 'mysql';
  slot?: { name: string; wal_held_bytes: number | null } | null;
  max_slot_wal_keep_size?: number | null;
}

/** A pipeline's live figures: `GET /v1/pipelines/{id}/live`, and the stream's `figures`. */
export interface Figures {
  id: string;
  node: string;
  owner_epoch: number;
  actual: Actual;
  as_of: string;
  error: string | null;
  /** `null` without a worker: the pipeline is stopped, or starting. */
  source: { detail: SourceDetail | null } | null;
  tables: Omit<TableItem, 'pipeline'>[];
  worker: { lag_seconds: number | null; degraded: string | null } | null;
}

/** An event, as `GET /v1/events` lists it and the stream's `event` sends it. */
export interface EventItem {
  key: string;
  at: string;
  kind: string;
  node: string;
  pipeline?: string;
  severity: 'info' | 'warning' | 'error';
  text: string;
  actor?: { name: string; role: string };
  note?: string;
  /** How an action ended: `accepted`, `unchanged`, `refused`, `denied`. */
  outcome?: string;
}

/** `GET /v1/history`'s series: points `[unix seconds, value]`, oldest first. */
export interface Series {
  name: string;
  pipeline?: string;
  points: [number, number][];
}
