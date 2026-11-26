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

/** A member as `GET /v1/cluster/status` lists it. */
export interface Member {
  node: string;
  /** Its gossip address. */
  addr?: string;
  phase: string;
  suspect: boolean;
  config_generation: number | null;
  build?: string;
  api_url?: string | null;
}

/** The cluster's settings a console states (`GET /v1/cluster/status`'s `settings`). */
export interface ClusterSettings {
  tokens: number;
  lock_backend: string;
  lease_seconds: number;
  rebalance_stabilization_seconds: number;
  max_concurrent_handoffs: number;
  drain_timeout_seconds: number;
  history_retention_seconds: number;
  events_retention_seconds: number;
}

/** `GET /v1/cluster/status`. */
export interface ClusterStatus {
  node: string;
  members: Member[];
  settings?: ClusterSettings;
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
  /** `streaming`, `halted`, `snapshot` or `copied`. */
  state: string;
  halt: Halt | null;
  snapshot: { rows_done: number; waiting: boolean } | null;
  /** The stream's changes committed in the last minute. */
  changes_per_minute: number;
  last_commit: string | null;
  /** The Delta table's version after the last commit. */
  version: number | null;
  /** The last MERGE's files rewritten, of the table's `files`. */
  files_rewritten: number | null;
  files: number | null;
  /** The sink's columns the source no longer has, kept and written NULL. */
  soft_dropped: number;
  /** What lake readers see: the source time of the newest change committed. */
  fresh_as_of: string | null;
  stale: boolean;
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
  /** Seconds left at the observed rate, `null` until there is one. */
  eta_seconds: number | null;
  requested_at: string;
  finished_at: string | null;
  error: string | null;
}

/** A snapshot job as a pipeline's page shows it: who asked, why, and each table's progress. */
export interface JobDetail extends Job {
  tables: { table: string; status: string; rows_done: number; estimate: number | null }[];
  /** Rows a second, observed. */
  rate: number | null;
  requested_by: string;
  requested_role: string | null;
  note: string | null;
  truncate: boolean;
  cancel: { by: string; role: string | null; at: string; note: string | null } | null;
}

/** PostgreSQL's source detail; MySQL's has no slot. */
export interface SourceDetail {
  engine: 'postgres' | 'mysql';
  /** The server's write position, tagged: `pg:3/A41C2B10`. */
  head?: string;
  slot?: {
    name: string;
    wal_held_bytes: number | null;
    active?: boolean;
    confirmed_flush?: string | null;
  } | null;
  max_slot_wal_keep_size?: number | null;
  publication?: { name: string; tables: number } | null;
  /** MySQL: how long the server keeps a binlog; `null` when it never expires them. */
  binlog_expire_seconds?: number | null;
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
  source: {
    detail: SourceDetail | null;
    read_at?: string | null;
    /** MySQL: how far the stream reads behind the source's newest change. */
    behind_seconds?: number | null;
  } | null;
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
  /** What else it concerns, by name: a table, a job id, a node. */
  object?: string;
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

/** `GET /v1/pipelines/{id}`: the pipeline, its lease and its halted tables, from storage. */
export interface PipelineDetail {
  id: string;
  name: string;
  desired_state: Desired;
  actual_state: Actual | null;
  source: SourceAt;
  sink: SinkAt;
  error: string | null;
  degraded: string | null;
  holder: {
    node: string;
    owner_epoch: number;
    renewed_at: string;
    released: boolean;
    stale: boolean;
    suspect: boolean;
  } | null;
  /** The committed position, tagged. */
  position: string | null;
  halted_tables: { table: string; code: string; detail: string; since: string | null }[];
}

/** A removal not torn down yet (`GET /v1/cluster/removals`, DA-API-21). */
export interface Removal {
  id: string;
  /** The last config generation that had the pipeline. */
  generation: number;
  removed_at: string;
}

/** A node's process, as its detail and its live `node` message show it. */
export interface Process {
  memory_bytes: number | null;
  memory_limit_bytes: number | null;
  cpu_seconds: number | null;
  cpu_cores: number | null;
  cpu_limit_cores: number | null;
}

/** What a node's gossip saw: its peers, its last probe, the suspicions of the last day. */
export interface Gossip {
  peers: { node: string; state: 'alive' | 'suspect'; phase: string }[];
  last_probe: { node: string; at: string } | null;
  probe_rtt_ms: number | null;
  suspicions: {
    raised: { node: string; at: string }[];
    received: number;
    raised_more: number;
    received_more: number;
  };
}

/** The stream's `node` message: a node's own figures, sent every 5 s. */
export interface NodeFigures {
  node: string;
  at: string;
  phase: string | null;
  process: Process;
  gossip: Gossip;
  /** The pipelines it runs. */
  pipelines: string[];
}
