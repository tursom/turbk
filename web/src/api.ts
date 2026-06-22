export interface Health {
  status: string;
  version: string;
  database: string;
  started_at: string;
  time: string;
}

export interface Counts {
  hosts: number;
  credentials: number;
  jobs: number;
  runs: number;
  snapshots: number;
}

export interface Bootstrap {
  version: string;
  counts: Counts;
  paths: {
    state_dir: string;
    repo_dir: string;
    restore_roots: string[];
  };
  repository: Record<string, string>;
  scheduler: {
    timezone: string;
    max_concurrent_runs: number;
  };
  agent: {
    command_ttl: string;
    default_poll_interval: string;
    max_chunk_check_batch: number;
    max_invalidation_response_hashes: number;
    invalidation_retention_days: number;
  };
  auth: {
    username: string;
    session_ttl_hours: number;
  };
  retention: {
    keep_last: number;
    keep_daily: number;
    keep_weekly: number;
  };
  maintenance: MaintenanceSettings;
}

export interface AuthSession {
  status: string;
  user: string;
  expires_at: string;
}

export interface AppSettings {
  auth: {
    username: string;
    session_ttl_hours: number;
  };
  retention: {
    keep_last: number;
    keep_daily: number;
    keep_weekly: number;
  };
  maintenance: MaintenanceSettings;
}

export interface MaintenanceSettings {
  enabled: boolean;
  timezone: string;
  cleanup_schedule: string;
  compact_enabled: boolean;
  compact_schedule: string;
  error_grace_period: string;
  stale_run_after: string;
  keep_deleted_metadata_days: number;
  compact_min_reclaim_ratio: number;
  compact_min_reclaim_bytes: string;
}

export interface Host {
  id: number;
  name: string;
  source_type: string;
  address: unknown;
  credential_id: unknown;
  credential?: CredentialSummary;
  agent?: AgentCredential;
  agent_status?: AgentStatus;
  status: string;
  last_seen_at: unknown;
  created_at: string;
  updated_at: string;
}

export interface AgentStatus {
  token_subject: string;
  hostname: string;
  agent_version: string;
  mode: string;
  state_dir: unknown;
  catalog_status: unknown;
  repository_id: unknown;
  chunk_generation: number;
  config_generation: number;
  command_generation: number;
  running_run_id: unknown;
  last_error: unknown;
  last_dropped_reason: unknown;
  last_dropped_at: unknown;
  last_seen_at: string;
}

export interface AgentCommand {
  id: number;
  host_id: number;
  job_id?: number;
  run_id?: number;
  type: string;
  status: string;
  payload?: Record<string, unknown>;
  reason?: string;
  created_by?: string;
  created_at: string;
  updated_at: string;
  expires_at: string;
  claimed_at?: string;
  finished_at?: string;
}

export interface CredentialSummary {
  id: number;
  name: string;
  type: string;
}

export interface AgentCredential {
  credential_id: number;
  host_id: number;
  client_id: string;
  client_secret?: string;
  subject?: string;
  created_at: string;
  updated_at: string;
  last_used_at: unknown;
  revoked_at: unknown;
}

export interface Credential {
  id: number;
  name: string;
  type: string;
  client_id?: string;
  client_secret?: string;
  subject?: string;
  created_at: string;
  updated_at: string;
}

export interface Job {
  id: number;
  host_id: unknown;
  credential_id: unknown;
  name: string;
  source_type: string;
  source_config: string;
  enabled: boolean;
  schedule: unknown;
  timezone: string;
  max_runtime_seconds: number;
  retry_attempts: number;
  created_at: string;
  updated_at: string;
}

export interface Run {
  id: number;
  job_id: unknown;
  host_id: unknown;
  status: string;
  started_at: unknown;
  finished_at: unknown;
  error_message: unknown;
  created_at: string;
  progress?: RunProgress;
}

export interface RunProgress {
  run_id: number;
  phase: string;
  total_files: number;
  processed_files: number;
  total_bytes: number;
  processed_bytes: number;
  uploaded_chunks: number;
  reused_chunks: number;
  message: string;
  updated_at: string;
}

export interface RunLog {
  id: number;
  run_id: number;
  level: string;
  message: string;
  created_at: string;
}

export interface Snapshot {
  id: number;
  job_id: unknown;
  host_id: unknown;
  run_id: unknown;
  source_type: string;
  manifest_ref: string;
  file_count: number;
  total_size: number;
  created_at: string;
  deleted_at: unknown;
  delete_reason: unknown;
  deleted_by: unknown;
  health: string;
  health_message: unknown;
  verified_at: unknown;
}

export interface SnapshotTreeEntry {
  path: string;
  name: string;
  type: 'file' | 'dir' | 'symlink';
  size: number;
  mode: number;
  mod_time: string;
  link_target?: string;
  synthetic?: boolean;
}

export interface SnapshotTree {
  snapshot: Snapshot;
  manifest: {
    id: string;
    source_type: string;
    source_root: string;
    created_at: string;
  };
  path: string;
  entries: SnapshotTreeEntry[];
}

export interface RestoreTask {
  id: number;
  snapshot_id: number;
  target_path: string;
  status: string;
  created_at: string;
  updated_at: string;
}

export interface RestoreResult {
  status: string;
  task: RestoreTask;
  snapshot: Snapshot;
  path: string;
}

export interface StorageHealth {
  status: string;
  repo: {
    path: string;
    mode: string;
    modified: string;
  };
  sqlite: {
    path: string;
    size: number;
    modified: string;
  };
  segment: {
    size: string;
    writeMode: string;
    count: number;
    bytes: number;
    appendOnlyRecords: boolean;
  };
  chunks: {
    count: number;
    logical_bytes: number;
    compressed_bytes: number;
    avg_size: number;
  };
  manifests: {
    count: number;
  };
  maintenance: {
    enabled: boolean;
    timezone: string;
    cleanup_schedule: string;
    next_cleanup_at: unknown;
    compact_enabled: boolean;
    compact_schedule: string;
    next_compact_at: unknown;
  };
}

export interface StorageMaintenance {
  status: string;
  mode: string;
  started_at: string;
  finished_at: string;
  retention: {
    policy: {
      keep_last: number;
      keep_daily: number;
      keep_weekly: number;
    };
    expired_snapshots: number;
    active_snapshots: number;
    deleted_snapshots: number;
  };
  segment: {
    count: number;
    bytes: number;
    logical_bytes: number;
    compressed_bytes: number;
    utilization: number;
  };
  chunks: {
    indexed: number;
    referenced: number;
    estimated_orphans: number;
  };
  manifests: {
    active: number;
    errors?: string[];
  };
  verify: {
    verified_chunks: number;
    missing_index: number;
    corrupt_chunks: number;
    errors?: string[];
  };
  compact: {
    rewritten_chunks: number;
    rewritten_bytes: number;
    removed_chunks: number;
    removed_segments: number;
    removed_segment_bytes: number;
    skipped_reason?: string;
  };
  cleanup: {
    stale_runs_failed: number;
    removed_manifests: number;
    removed_manifest_bytes: number;
    removed_chunks: number;
    removed_logical_bytes: number;
    removed_compressed_bytes: number;
    skipped_reason?: string;
    errors?: string[];
  };
}

export interface MaintenanceRun {
  id: number;
  mode: string;
  status: string;
  started_at: string;
  finished_at: unknown;
  skipped_reason: unknown;
  report_json: string;
  error_message: unknown;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  headers.set('Accept', 'application/json');
  const response = await fetch(path, {
    ...init,
    credentials: 'same-origin',
    headers
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || response.statusText);
  }
  return response.json() as Promise<T>;
}

export const api = {
  health: () => request<Health>('/api/v1/health'),
  login: (payload: { username: string; password: string }) =>
    request<AuthSession>('/api/v1/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }),
  logout: () =>
    request<{ status: string }>('/api/v1/auth/logout', {
      method: 'POST'
    }),
  session: () => request<AuthSession>('/api/v1/auth/session'),
  bootstrap: () => request<Bootstrap>('/api/v1/bootstrap'),
  settings: () => request<{ settings: AppSettings }>('/api/v1/settings'),
  updateSettings: (payload: {
    admin_username?: string;
    current_password?: string;
    admin_password?: string;
    session_ttl_hours?: number;
    retention?: {
      keep_last?: number;
      keep_daily?: number;
      keep_weekly?: number;
    };
    maintenance?: Partial<MaintenanceSettings>;
  }) =>
    request<{ settings: AppSettings }>('/api/v1/settings', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }),
  hosts: () => request<{ hosts: Host[] }>('/api/v1/hosts'),
  createHost: (payload: { name: string; source_type: string; address?: string; credential_id?: number; status?: string }) =>
    request<{ host: Host; credential?: Credential; agent?: { client_id: string; client_secret: string } }>('/api/v1/hosts', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }),
  credentials: () => request<{ credentials: Credential[] }>('/api/v1/credentials'),
  createCredential: (payload: { name: string; type: string; payload: Record<string, unknown> }) =>
    request<{ credential: Credential }>('/api/v1/credentials', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }),
  jobs: () => request<{ jobs: Job[] }>('/api/v1/jobs'),
  createJob: (payload: {
    name: string;
    host_id: number;
    source_config: Record<string, unknown>;
    enabled: boolean;
    schedule?: string;
    timezone?: string;
    max_runtime_seconds?: number;
    retry_attempts?: number;
  }) =>
    request<{ job: Job }>('/api/v1/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }),
  updateJob: (
    id: number,
    payload: {
      name?: string;
      source_config?: Record<string, unknown>;
      enabled?: boolean;
      schedule?: string;
      timezone?: string;
      max_runtime_seconds?: number;
      retry_attempts?: number;
    }
  ) =>
    request<{ job: Job }>(`/api/v1/jobs/${id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }),
  runJob: (id: number) =>
    request<{ status: string; run?: Run; command?: AgentCommand; snapshot?: Snapshot; manifest?: Record<string, unknown> }>(`/api/v1/jobs/${id}/run`, {
      method: 'POST'
    }),
  runs: () => request<{ runs: Run[] }>('/api/v1/runs'),
  runLogs: (id: number) => request<{ logs: RunLog[] }>(`/api/v1/runs/${id}/logs`),
  snapshots: () => request<{ snapshots: Snapshot[] }>('/api/v1/snapshots'),
  deleteSnapshot: (id: number) =>
    request<{ status: string; deleted: boolean; snapshot: Snapshot; space_reclaim: { requires_compact: boolean; message: string } }>(
      `/api/v1/snapshots/${id}`,
      {
        method: 'DELETE'
      }
    ),
  deleteSnapshots: (snapshotIds: number[]) =>
    request<{
      status: string;
      results: Array<{ id: number; status: string; deleted: boolean; snapshot?: Snapshot; error?: string }>;
      space_reclaim: { requires_compact: boolean; message: string };
    }>('/api/v1/snapshots/delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ snapshot_ids: snapshotIds })
    }),
  snapshotTree: (id: number, path = '.') =>
    request<SnapshotTree>(`/api/v1/snapshots/${id}/tree?path=${encodeURIComponent(path)}`),
  snapshotDownloadURL: (id: number, path = '.') =>
    `/api/v1/snapshots/${id}/files?path=${encodeURIComponent(path)}`,
  restore: (payload: { snapshot_id: number; path: string; target_path: string }) =>
    request<RestoreResult>('/api/v1/restore', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }),
  restoreTasks: () => request<{ tasks: RestoreTask[] }>('/api/v1/restore/tasks'),
  storageHealth: () => request<StorageHealth>('/api/v1/storage/health'),
  maintenanceRuns: () => request<{ runs: MaintenanceRun[] }>('/api/v1/storage/maintenance/runs'),
  storageMaintenance: (mode = 'retention') =>
    request<StorageMaintenance>('/api/v1/storage/maintenance', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode })
    })
};
