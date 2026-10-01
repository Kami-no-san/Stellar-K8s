/**
 * DR API client — Issue #92
 *
 * Typed REST wrappers for the disaster-recovery backend surfaces consumed by
 * DrCommandCenter. The WebSocket stream (real-time phase updates) is handled
 * directly by the component via useReconnectingWs; this module covers the
 * trigger / status / reset REST calls.
 */

// ─── Phase / status types ────────────────────────────────────────────────────

export type DrPhaseKey =
  | 'snapshot_restoration'
  | 'pod_recreation'
  | 'catchup_sync'
  | 'traffic_redirection';

export type DrPhaseStatus =
  | 'pending'
  | 'running'
  | 'passed'
  | 'failed'
  | 'skipped';

export interface DrPhaseState {
  status:      DrPhaseStatus;
  startedAt:   string | null;   // ISO-8601
  completedAt: string | null;   // ISO-8601
  message:     string;
  durationMs:  number | null;
}

export interface DrNodeState {
  node:        string;
  drillId:     string | null;
  startedAt:   string | null;
  completedAt: string | null;
  phases:      Record<DrPhaseKey, DrPhaseState>;
}

// ─── Request / response shapes ───────────────────────────────────────────────

export interface DrTriggerRequest {
  /** Target cluster node (e.g. "validator-primary"). */
  node:     string;
  /** true → dry-run (no real failover). Default: true. */
  dry_run?: boolean;
}

export interface DrTriggerResponse {
  drill_id:   string;
  node:       string;
  dry_run:    boolean;
  queued_at:  string;   // ISO-8601
}

export interface DrStatusResponse {
  drill_id:        string;
  node:            string;
  overall_status:  DrPhaseStatus | 'complete';
  phases:          Record<DrPhaseKey, DrPhaseState>;
  started_at:      string | null;
  completed_at:    string | null;
}

export interface DrResetResponse {
  node:     string;
  reset_at: string;   // ISO-8601
}

// ─── WebSocket frame types ───────────────────────────────────────────────────

export type DrWsMessage =
  | {
      type:      'phase_update';
      node:      string;
      phase:     DrPhaseKey;
      status:    DrPhaseStatus;
      message?:  string;
      timestamp: string;
    }
  | {
      type:      'drill_started';
      node:      string;
      drill_id:  string;
      timestamp: string;
    }
  | {
      type:      'drill_complete';
      node:      string;
      drill_id:  string;
      status:    'passed' | 'failed';
      timestamp: string;
    };

// ─── Error class ─────────────────────────────────────────────────────────────

export class DrApiError extends Error {
  constructor(
    public readonly statusCode: number,
    message: string,
    public readonly path: string,
  ) {
    super(`DR API ${statusCode} on ${path}: ${message}`);
    this.name = 'DrApiError';
  }
}

// ─── Client ──────────────────────────────────────────────────────────────────

export class DrClient {
  private readonly base:    string;
  private readonly headers: Record<string, string>;

  constructor(base = '', extraHeaders: Record<string, string> = {}) {
    this.base    = base.replace(/\/$/, '');
    this.headers = { 'Content-Type': 'application/json', ...extraHeaders };
  }

  private async req<T>(method: string, path: string, body?: unknown): Promise<T> {
    const res = await fetch(`${this.base}${path}`, {
      method,
      headers: this.headers,
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });

    if (!res.ok) {
      let detail = res.statusText;
      try { detail = ((await res.json()) as { message?: string }).message ?? detail; } catch { /* */ }
      throw new DrApiError(res.status, detail, path);
    }

    return res.json() as Promise<T>;
  }

  /**
   * POST /api/dr/trigger
   *
   * Enqueues a dry-run (default) or live failover drill for the given node.
   * The returned drill_id can be used to subscribe to WebSocket status frames.
   */
  trigger(req: DrTriggerRequest): Promise<DrTriggerResponse> {
    return this.req('POST', '/api/dr/trigger', { node: req.node, dry_run: req.dry_run ?? true });
  }

  /**
   * GET /api/dr/status/:drillId
   *
   * Fetch current phase-by-phase state for a drill.
   * Use this to hydrate the dashboard after a page reload; prefer the
   * WebSocket stream for live updates.
   */
  status(drillId: string): Promise<DrStatusResponse> {
    return this.req('GET', `/api/dr/status/${drillId}`);
  }

  /**
   * POST /api/dr/reset
   *
   * Clear backend drill state for the given node.
   * Intended for development / test environments.
   */
  reset(node: string): Promise<DrResetResponse> {
    return this.req('POST', '/api/dr/reset', { node });
  }

  /** WebSocket URL for the real-time DR event stream. */
  wsUrl(wsBase = 'ws://localhost:8080'): string {
    return `${wsBase}/api/dr/stream`;
  }
}

/** Default singleton for single-backend apps. */
export const drClient = new DrClient();
export default drClient;
