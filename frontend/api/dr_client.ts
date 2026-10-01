/**
 * DR API client — Issue #92
 *
 * Typed interface for all disaster-recovery REST API endpoints consumed by
 * the DR Command Center dashboard.  The WebSocket stream is handled directly
 * in DrCommandCenter.jsx via the useReconnectingWs hook; this module covers
 * the REST trigger/reset/status surfaces.
 *
 * All functions are pure fetch wrappers with no framework dependency so they
 * can be used from React components, scripts, or integration tests without
 * modification.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

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
  status: DrPhaseStatus;
  startedAt: string | null;   // ISO-8601
  completedAt: string | null; // ISO-8601
  message: string;
  durationMs: number | null;
}

export interface DrNodeState {
  node: string;
  drillId: string | null;
  startedAt: string | null;
  completedAt: string | null;
  phases: Record<DrPhaseKey, DrPhaseState>;
}

export interface DrTriggerRequest {
  /** Target cluster node name (e.g. "validator-primary"). */
  node: string;
  /** When true the drill executes in dry-run mode — no real failover occurs. */
  dry_run?: boolean;
}

export interface DrTriggerResponse {
  drill_id: string;
  node: string;
  dry_run: boolean;
  queued_at: string; // ISO-8601
}

export interface DrStatusResponse {
  drill_id: string;
  node: string;
  overall_status: DrPhaseStatus | 'complete';
  phases: Record<DrPhaseKey, DrPhaseState>;
  started_at: string | null;
  completed_at: string | null;
}

export interface DrResetResponse {
  node: string;
  reset_at: string;
}

// ---------------------------------------------------------------------------
// WebSocket message types
// ---------------------------------------------------------------------------

export type DrWsMessage =
  | {
      type: 'phase_update';
      node: string;
      phase: DrPhaseKey;
      status: DrPhaseStatus;
      message?: string;
      timestamp: string;
    }
  | {
      type: 'drill_started';
      node: string;
      drill_id: string;
      timestamp: string;
    }
  | {
      type: 'drill_complete';
      node: string;
      drill_id: string;
      status: 'passed' | 'failed';
      timestamp: string;
    };

// ---------------------------------------------------------------------------
// Client class
// ---------------------------------------------------------------------------

export class DrClient {
  private readonly base: string;
  private readonly defaultHeaders: Record<string, string>;

  constructor(base = '', headers: Record<string, string> = {}) {
    this.base = base.replace(/\/$/, '');
    this.defaultHeaders = {
      'Content-Type': 'application/json',
      ...headers,
    };
  }

  private async request<T>(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<T> {
    const resp = await fetch(`${this.base}${path}`, {
      method,
      headers: this.defaultHeaders,
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });

    if (!resp.ok) {
      let detail = resp.statusText;
      try {
        const json = (await resp.json()) as { message?: string };
        detail = json.message ?? detail;
      } catch {
        /* ignore parse error */
      }
      throw new DrApiError(resp.status, detail, path);
    }

    return resp.json() as Promise<T>;
  }

  /**
   * POST /api/dr/trigger
   *
   * Enqueues a dry-run (default) or live failover drill for the given node.
   * The returned drill_id can be used to subscribe to WebSocket updates and
   * to poll status.
   */
  async trigger(req: DrTriggerRequest): Promise<DrTriggerResponse> {
    return this.request<DrTriggerResponse>('POST', '/api/dr/trigger', {
      node: req.node,
      dry_run: req.dry_run ?? true,
    });
  }

  /**
   * GET /api/dr/status/:drillId
   *
   * Polls the current phase-by-phase execution state for a drill.
   * Prefer the WebSocket stream for live updates; use this for initial
   * hydration after a page reload.
   */
  async status(drillId: string): Promise<DrStatusResponse> {
    return this.request<DrStatusResponse>('GET', `/api/dr/status/${drillId}`);
  }

  /**
   * POST /api/dr/reset
   *
   * Clears backend drill state for the given node.  Intended for
   * development / test environments; guarded by the backend in production.
   */
  async reset(node: string): Promise<DrResetResponse> {
    return this.request<DrResetResponse>('POST', '/api/dr/reset', { node });
  }

  /**
   * Returns the WebSocket URL for a given drill.
   * Callers should pass this to `useReconnectingWs` in DrCommandCenter.
   */
  wsUrl(wsBase = 'ws://localhost:8080'): string {
    return `${wsBase}/api/dr/stream`;
  }
}

// ---------------------------------------------------------------------------
// Error class
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Default singleton (convenience export for typical single-backend setups)
// ---------------------------------------------------------------------------

export const drClient = new DrClient();

export default drClient;
