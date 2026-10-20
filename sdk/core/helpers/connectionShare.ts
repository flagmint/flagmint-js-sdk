/**
 * Same-origin connection sharing for Flagmint streaming transports.
 *
 * When two or more FlagClient instances run in the same origin — typical cases
 * are a host page plus sibling iframes, or two widgets on one Python-embedded
 * page — each would otherwise open its own ASL handshake and SSE/long-poll
 * stream. That doubles evaluation quota and is almost never intended.
 *
 * This hub elects a single **leader** per API key:
 *
 * - **Leader** holds the real transport (handshake + EventSource).
 * - **Followers** skip handshake and EventSource.
 *
 * Two modes (separate BroadcastChannel / lock keys so they never mix):
 *
 * - **`evaluated`** (server eval, default): leader broadcasts evaluated flag
 *   maps; followers forward `updateContext` onto the leader stream (last write
 *   wins). Same behavior as before.
 * - **`rules`** (config sync): leader broadcasts verified **rules snapshots**;
 *   each tab keeps its own context and evaluates locally. Followers do **not**
 *   forward context to the leader.
 *
 * Leadership is stored in `localStorage` with a short TTL and heartbeat.
 * `destroy()` and a `pagehide` / `beforeunload` listener post `bye` and release
 * the lock so another member can take over immediately.
 *
 * Trust: `BroadcastChannel` and `localStorage` are same-origin. If the origin
 * serves untrusted content, set `shareConnection: false`.
 *
 * @module connectionShare
 */
import type { Transport } from '@/core/transports/Transport';
import type { RulesCacheSnapshot } from '@/core/config-sync/types';
import { logger } from '@/core/helpers/logger';
import { toJsonCloneable } from '@/core/helpers/jsonCloneable';

const LOCK_TTL_MS = 4000;
const HEARTBEAT_MS = 1500;
const FOLLOWER_WAIT_MS = 5000;
const CONTEXT_WAIT_MS = 5000;
const TAKEOVER_STAGGER_MS = 250;

/** Role this document plays in the share group for a given API key. */
export type ShareRole = 'leader' | 'follower';

/**
 * What the leader shares with followers.
 * - `evaluated` — classic server-eval flag maps + context RPC
 * - `rules` — config-sync rules snapshots; local eval per tab
 */
export type ConnectionShareMode = 'evaluated' | 'rules';

/**
 * Messages on `flagmint-share:<apiKey>` (`evaluated`) or
 * `flagmint-share-rules:<apiKey>` (`rules`).
 */
type ShareMessage<C = unknown, T = unknown> =
  | { type: 'hello'; memberId: string }
  | { type: 'alive'; memberId: string }
  | { type: 'flags'; memberId: string; flags: Record<string, T> }
  | {
      type: 'rules';
      memberId: string;
      snapshot: RulesCacheSnapshot;
      analytics?: Record<string, boolean>;
    }
  | { type: 'error'; memberId: string; message: string; code?: string; retryAfter?: number }
  | { type: 'context-request'; memberId: string; requestId: string; context: C; persist?: boolean }
  | { type: 'context-result'; requestId: string; flags?: Record<string, T>; error?: { message: string; code?: string } }
  /** Rules follower: ask the leader to reconnect for a fresh lease / fullConfig. */
  | { type: 'lease-renew-request'; memberId: string }
  | { type: 'bye'; memberId: string };

/**
 * Persistence used for the leadership lock.
 * Defaults to `localStorage` so sibling iframes on the same origin can compete.
 */
export interface ShareStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

/**
 * Cross-document bus. Production uses {@link BroadcastChannel};
 * tests inject an in-memory implementation.
 */
export interface ShareChannel {
  postMessage(data: unknown): void;
  close(): void;
  onmessage: ((event: { data: unknown }) => void) | null;
}

/**
 * Test/runtime hooks. Omit in production — the hub uses `BroadcastChannel`,
 * `localStorage`, and `setInterval`.
 */
export interface ConnectionShareOptions {
  /**
   * `evaluated` (default): server-eval share.
   * `rules`: config-sync share — rules only, per-tab context.
   */
  mode?: ConnectionShareMode;
  /** Override `new BroadcastChannel(name)` (tests). */
  channelFactory?: (name: string) => ShareChannel;
  /** Override `localStorage` for the leadership lock (tests). */
  storage?: ShareStorage;
  /** Clock used for lock expiry. */
  now?: () => number;
  setTimer?: (handler: () => void, ms: number) => ReturnType<typeof setInterval>;
  clearTimer?: (id: ReturnType<typeof setInterval>) => void;
  /** Delay used to stagger lock confirmation (tests inject a no-op). */
  delay?: (ms: number) => Promise<void>;
}

/** `localStorage` payload for `flagmint_share_lock:<apiKey>`. */
interface LockRecord {
  ownerId: string;
  expiresAt: number;
}

/**
 * Whether this runtime can share a stream across documents.
 *
 * @returns `true` in a browser with {@link BroadcastChannel} (includes same-origin
 * iframes). `false` in Node, so cluster workers each keep their own connection.
 */
export function isConnectionSharingAvailable(): boolean {
  return typeof window !== 'undefined' && typeof BroadcastChannel !== 'undefined';
}

function randomMemberId(): string {
  return `fm_${Math.random().toString(36).slice(2)}_${Date.now().toString(36)}`;
}

/**
 * Elects a single streaming leader per API key across same-origin documents.
 *
 * {@link FlagClient} constructs one hub when `shareConnection` is enabled (the
 * browser default). The leader opens SSE; followers use
 * {@link ConnectionShareHub.createFollowerTransport} instead.
 *
 * @template C Evaluation context forwarded from follower `updateContext` calls.
 * @template T Flag value type in snapshots broadcast by the leader.
 */
export class ConnectionShareHub<C = Record<string, unknown>, T = unknown> {
  /** Stable id for this document in channel messages and the lock record. */
  readonly memberId = randomMemberId();
  private role: ShareRole = 'follower';
  private channel: ShareChannel | null = null;
  private heartbeatId: ReturnType<typeof setInterval> | null = null;
  private watchId: ReturnType<typeof setInterval> | null = null;
  private closed = false;
  private latestFlags: Record<string, T> | null = null;
  private latestRules: {
    snapshot: RulesCacheSnapshot;
    analytics?: Record<string, boolean>;
  } | null = null;
  private flagsWaiters: Array<(flags: Record<string, T>) => void> = [];
  private rulesWaiters: Array<(payload: {
    snapshot: RulesCacheSnapshot;
    analytics?: Record<string, boolean>;
  }) => void> = [];
  private flagsTimeouts: Array<{
    timeoutId: ReturnType<typeof setTimeout>;
    reject: (err: Error) => void;
  }> = [];
  private rulesTimeouts: Array<{
    timeoutId: ReturnType<typeof setTimeout>;
    reject: (err: Error) => void;
  }> = [];
  private contextWaiters = new Map<string, {
    resolve: (flags: Record<string, T>) => void;
    reject: (err: Error) => void;
  }>();
  private onPromoteCallback?: () => void | Promise<void>;
  private leaderContextHandler?: (
    context: C,
    options?: { persist?: boolean }
  ) => Promise<Record<string, T>>;
  /** Rules mode: apply leader snapshot → return flags evaluated for this tab. */
  private followerRulesApply?: (payload: {
    snapshot: RulesCacheSnapshot;
    analytics?: Record<string, boolean>;
  }) => Record<string, T>;
  /** Rules mode: re-evaluate after local `updateContext` (no leader RPC). */
  private followerLocalEval?: (context: C) => Record<string, T>;
  /** Rules mode: leader reconnects SSE when a follower reports lease expiry. */
  private leaderLeaseRenewHandler?: () => void;
  private followerFlagsCallback?: (flags: Record<string, T>) => void;
  private followerErrorCallback?: (error: Error) => void;
  private readonly now: () => number;
  private readonly storage: ShareStorage | null;
  private readonly channelFactory?: (name: string) => ShareChannel;
  private readonly setTimer: (handler: () => void, ms: number) => ReturnType<typeof setInterval>;
  private readonly clearTimer: (id: ReturnType<typeof setInterval>) => void;
  private readonly delay: (ms: number) => Promise<void>;
  private unloadHandler?: () => void;
  private readonly lockKey: string;
  private readonly channelName: string;
  private readonly mode: ConnectionShareMode;

  /**
   * @param apiKey Share groups are keyed by API key. Distinct keys never share.
   * @param options Optional test doubles for channel, storage, and timers.
   */
  constructor(
    private readonly apiKey: string,
    options: ConnectionShareOptions = {}
  ) {
    this.mode = options.mode ?? 'evaluated';
    this.lockKey =
      this.mode === 'rules'
        ? `flagmint_share_rules_lock:${apiKey}`
        : `flagmint_share_lock:${apiKey}`;
    this.channelName =
      this.mode === 'rules'
        ? `flagmint-share-rules:${apiKey}`
        : `flagmint-share:${apiKey}`;
    this.now = options.now ?? (() => Date.now());
    this.storage = options.storage ?? (typeof localStorage !== 'undefined' ? localStorage : null);
    this.channelFactory = options.channelFactory;
    this.setTimer = options.setTimer ?? ((handler, ms) => setInterval(handler, ms));
    this.clearTimer = options.clearTimer ?? ((id) => clearInterval(id));
    this.delay = options.delay ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  }

  /** `leader` if this document holds the lock; otherwise `follower`. */
  get currentRole(): ShareRole {
    return this.role;
  }

  /** `evaluated` (server eval) or `rules` (config sync). */
  get shareMode(): ConnectionShareMode {
    return this.mode;
  }

  /**
   * Join the share group: compete for the lock, subscribe to the channel.
   *
   * If {@link BroadcastChannel} is missing, this member becomes leader locally
   * and never shares (same as a Node process).
   *
   * @returns The role this member should take. Only the leader should call
   * ASL handshake / `SseTransport.init()`.
   */
  async join(): Promise<ShareRole> {
    this.channel = this.openChannel();
    if (!this.channel) {
      this.role = 'leader';
      return this.role;
    }

    this.channel.onmessage = (event) => this.handleMessage(event.data as ShareMessage<C, T>);

    if (await this.tryAcquireLock()) {
      this.becomeLeader();
    } else {
      this.role = 'follower';
      this.post({ type: 'hello', memberId: this.memberId });
      this.watchId = this.setTimer(() => this.maybeTakeover(), HEARTBEAT_MS);
      logger.log('[ConnectionShare] Joined as follower; waiting for leader flag snapshot.');
    }

    this.bindUnloadListener();
    return this.role;
  }

  /**
   * Invoked after a follower wins the lock (leader `bye` or expired heartbeat).
   * The client should open a real transport here, then
   * {@link ConnectionShareHub.broadcastFlags}.
   */
  onPromote(callback: () => void | Promise<void>): void {
    this.onPromoteCallback = callback;
  }

  /**
   * Leader-only: run `updateContext` on the real transport when a follower
   * sends `context-request`. The context is whatever that same-origin
   * document posted; the leader does not authenticate the sender beyond origin.
   */
  setLeaderContextHandler(
    handler: (context: C, options?: { persist?: boolean }) => Promise<Record<string, T>>
  ): void {
    this.leaderContextHandler = handler;
  }

  /**
   * Rules-mode follower: hydrate local rules from the leader snapshot and
   * return flags evaluated for **this** document's context.
   *
   * @param handler Apply snapshot + local eval → flag map
   */
  setFollowerRulesApply(
    handler: (payload: {
      snapshot: RulesCacheSnapshot;
      analytics?: Record<string, boolean>;
    }) => Record<string, T>,
  ): void {
    this.followerRulesApply = handler;
  }

  /**
   * Rules-mode follower: re-evaluate after local `updateContext` (no RPC).
   *
   * @param handler Context → flag map using this tab's rules store
   */
  setFollowerLocalEval(handler: (context: C) => Record<string, T>): void {
    this.followerLocalEval = handler;
  }

  /**
   * Rules-mode leader: run when a follower asks for lease renew (SSE reconnect).
   *
   * @param handler Typically `transport.requestReconnect('lease_renew_requested')`
   */
  setLeaderLeaseRenewHandler(handler: () => void): void {
    this.leaderLeaseRenewHandler = handler;
  }

  /**
   * Ask the share leader to renew the config-sync lease (reconnect SSE).
   * Leaders invoke the handler locally; followers post `lease-renew-request`.
   */
  requestLeaseRenew(): void {
    if (this.mode !== 'rules') return;
    if (this.role === 'leader') {
      this.leaderLeaseRenewHandler?.();
      return;
    }
    this.post({ type: 'lease-renew-request', memberId: this.memberId });
  }

  /**
   * Leader-only (non-rules mode): publish the current evaluated flags to all followers.
   * No-op on followers (avoids echo loops). Evaluated mode only.
   *
   * @param flags Evaluated flag map from the leader stream
   */
  broadcastFlags(flags: Record<string, T>): void {
    if (this.mode !== 'evaluated') return;
    if (this.role !== 'leader' || !this.channel) return;
    this.latestFlags = flags;
    this.post({ type: 'flags', memberId: this.memberId, flags });
  }

  /**
   * Leader-only (rules mode): publish a verified rules snapshot.
   * Followers hydrate and evaluate with their own context.
   *
   * @param snapshot Rules store snapshot (MAC already verified on the leader)
   * @param analytics Optional per-flag analytics map from rules
   */
  broadcastRules(
    snapshot: RulesCacheSnapshot,
    analytics?: Record<string, boolean>,
  ): void {
    if (this.mode !== 'rules') return;
    if (this.role !== 'leader' || !this.channel) return;
    this.latestRules = { snapshot, analytics };
    this.post({
      type: 'rules',
      memberId: this.memberId,
      snapshot,
      analytics,
    });
  }

  /** Leader-only: mirror a transport `onError` (auth, quota) to followers. */
  broadcastError(error: Error): void {
    if (this.role !== 'leader' || !this.channel) return;
    this.post({
      type: 'error',
      memberId: this.memberId,
      message: error.message,
      code: (error as { code?: string }).code,
      retryAfter: (error as { retryAfter?: number }).retryAfter,
    });
  }

  /**
   * Transport used by follower FlagClients.
   * - Evaluated: `init` waits for flags; `fetchFlags` RPCs through the leader.
   * - Rules: `init` waits for a rules snapshot; `fetchFlags` evaluates locally.
   * `destroy` is a no-op — {@link ConnectionShareHub.destroy} owns teardown.
   */
  createFollowerTransport(): Transport<C, T> {
    if (this.mode === 'rules') {
      return {
        init: async () => {
          const payload = await this.waitForRules();
          const flags = this.followerRulesApply?.(payload) ?? {};
          this.followerFlagsCallback?.(flags);
        },
        fetchFlags: async (context: C) => {
          if (!this.followerLocalEval) {
            throw new Error('Rules-share follower has no local eval handler.');
          }
          return this.followerLocalEval(context);
        },
        /**
         * Follower has no SSE — ask the leader to reconnect for a fresh lease.
         *
         * @param _reason Ignored; leader always reconnects for renew
         */
        requestReconnect: (_reason?: string) => {
          this.requestLeaseRenew();
        },
        destroy: () => undefined,
        onFlagsUpdated: (callback) => {
          this.followerFlagsCallback = callback;
        },
        onError: (callback) => {
          this.followerErrorCallback = callback;
        },
      };
    }

    return {
      init: async () => {
        const flags = await this.waitForFlags();
        this.followerFlagsCallback?.(flags);
      },
      fetchFlags: (context: C, options?: { persist?: boolean }) =>
        this.requestContextUpdate(context, options),
      destroy: () => undefined,
      onFlagsUpdated: (callback) => {
        this.followerFlagsCallback = callback;
      },
      onError: (callback) => {
        this.followerErrorCallback = callback;
      },
    };
  }

  /**
   * Leave the share group. Leaders release the lock and post `bye` so another
   * iframe can open the stream. Also invoked from `pagehide` / `beforeunload`.
   * Channel close is deferred one tick so `bye` can flush on native
   * {@link BroadcastChannel}.
   */
  destroy(): void {
    if (this.closed) return;
    this.closed = true;
    this.unbindUnloadListener();
    if (this.heartbeatId) this.clearTimer(this.heartbeatId);
    if (this.watchId) this.clearTimer(this.watchId);
    this.heartbeatId = null;
    this.watchId = null;

    if (this.role === 'leader') {
      this.releaseLock();
      this.post({ type: 'bye', memberId: this.memberId });
    }

    const channel = this.channel;
    this.channel = null;
    if (channel) {
      setTimeout(() => channel.close(), 0);
    }
    this.flagsWaiters = [];
    this.rulesWaiters = [];
    this.flagsTimeouts.splice(0).forEach(({ timeoutId, reject }) => {
      clearTimeout(timeoutId);
      reject(new Error('Connection share hub closed.'));
    });
    this.rulesTimeouts.splice(0).forEach(({ timeoutId, reject }) => {
      clearTimeout(timeoutId);
      reject(new Error('Connection share hub closed.'));
    });
    this.contextWaiters.forEach(({ reject }) => reject(new Error('Connection share hub closed.')));
    this.contextWaiters.clear();
  }

  private bindUnloadListener(): void {
    if (typeof window === 'undefined' || this.unloadHandler) return;
    this.unloadHandler = () => this.destroy();
    window.addEventListener('pagehide', this.unloadHandler);
    window.addEventListener('beforeunload', this.unloadHandler);
  }

  private unbindUnloadListener(): void {
    if (typeof window === 'undefined' || !this.unloadHandler) return;
    window.removeEventListener('pagehide', this.unloadHandler);
    window.removeEventListener('beforeunload', this.unloadHandler);
    this.unloadHandler = undefined;
  }

  /**
   * Post a share-hub message after forcing a plain JSON clone.
   *
   * `BroadcastChannel.postMessage` uses the structured-clone algorithm. That
   * rejects Vue reactive Proxies (and similar host objects) with
   * `DataCloneError`, which would break multi-tab share when the app passes
   * a reactive context or flag map.
   *
   * We run {@link toJsonCloneable} first: `JSON.stringify` can walk those
   * Proxies into a plain object tree that structured clone accepts. If the
   * value still cannot be serialized (functions, DOM nodes, cycles), we throw
   * `ERR_CONTEXT_NOT_CLONEABLE` instead of a bare `DataCloneError`.
   *
   * @param message Share protocol payload (flags, rules, context RPC, etc.)
   */
  private post(message: ShareMessage<C, T>): void {
    if (!this.channel) return;
    const payload = toJsonCloneable(message);
    try {
      this.channel.postMessage(payload);
    } catch (err) {
      throw Object.assign(
        new Error(
          'Failed to broadcast on the shared Flagmint connection. Context and flags must be JSON-serializable.'
        ),
        {
          code: 'ERR_CONTEXT_NOT_CLONEABLE',
          cause: err instanceof Error ? err : new Error(String(err)),
        }
      );
    }
  }

  /** Open the BroadcastChannel for this API key, or a test factory channel. */
  private openChannel(): ShareChannel | null {
    if (this.channelFactory) {
      return this.channelFactory(this.channelName);
    }
    if (typeof BroadcastChannel === 'undefined') {
      return null;
    }
    return new BroadcastChannel(this.channelName) as unknown as ShareChannel;
  }

  /** Mark this member leader and start lock heartbeat + `alive` pings. */
  private becomeLeader(): void {
    this.role = 'leader';
    if (this.watchId) {
      this.clearTimer(this.watchId);
      this.watchId = null;
    }
    this.heartbeatId = this.setTimer(() => {
      this.refreshLock();
      this.post({ type: 'alive', memberId: this.memberId });
    }, HEARTBEAT_MS);
    logger.log('[ConnectionShare] Elected leader for API key share group.');
  }

  /**
   * Follower poll: if the lock is free or expired, take it and run
   * {@link ConnectionShareHub.onPromote}.
   */
  private async maybeTakeover(): Promise<void> {
    if (this.closed || this.role !== 'follower') return;
    await this.delay(Math.random() * TAKEOVER_STAGGER_MS);
    if (this.closed || this.role !== 'follower') return;
    if (!await this.tryAcquireLock()) return;
    if (this.storage && this.readLock()?.ownerId !== this.memberId) return;

    this.becomeLeader();
    try {
      await this.onPromoteCallback?.();
    } catch (err) {
      logger.warn('[ConnectionShare] Follower failed to promote to leader.', err);
      this.releaseLock();
      this.role = 'follower';
      if (this.heartbeatId) this.clearTimer(this.heartbeatId);
      this.heartbeatId = null;
      this.watchId = this.setTimer(() => void this.maybeTakeover(), HEARTBEAT_MS);
    }
  }

  /** Dispatch a {@link ShareMessage} from another document in the group. */
  private handleMessage(message: ShareMessage<C, T>): void {
    if (!message || typeof message !== 'object') return;

    switch (message.type) {
      case 'hello':
        if (this.role !== 'leader') break;
        if (this.mode === 'rules' && this.latestRules) {
          this.post({
            type: 'rules',
            memberId: this.memberId,
            snapshot: this.latestRules.snapshot,
            analytics: this.latestRules.analytics,
          });
        } else if (this.mode === 'evaluated' && this.latestFlags) {
          this.post({
            type: 'flags',
            memberId: this.memberId,
            flags: this.latestFlags,
          });
        }
        break;
      case 'flags':
        if (this.mode !== 'evaluated' || this.role === 'leader') return;
        if (!message.flags || typeof message.flags !== 'object' || Array.isArray(message.flags)) {
          return;
        }
        this.latestFlags = message.flags;
        this.flagsWaiters.splice(0).forEach((resolve) => resolve(message.flags));
        this.followerFlagsCallback?.(message.flags);
        break;
      case 'rules':
        if (this.mode !== 'rules' || this.role === 'leader') return;
        if (!message.snapshot || typeof message.snapshot !== 'object') return;
        {
          const payload = {
            snapshot: message.snapshot,
            analytics: message.analytics,
          };
          this.latestRules = payload;
          this.rulesWaiters.splice(0).forEach((resolve) => resolve(payload));
          const flags = this.followerRulesApply?.(payload) ?? {};
          this.followerFlagsCallback?.(flags);
        }
        break;
      case 'error':
        if (this.role === 'leader') return;
        if (typeof message.message !== 'string' || message.message.length === 0) return;
        {
          const err = new Error(message.message);
          Object.assign(err, { code: message.code, retryAfter: message.retryAfter });
          this.followerErrorCallback?.(err);
        }
        break;
      case 'lease-renew-request':
        if (this.mode !== 'rules' || this.role !== 'leader') return;
        this.leaderLeaseRenewHandler?.();
        break;
      case 'context-request':
        // Rules mode never forwards follower context onto the shared stream.
        if (this.mode !== 'evaluated' || this.role !== 'leader') return;
        void this.handleContextRequest(message);
        break;
      case 'context-result':
        {
          const waiter = this.contextWaiters.get(message.requestId);
          if (!waiter) return;
          this.contextWaiters.delete(message.requestId);
          if (message.error) {
            const err = new Error(message.error.message);
            Object.assign(err, { code: message.error.code });
            waiter.reject(err);
          } else if (!message.flags || typeof message.flags !== 'object' || Array.isArray(message.flags)) {
            waiter.reject(new Error('Shared context update returned no flags object.'));
          } else {
            waiter.resolve(message.flags);
          }
        }
        break;
      case 'bye':
        if (this.role === 'follower') {
          void this.maybeTakeover();
        }
        break;
      default:
        break;
    }
  }

  /**
   * Leader: evaluate a follower's context on the real transport and reply.
   * Any same-origin document can trigger this; the resulting flags are
   * broadcast to the whole group.
   */
  private async handleContextRequest(
    message: Extract<ShareMessage<C, T>, { type: 'context-request' }>
  ): Promise<void> {
    if (!this.leaderContextHandler) {
      this.post({
        type: 'context-result',
        requestId: message.requestId,
        error: { message: 'Leader has no active transport for context updates.', code: 'ERR_INTERNAL' },
      });
      return;
    }
    try {
      const flags = await this.leaderContextHandler(message.context, {
        persist: message.persist,
      });
      this.broadcastFlags(flags);
      this.post({
        type: 'context-result',
        requestId: message.requestId,
        flags,
      });
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      this.post({
        type: 'context-result',
        requestId: message.requestId,
        error: { message: error.message, code: (error as { code?: string }).code },
      });
    }
  }

  /**
   * Block until the leader has sent a `flags` snapshot, or {@link FOLLOWER_WAIT_MS}.
   * Used by follower `init()`.
   */
  private waitForFlags(): Promise<Record<string, T>> {
    if (this.latestFlags) return Promise.resolve(this.latestFlags);
    if (this.closed) return Promise.reject(new Error('Connection share hub closed.'));

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        this.flagsWaiters = this.flagsWaiters.filter((waiter) => waiter !== onFlags);
        this.flagsTimeouts = this.flagsTimeouts.filter((entry) => entry.timeoutId !== timeoutId);
        reject(new Error('Timed out waiting for shared flag snapshot from the leader iframe.'));
      }, FOLLOWER_WAIT_MS);

      const onFlags = (flags: Record<string, T>) => {
        clearTimeout(timeoutId);
        this.flagsTimeouts = this.flagsTimeouts.filter((entry) => entry.timeoutId !== timeoutId);
        resolve(flags);
      };
      this.flagsWaiters.push(onFlags);
      this.flagsTimeouts.push({ timeoutId, reject });
    });
  }

  /**
   * Block until the leader has sent a `rules` snapshot, or {@link FOLLOWER_WAIT_MS}.
   *
   * @returns Latest rules payload from the leader
   */
  private waitForRules(): Promise<{
    snapshot: RulesCacheSnapshot;
    analytics?: Record<string, boolean>;
  }> {
    if (this.latestRules) return Promise.resolve(this.latestRules);
    if (this.closed) return Promise.reject(new Error('Connection share hub closed.'));

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        this.rulesWaiters = this.rulesWaiters.filter((waiter) => waiter !== onRules);
        this.rulesTimeouts = this.rulesTimeouts.filter((entry) => entry.timeoutId !== timeoutId);
        reject(new Error('Timed out waiting for shared rules snapshot from the leader.'));
      }, FOLLOWER_WAIT_MS);

      const onRules = (payload: {
        snapshot: RulesCacheSnapshot;
        analytics?: Record<string, boolean>;
      }) => {
        clearTimeout(timeoutId);
        this.rulesTimeouts = this.rulesTimeouts.filter((entry) => entry.timeoutId !== timeoutId);
        resolve(payload);
      };
      this.rulesWaiters.push(onRules);
      this.rulesTimeouts.push({ timeoutId, reject });
    });
  }

  /**
   * Follower `fetchFlags`: ask the leader to POST `/context` and wait for
   * `context-result`. Leaders run the handler locally.
   *
   * The posted context is not origin-isolated beyond BroadcastChannel itself.
   * Untrusted same-origin frames must not share a connection with a trusted
   * leader — set `shareConnection: false` on those clients.
   */
  private requestContextUpdate(
    context: C,
    options?: { persist?: boolean }
  ): Promise<Record<string, T>> {
    if (this.role === 'leader') {
      if (!this.leaderContextHandler) {
        return Promise.reject(new Error('Leader has no active transport for context updates.'));
      }
      return this.leaderContextHandler(context, options);
    }

    const requestId = randomMemberId();
    return new Promise<Record<string, T>>((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        this.contextWaiters.delete(requestId);
        reject(new Error('Timed out waiting for shared context update.'));
      }, CONTEXT_WAIT_MS);

      this.contextWaiters.set(requestId, {
        resolve: (flags) => {
          clearTimeout(timeoutId);
          resolve(flags);
        },
        reject: (err) => {
          clearTimeout(timeoutId);
          reject(err);
        },
      });

      try {
        this.post({
          type: 'context-request',
          memberId: this.memberId,
          requestId,
          context,
          persist: options?.persist,
        });
      } catch (err) {
        this.contextWaiters.delete(requestId);
        clearTimeout(timeoutId);
        reject(err instanceof Error ? err : new Error(String(err)));
      }
    });
  }

  /** Read the leadership lock, or `null` if missing/corrupt. */
  private readLock(): LockRecord | null {
    if (!this.storage) return null;
    try {
      const raw = this.storage.getItem(this.lockKey);
      if (!raw) return null;
      const parsed = JSON.parse(raw) as LockRecord;
      if (!parsed?.ownerId || typeof parsed.expiresAt !== 'number') return null;
      return parsed;
    } catch {
      return null;
    }
  }

  /**
   * Write the lock, wait so a competing write can land, then re-read.
   * localStorage has no compare-and-set; the delay plus confirm is the
   * best we can do so two iframes do not both become leader.
   *
   * @returns `true` if this member still owns the lock after the confirm.
   */
  private async tryAcquireLock(): Promise<boolean> {
    if (!this.storage) return true;
    const now = this.now();
    const current = this.readLock();
    if (current && current.ownerId !== this.memberId && current.expiresAt > now) {
      return false;
    }
    const next: LockRecord = { ownerId: this.memberId, expiresAt: now + LOCK_TTL_MS };
    this.storage.setItem(this.lockKey, JSON.stringify(next));
    await this.delay(Math.random() * TAKEOVER_STAGGER_MS);
    if (this.closed) {
      this.releaseLock();
      return false;
    }
    return this.readLock()?.ownerId === this.memberId;
  }

  /** Extend lock expiry; no-op if another member stole the lock. */
  private refreshLock(): void {
    if (this.role !== 'leader' || !this.storage) return;
    const current = this.readLock();
    if (current && current.ownerId !== this.memberId && current.expiresAt > this.now()) {
      return;
    }
    this.storage.setItem(
      this.lockKey,
      JSON.stringify({ ownerId: this.memberId, expiresAt: this.now() + LOCK_TTL_MS })
    );
  }

  /** Drop the lock only if this member still owns it. */
  private releaseLock(): void {
    const current = this.readLock();
    if (current?.ownerId === this.memberId) {
      this.storage?.removeItem(this.lockKey);
    }
  }
}
