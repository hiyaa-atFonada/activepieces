import { createHash } from 'node:crypto';
import type { PreparedChunk } from './audio';

async function transcribeChunks({ chunks, language, checkpointKey, store, send, policy, clock, log }: TranscribeParams): Promise<TranscribeResult> {
  const loaded = await loadCheckpoint({ store, checkpointKey, chunkCount: chunks.length, log });
  const texts: Array<string | undefined> = chunks.map((_, index) => loaded[index]);
  const reusedChunks = texts.filter((text) => typeof text === 'string').length;
  const pending = chunks.map((_, index) => index).filter((index) => typeof texts[index] !== 'string');
  const totals = emptyTotals();
  const checkpoint: CheckpointState = { savedChunks: reusedChunks, disabled: false, writes: Promise.resolve() };
  const shared: SharedState = { nextStartAt: 0, stop: false, failures: [] };
  const workerCount = Math.max(1, Math.min(policy.concurrency, pending.length));
  log('checkpoint_loaded', { chunkCount: chunks.length, reusedChunks, pendingChunks: pending.length, concurrency: workerCount });

  const worker = async (): Promise<void> => {
    while (!shared.stop) {
      const index = pending.shift();
      if (index === undefined) return;
      const outcome = await transcribeOneChunk({ chunk: chunks[index], index, chunkCount: chunks.length, language, send, policy, clock, log, shared });
      addStats({ totals, stats: outcome.stats, chunkBytes: chunks[index].bytes.length });
      if (outcome.kind === 'failed') {
        shared.stop = true;
        shared.failures.push({ index, failure: outcome.failure });
        return;
      }
      texts[index] = outcome.text;
      checkpoint.writes = checkpoint.writes.then(() => saveCheckpoint({ store, checkpointKey, chunkCount: chunks.length, texts, checkpoint, log }));
      await checkpoint.writes;
    }
  };
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  await checkpoint.writes;

  const firstFailure = [...shared.failures].sort((a, b) => a.index - b.index)[0];
  if (firstFailure) {
    const { index, failure } = firstFailure;
    log('run_failed', { chunk: index + 1, chunkCount: chunks.length, status: failure.status ?? null, attempts: failure.attempts, retried: failure.attempts > 1, checkpointedChunks: checkpoint.savedChunks, reason: failure.reason, ...totals });
    throw new Error(describeFailure({ failure, index, chunkCount: chunks.length, savedChunks: checkpoint.savedChunks, reusedChunks }));
  }

  const ordered = texts.filter((text): text is string => typeof text === 'string');
  if (ordered.length !== chunks.length) throw new Error(`Internal ordering error: ${ordered.length} of ${chunks.length} chunk transcripts are present.`);
  const { error: deleteError } = await settle(store.delete(checkpointKey));
  if (deleteError) log('checkpoint_delete_failed', { reason: errorText(deleteError) });
  return {
    text: ordered.join('\n'),
    chunks: chunks.length,
    reusedChunks,
    stats: { ...totals, transcribedChunks: chunks.length - reusedChunks, concurrency: workerCount },
  };
}

async function transcribeOneChunk({ chunk, index, chunkCount, language, send, policy, clock, log, shared }: ChunkParams): Promise<ChunkOutcome> {
  const stats: ChunkStats = { attempts: 0, rateLimitRetries: 0, upstreamRetries: 0, timeoutRetries: 0, retryWaitMs: 0, pacingWaitMs: 0, asrMs: 0, maxAsrMs: 0 };
  const fail = ({ status, reason }: { status?: number; reason: string }): ChunkOutcome => ({ kind: 'failed', stats, failure: { status, reason, attempts: stats.attempts, rateLimitRetries: stats.rateLimitRetries, upstreamRetries: stats.upstreamRetries + stats.timeoutRetries, recoverUpstream: policy.recoverUpstream } });
  const attemptTimeoutMs = policy.attemptTimeoutMs + Math.ceil(chunk.bytes.length / 1_000_000) * ATTEMPT_TIMEOUT_PER_MB_MS;
  while (true) {
    if (shared.stop) return fail({ reason: 'stopped because another chunk failed' });
    const waited = await acquireStartSlot({ shared, policy, clock });
    if (waited === undefined) return fail({ reason: 'stopped before the action time budget ran out' });
    if (shared.stop) return fail({ reason: 'stopped because another chunk failed' });
    if (stats.attempts > 0) stats.retryWaitMs += waited;
    else stats.pacingWaitMs += waited;
    const timeoutMs = Math.min(attemptTimeoutMs, policy.deadlineAt - clock.now());
    const started = clock.now();
    stats.attempts++;
    const { data: response, error } = await settle(send({ chunk, language, timeoutMs }));
    const elapsed = clock.now() - started;
    stats.asrMs += elapsed;
    stats.maxAsrMs = Math.max(stats.maxAsrMs, elapsed);
    const attemptLog = { chunk: index + 1, chunkCount, attempt: stats.attempts, bytes: chunk.bytes.length, audioSeconds: chunk.audioSeconds === undefined ? null : Math.round(chunk.audioSeconds * 10) / 10, ms: elapsed, timeoutMs };

    if (error || !response) {
      const timedOut = isTimeout(error);
      log('chunk_attempt', { ...attemptLog, status: null, outcome: timedOut ? 'timeout' : 'network_error' });
      if (!timedOut) return fail({ reason: `${errorText(error)}; not retried because the request may already have reached FonadaLabs` });
      if (!policy.recoverUpstream || stats.upstreamRetries + stats.timeoutRetries >= policy.maxUpstreamRetries) {
        return fail({ reason: `no response within ${Math.round(timeoutMs / 1000)} s${policy.recoverUpstream ? '' : '; enable "Retry upstream failures" to retry'}` });
      }
      stats.timeoutRetries++;
      const delayMs = upstreamDelayMs({ policy, retry: stats.upstreamRetries + stats.timeoutRetries });
      if (!scheduleCooldown({ shared, policy, clock, delayMs })) return fail({ reason: `no response within ${Math.round(timeoutMs / 1000)} s; the retry wait would exceed the action time budget` });
      log('chunk_retry_wait', { chunk: index + 1, status: null, cause: 'timeout', waitMs: delayMs, upstreamRetries: stats.upstreamRetries + stats.timeoutRetries });
      continue;
    }

    log('chunk_attempt', { ...attemptLog, status: response.status, outcome: response.status >= 200 && response.status < 300 ? 'ok' : 'http_error' });
    if (response.status >= 200 && response.status < 300) {
      const text = extractText(response.body);
      if (!text) return fail({ status: response.status, reason: 'response contained no transcript text; refusing a partial transcript' });
      return { kind: 'ok', text, stats };
    }
    const delayMs = retryDelayMs({ response, stats, policy });
    if (delayMs === undefined) return fail({ status: response.status, reason: providerMessage(response) });
    if (delayMs > MAX_SINGLE_WAIT_MS || !scheduleCooldown({ shared, policy, clock, delayMs })) {
      return fail({ status: response.status, reason: `${providerMessage(response)}; the ${Math.round(delayMs / 1000)} s retry wait would exceed the action time budget` });
    }
    if (response.status === 429) stats.rateLimitRetries++;
    else stats.upstreamRetries++;
    log('chunk_retry_wait', { chunk: index + 1, status: response.status, waitMs: delayMs, rateLimitRetries: stats.rateLimitRetries, upstreamRetries: stats.upstreamRetries + stats.timeoutRetries });
  }
}

async function acquireStartSlot({ shared, policy, clock }: { shared: SharedState; policy: TranscribePolicy; clock: Clock }): Promise<number | undefined> {
  let waited = 0;
  while (true) {
    const wait = shared.nextStartAt - clock.now();
    if (wait <= 0) {
      if (clock.now() + MIN_REQUEST_WINDOW_MS > policy.deadlineAt) return undefined;
      shared.nextStartAt = clock.now() + policy.requestIntervalMs;
      return waited;
    }
    if (clock.now() + wait + MIN_REQUEST_WINDOW_MS > policy.deadlineAt) return undefined;
    await clock.sleep(wait);
    waited += wait;
    if (shared.stop) return waited;
  }
}

function scheduleCooldown({ shared, policy, clock, delayMs }: { shared: SharedState; policy: TranscribePolicy; clock: Clock; delayMs: number }): boolean {
  const resumeAt = clock.now() + delayMs;
  if (resumeAt + MIN_REQUEST_WINDOW_MS > policy.deadlineAt) return false;
  shared.nextStartAt = Math.max(shared.nextStartAt, resumeAt);
  return true;
}

function retryDelayMs({ response, stats, policy }: { response: SendResponse; stats: ChunkStats; policy: TranscribePolicy }): number | undefined {
  if (response.status === 429 && stats.rateLimitRetries < policy.maxRateLimitRetries) {
    const retryAfterMs = parseRetryAfterMs({ header: response.retryAfter, now: Date.now() });
    return Math.max(RATE_LIMIT_FLOOR_MS, retryAfterMs ?? 0);
  }
  const used = stats.upstreamRetries + stats.timeoutRetries;
  if (UPSTREAM_STATUSES.includes(response.status) && policy.recoverUpstream && used < policy.maxUpstreamRetries) {
    return upstreamDelayMs({ policy, retry: used + 1 });
  }
  return undefined;
}

function upstreamDelayMs({ policy, retry }: { policy: TranscribePolicy; retry: number }): number {
  const base = Math.min(MAX_UPSTREAM_WAIT_MS, policy.upstreamRetryWaitMs * 2 ** (retry - 1));
  return Math.round(base * (1 - JITTER + Math.random() * JITTER * 2));
}

function parseRetryAfterMs({ header, now }: { header: string | null; now: number }): number | undefined {
  if (!header) return undefined;
  const trimmed = header.trim();
  if (/^\d+$/.test(trimmed)) return Number(trimmed) * 1000;
  const date = Date.parse(trimmed);
  return Number.isFinite(date) ? Math.max(0, date - now) : undefined;
}

function addStats({ totals, stats, chunkBytes }: { totals: Totals; stats: ChunkStats; chunkBytes: number }): void {
  totals.attempts += stats.attempts;
  totals.rateLimitRetries += stats.rateLimitRetries;
  totals.upstreamRetries += stats.upstreamRetries;
  totals.timeoutRetries += stats.timeoutRetries;
  totals.retryWaitMs += stats.retryWaitMs;
  totals.pacingWaitMs += stats.pacingWaitMs;
  totals.asrMs += stats.asrMs;
  totals.maxAsrMs = Math.max(totals.maxAsrMs, stats.maxAsrMs);
  totals.uploadedBytes += chunkBytes * stats.attempts;
  if (stats.attempts > 1) totals.retriedChunks++;
}

function emptyTotals(): Totals {
  return { attempts: 0, retriedChunks: 0, rateLimitRetries: 0, upstreamRetries: 0, timeoutRetries: 0, retryWaitMs: 0, pacingWaitMs: 0, asrMs: 0, maxAsrMs: 0, uploadedBytes: 0 };
}

function extractText(body: unknown): string | undefined {
  if (!isRecord(body)) return undefined;
  const candidate = [body['text'], body['transcription'], body['transcript']].find((value) => typeof value === 'string' && value.trim());
  return typeof candidate === 'string' ? candidate.trim() : undefined;
}

function providerMessage(response: SendResponse): string {
  const detail = isRecord(response.body) ? response.body['detail'] : undefined;
  if (isRecord(detail) && typeof detail['message'] === 'string') return detail['message'];
  if (typeof detail === 'string') return detail;
  return STATUS_MESSAGES[response.status] ?? 'upstream request failed';
}

function describeFailure({ failure, index, chunkCount, savedChunks, reusedChunks }: { failure: ChunkFailure; index: number; chunkCount: number; savedChunks: number; reusedChunks: number }): string {
  const status = failure.status === undefined ? 'no HTTP response' : `HTTP ${failure.status}`;
  const retries = failure.attempts > 1
    ? `retried: yes (${failure.attempts} attempts; ${failure.rateLimitRetries} after HTTP 429, ${failure.upstreamRetries} after HTTP 5xx or timeout)`
    : `retried: no${failure.status !== undefined && UPSTREAM_STATUSES.includes(failure.status) && !failure.recoverUpstream ? ' (enable "Retry upstream failures" to retry HTTP 5xx)' : ''}`;
  return [
    `FonadaLabs failed on chunk ${index + 1} of ${chunkCount} (${status}; ${retries}): ${failure.reason}.`,
    `${savedChunks} of ${chunkCount} chunk transcripts are checkpointed (${reusedChunks} reused from an earlier run, ${savedChunks - reusedChunks} saved in this run).`,
    `Re-run the same recording with the same language, chunk duration and connection to resume. Attempts that reached FonadaLabs may already be billed.`,
  ].join(' ');
}

async function loadCheckpoint({ store, checkpointKey, chunkCount, log }: { store: CheckpointStore; checkpointKey: string; chunkCount: number; log: Logger }): Promise<Record<number, string>> {
  const { data, error } = await settle(store.get(checkpointKey));
  if (error) {
    log('checkpoint_load_failed', { reason: errorText(error) });
    return {};
  }
  if (!isRecord(data) || data['chunkCount'] !== chunkCount || !isRecord(data['texts'])) return {};
  const texts = data['texts'];
  return Object.fromEntries(Object.entries(texts).filter((entry): entry is [string, string] => typeof entry[1] === 'string' && entry[1].length > 0).map(([key, value]) => [Number(key), value]));
}

async function saveCheckpoint({ store, checkpointKey, chunkCount, texts, checkpoint, log }: { store: CheckpointStore; checkpointKey: string; chunkCount: number; texts: Array<string | undefined>; checkpoint: CheckpointState; log: Logger }): Promise<void> {
  if (checkpoint.disabled) return;
  const value = { chunkCount, texts: Object.fromEntries(texts.flatMap((text, index) => (typeof text === 'string' ? [[index, text]] : []))) };
  const size = Buffer.byteLength(JSON.stringify(value));
  if (size > CHECKPOINT_MAX_BYTES) {
    checkpoint.disabled = true;
    log('checkpoint_disabled', { reason: 'checkpoint would exceed the Activepieces store value limit', bytes: size });
    return;
  }
  const { error } = await settle(store.put(checkpointKey, value));
  if (error) {
    log('checkpoint_save_failed', { reason: errorText(error) });
    return;
  }
  checkpoint.savedChunks = Object.keys(value.texts).length;
}

function checkpointKeyFor({ bytes, language, chunkSeconds, mode, apiKey }: { bytes: Buffer; language: string; chunkSeconds: number; mode: string; apiKey: string }): string {
  const digest = createHash('sha256')
    .update(bytes)
    .update(`\u0000${language}\u0000${chunkSeconds}\u0000${mode}\u0000`)
    .update(createHash('sha256').update(apiKey).digest())
    .digest('hex');
  return `fonadalabs:v2:${digest}`;
}

async function settle<T>(promise: Promise<T>): Promise<{ data: T; error: null } | { data: null; error: unknown }> {
  try {
    return { data: await promise, error: null };
  } catch (error) {
    return { data: null, error };
  }
}

function isTimeout(error: unknown): boolean {
  return error instanceof Error && (error.name === 'TimeoutError' || error.name === 'AbortError');
}

function errorText(error: unknown): string {
  if (isTimeout(error)) return 'request timed out';
  if (error instanceof Error) return error.message;
  return 'unknown error';
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

const MIN_REQUEST_WINDOW_MS = 30_000;
const ATTEMPT_TIMEOUT_PER_MB_MS = 4_000;
const MAX_SINGLE_WAIT_MS = 3_600_000;
const RATE_LIMIT_FLOOR_MS = 60_000;
const MAX_UPSTREAM_WAIT_MS = 120_000;
const JITTER = 0.2;
const CHECKPOINT_MAX_BYTES = 500_000;
const UPSTREAM_STATUSES = [502, 503, 504];
const STATUS_MESSAGES: Record<number, string> = {
  400: 'FonadaLabs rejected the audio or language',
  401: 'FonadaLabs rejected the API key',
  402: 'FonadaLabs reports insufficient credits',
  403: 'FonadaLabs denied access for this API key',
  413: 'request exceeds the FonadaLabs 25 MB limit',
  415: 'FonadaLabs does not support this audio format',
  429: 'FonadaLabs rate limit remained active',
  502: 'FonadaLabs ASR upstream unavailable',
  503: 'FonadaLabs service unavailable',
  504: 'FonadaLabs gateway timed out',
};

export const fonadaTranscription = { transcribeChunks, checkpointKeyFor, parseRetryAfterMs };

type Logger = (event: string, fields: Record<string, unknown>) => void;

type ChunkFailure = { status?: number; reason: string; attempts: number; rateLimitRetries: number; upstreamRetries: number; recoverUpstream: boolean };

type ChunkStats = { attempts: number; rateLimitRetries: number; upstreamRetries: number; timeoutRetries: number; retryWaitMs: number; pacingWaitMs: number; asrMs: number; maxAsrMs: number };

type Totals = Omit<ChunkStats, never> & { retriedChunks: number; uploadedBytes: number };

type ChunkOutcome = { kind: 'ok'; text: string; stats: ChunkStats } | { kind: 'failed'; failure: ChunkFailure; stats: ChunkStats };

type SharedState = { nextStartAt: number; stop: boolean; failures: Array<{ index: number; failure: ChunkFailure }> };

type CheckpointState = { savedChunks: number; disabled: boolean; writes: Promise<void> };

type ChunkParams = {
  chunk: PreparedChunk;
  index: number;
  chunkCount: number;
  language: string;
  send: Sender;
  policy: TranscribePolicy;
  clock: Clock;
  log: Logger;
  shared: SharedState;
};

export type SendResponse = { status: number; retryAfter: string | null; body: unknown };

export type Sender = (params: { chunk: PreparedChunk; language: string; timeoutMs: number }) => Promise<SendResponse>;

export type Clock = { now: () => number; sleep: (ms: number) => Promise<void> };

export type CheckpointStore = {
  get: (key: string) => Promise<unknown>;
  put: (key: string, value: unknown) => Promise<unknown>;
  delete: (key: string) => Promise<unknown>;
};

export type TranscribePolicy = {
  concurrency: number;
  requestIntervalMs: number;
  attemptTimeoutMs: number;
  recoverUpstream: boolean;
  upstreamRetryWaitMs: number;
  maxUpstreamRetries: number;
  maxRateLimitRetries: number;
  deadlineAt: number;
};

export type TranscribeParams = {
  chunks: PreparedChunk[];
  language: string;
  checkpointKey: string;
  store: CheckpointStore;
  send: Sender;
  policy: TranscribePolicy;
  clock: Clock;
  log: Logger;
};

export type TranscribeResult = {
  text: string;
  chunks: number;
  reusedChunks: number;
  stats: Totals & { transcribedChunks: number; concurrency: number };
};
