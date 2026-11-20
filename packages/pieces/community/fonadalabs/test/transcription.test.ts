import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { PreparedChunk } from '../src/lib/audio';
import { CheckpointStore, fonadaTranscription, SendResponse, TranscribeParams, TranscribeResult } from '../src/lib/transcription';

beforeEach(() => {
  vi.useFakeTimers({ now: 0 });
  vi.spyOn(Math, 'random').mockReturnValue(0.5);
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('fonadaTranscription.transcribeChunks', () => {
  it('runs up to the configured concurrency and joins transcripts in original order', async () => {
    const harness = createHarness({
      chunkCount: 6,
      script: { 1: [ok('one', 5000)], 2: [ok('two', 300)], 3: [ok('three', 2000)], 4: [ok('four', 100)], 5: [ok('five', 900)], 6: [ok('six', 50)] },
      policy: { concurrency: 4, requestIntervalMs: 0 },
    });
    const result = await run(harness);
    expect(result.text).toBe('one\ntwo\nthree\nfour\nfive\nsix');
    expect(harness.maxInFlight).toBe(4);
    expect(result.stats.concurrency).toBe(4);
    expect(harness.store.has(KEY)).toBe(false);
  });

  it('never exceeds the concurrency limit and honours the shared start interval', async () => {
    const harness = createHarness({ chunkCount: 5, script: allOk({ count: 5, latencyMs: 10_000 }), policy: { concurrency: 2, requestIntervalMs: 2000 } });
    await run(harness);
    expect(harness.maxInFlight).toBe(2);
    const gaps = harness.starts.slice(1).map((start, index) => start - harness.starts[index]);
    expect(Math.min(...gaps)).toBeGreaterThanOrEqual(2000);
  });

  it('is strictly sequential with concurrency 1', async () => {
    const harness = createHarness({ chunkCount: 3, script: allOk({ count: 3, latencyMs: 500 }), policy: { concurrency: 1, requestIntervalMs: 8000 } });
    const result = await run(harness);
    expect(harness.maxInFlight).toBe(1);
    expect(harness.starts).toEqual([0, 8000, 16000]);
    expect(result.text).toBe('t1\nt2\nt3');
  });

  it('reuses checkpointed chunks and only uploads the missing ones', async () => {
    const harness = createHarness({ chunkCount: 3, script: { 2: [ok('two')] }, initial: { chunkCount: 3, texts: { 0: 'one', 2: 'three' } } });
    const result = await run(harness);
    expect(result.text).toBe('one\ntwo\nthree');
    expect(result.reusedChunks).toBe(2);
    expect(harness.sent).toEqual(['chunk-0002.mp3']);
  });

  it('ignores a checkpoint written for a different chunk layout', async () => {
    const harness = createHarness({ chunkCount: 3, script: allOk({ count: 3 }), initial: { chunkCount: 5, texts: { 0: 'stale' } } });
    const result = await run(harness);
    expect(result.text).toBe('t1\nt2\nt3');
    expect(result.reusedChunks).toBe(0);
  });

  it('pauses every worker during a cooldown after an upstream failure', async () => {
    const harness = createHarness({
      chunkCount: 4,
      script: { 1: [rejected(502, undefined, 1000), ok('one')], 2: [ok('two', 50_000)], 3: [ok('three')], 4: [ok('four')] },
      policy: { concurrency: 2, requestIntervalMs: 0, recoverUpstream: true, upstreamRetryWaitMs: 30_000 },
    });
    const result = await run(harness);
    expect(result.text).toBe('one\ntwo\nthree\nfour');
    const afterFailure = harness.starts.filter((start) => start > 1000);
    expect(Math.min(...afterFailure)).toBeGreaterThanOrEqual(31_000);
  });

  it('abandons a hung attempt after the attempt timeout and retries it when recovery is enabled', async () => {
    const harness = createHarness({
      chunkCount: 1,
      script: { 1: [hang(), ok('one')] },
      policy: { concurrency: 1, recoverUpstream: true, attemptTimeoutMs: 20_000, upstreamRetryWaitMs: 30_000 },
    });
    const result = await run(harness);
    expect(result.text).toBe('one');
    expect(harness.timeouts).toEqual([32_000, 32_000]);
    expect(result.stats.timeoutRetries).toBe(1);
    expect(harness.starts).toEqual([0, 32_000 + 30_000]);
  });

  it('does not retry a timed-out attempt unless recovery is enabled', async () => {
    const harness = createHarness({ chunkCount: 1, script: { 1: [hang()] }, policy: { concurrency: 1, attemptTimeoutMs: 20_000 } });
    const error = await runError(harness);
    expect(error).toContain('chunk 1 of 1 (no HTTP response; retried: no): no response within 32 s; enable "Retry upstream failures" to retry');
  });

  it('honours Retry-After on HTTP 429, with a 60 second floor', async () => {
    const harness = createHarness({ chunkCount: 2, script: { 1: [rejected(429, '90'), ok('one')], 2: [rejected(429, '5'), ok('two')] }, policy: { concurrency: 1 } });
    const result = await run(harness);
    expect(result.text).toBe('one\ntwo');
    expect(result.stats.rateLimitRetries).toBe(2);
    expect(harness.starts[1]).toBeGreaterThanOrEqual(90_000);
  });

  it('fails after three HTTP 429 retries on one chunk and reports checkpoints', async () => {
    const harness = createHarness({ chunkCount: 3, script: { 1: [ok('one')], 2: [rejected(429), rejected(429), rejected(429), rejected(429)] }, policy: { concurrency: 1 } });
    const error = await runError(harness);
    expect(error).toContain('chunk 2 of 3 (HTTP 429; retried: yes (4 attempts; 3 after HTTP 429, 0 after HTTP 5xx or timeout))');
    expect(error).toContain('1 of 3 chunk transcripts are checkpointed');
    expect(harness.store.get(KEY)).toEqual({ chunkCount: 3, texts: { 0: 'one' } });
  });

  it('stops launching chunks after a failure but checkpoints chunks already in flight', async () => {
    const harness = createHarness({
      chunkCount: 6,
      script: { 1: [ok('one', 10_000)], 2: [rejected(502, undefined, 100)], 3: [ok('three', 5000)], 4: [ok('four', 3000)] },
      policy: { concurrency: 4, requestIntervalMs: 0 },
    });
    const error = await runError(harness);
    expect(error).toContain('chunk 2 of 6 (HTTP 502; retried: no (enable "Retry upstream failures" to retry HTTP 5xx))');
    expect(error).toContain('3 of 6 chunk transcripts are checkpointed (0 reused from an earlier run, 3 saved in this run)');
    expect(harness.sent).not.toContain('chunk-0005.mp3');
    expect(harness.store.get(KEY)).toEqual({ chunkCount: 6, texts: { 0: 'one', 2: 'three', 3: 'four' } });
  });

  it('retries HTTP 502 with doubling backoff capped at 120 seconds when enabled', async () => {
    const harness = createHarness({ chunkCount: 1, script: { 1: [rejected(502), rejected(502), rejected(502), rejected(502), ok('one')] }, policy: { concurrency: 1, recoverUpstream: true, upstreamRetryWaitMs: 30_000 } });
    const result = await run(harness);
    expect(result.text).toBe('one');
    expect(result.stats.retryWaitMs).toBe(30_000 + 60_000 + 120_000 + 120_000);
  });

  it('never retries a request that failed without an HTTP response', async () => {
    const harness = createHarness({ chunkCount: 2, script: { 1: [ok('one')], 2: ['network'] }, policy: { concurrency: 1 } });
    const error = await runError(harness);
    expect(error).toContain('chunk 2 of 2 (no HTTP response; retried: no)');
    expect(error).toContain('may already have reached FonadaLabs');
  });

  it('stops before the time budget instead of being killed, keeping checkpoints', async () => {
    const harness = createHarness({ chunkCount: 3, script: { 1: [ok('one')], 2: [rejected(429, '600')] }, policy: { concurrency: 1, deadlineAt: 300_000 } });
    const error = await runError(harness);
    expect(error).toContain('chunk 2 of 3 (HTTP 429; retried: no)');
    expect(error).toContain('retry wait would exceed the action time budget');
    expect(error).toContain('1 of 3 chunk transcripts are checkpointed');
  });

  it('refuses an empty transcript instead of returning a partial one', async () => {
    const harness = createHarness({ chunkCount: 2, script: { 1: [ok('one')], 2: [ok('   ')] }, policy: { concurrency: 1 } });
    const error = await runError(harness);
    expect(error).toContain('chunk 2 of 2 (HTTP 200; retried: no): response contained no transcript text');
  });

  it('keeps transcribing when a checkpoint write fails', async () => {
    const harness = createHarness({ chunkCount: 3, script: allOk({ count: 3 }), failPuts: true });
    const result = await run(harness);
    expect(result.text).toBe('t1\nt2\nt3');
    expect(harness.events).toContain('checkpoint_save_failed');
  });

  it('never logs the API key or transcript text', async () => {
    const harness = createHarness({ chunkCount: 1, script: { 1: [ok('secret words')] } });
    await run(harness);
    expect(harness.logLines.join('\n')).not.toMatch(/secret words|sk-test/);
  });
});

describe('fonadaTranscription helpers', () => {
  it('parses Retry-After as seconds or an HTTP date', () => {
    expect(fonadaTranscription.parseRetryAfterMs({ header: '120', now: 0 })).toBe(120_000);
    expect(fonadaTranscription.parseRetryAfterMs({ header: new Date(30_000).toUTCString(), now: 0 })).toBe(30_000);
    expect(fonadaTranscription.parseRetryAfterMs({ header: null, now: 0 })).toBeUndefined();
    expect(fonadaTranscription.parseRetryAfterMs({ header: 'soon', now: 0 })).toBeUndefined();
  });

  it('derives a store-safe checkpoint key that depends on language, chunking and connection', () => {
    const base = { bytes: Buffer.from('audio'), language: 'en', chunkSeconds: 240, mode: 'mp3-normalized', apiKey: 'sk-test' };
    const key = fonadaTranscription.checkpointKeyFor(base);
    expect(key.length).toBeLessThanOrEqual(128);
    expect(key).not.toContain('sk-test');
    expect(fonadaTranscription.checkpointKeyFor({ ...base, language: 'hi' })).not.toBe(key);
    expect(fonadaTranscription.checkpointKeyFor({ ...base, chunkSeconds: 120 })).not.toBe(key);
    expect(fonadaTranscription.checkpointKeyFor({ ...base, apiKey: 'other' })).not.toBe(key);
  });
});

function createHarness({ chunkCount, script, initial, policy, failPuts }: { chunkCount: number; script: Record<number, Step[]>; initial?: Checkpoint; policy?: Partial<TranscribeParams['policy']>; failPuts?: boolean }) {
  const store = new Map<string, unknown>(initial ? [[KEY, initial]] : []);
  const queues = new Map(Object.entries(script).map(([index, steps]) => [Number(index), [...steps]]));
  const state = { inFlight: 0 };
  const harness = {
    store,
    sent: [] as string[],
    starts: [] as number[],
    timeouts: [] as number[],
    events: [] as string[],
    logLines: [] as string[],
    maxInFlight: 0,
    params: {} as TranscribeParams,
  };
  const checkpointStore: CheckpointStore = {
    get: async (key) => store.get(key),
    put: async (key, value) => {
      if (failPuts) throw new Error('store unavailable');
      store.set(key, structuredClone(value));
    },
    delete: async (key) => store.delete(key),
  };
  harness.params = {
    chunks: Array.from({ length: chunkCount }, (_, index) => chunk(index + 1)),
    language: 'en',
    checkpointKey: KEY,
    store: checkpointStore,
    send: async ({ chunk: current, timeoutMs }) => {
      const index = Number(current.filename.slice(6, 10));
      harness.sent.push(current.filename);
      harness.starts.push(Date.now());
      harness.timeouts.push(timeoutMs);
      state.inFlight++;
      harness.maxInFlight = Math.max(harness.maxInFlight, state.inFlight);
      try {
        const step = queues.get(index)?.shift();
        if (step === undefined) throw new Error(`unexpected request for chunk ${index}`);
        if (step === 'network') throw new Error('socket hang up');
        if (step === 'hang') {
          await sleep(timeoutMs);
          throw new DOMException('The operation was aborted due to timeout', 'TimeoutError');
        }
        await sleep(step.latencyMs);
        return step.response;
      } finally {
        state.inFlight--;
      }
    },
    policy: { concurrency: 4, requestIntervalMs: 0, attemptTimeoutMs: 20_000, recoverUpstream: false, upstreamRetryWaitMs: 30_000, maxUpstreamRetries: 6, maxRateLimitRetries: 3, deadlineAt: 3_300_000, ...policy },
    clock: { now: () => Date.now(), sleep },
    log: (event, fields) => {
      harness.events.push(event);
      harness.logLines.push(`${event} ${JSON.stringify(fields)}`);
    },
  };
  return harness;
}

async function run(harness: ReturnType<typeof createHarness>): Promise<TranscribeResult> {
  const promise = fonadaTranscription.transcribeChunks(harness.params);
  promise.catch(() => undefined);
  await vi.runAllTimersAsync();
  return promise;
}

async function runError(harness: ReturnType<typeof createHarness>): Promise<string> {
  try {
    await run(harness);
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
  throw new Error('expected a failure');
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function chunk(index: number): PreparedChunk {
  return { bytes: Buffer.alloc(2_881_440, index), filename: `chunk-${String(index).padStart(4, '0')}.mp3`, audioSeconds: 240 };
}

function allOk({ count, latencyMs = REQUEST_MS }: { count: number; latencyMs?: number }): Record<number, Step[]> {
  return Object.fromEntries(Array.from({ length: count }, (_, index) => [index + 1, [ok(`t${index + 1}`, latencyMs)]]));
}

function ok(text: string, latencyMs = REQUEST_MS): Step {
  return { latencyMs, response: { status: 200, retryAfter: null, body: { text } } };
}

function rejected(status: number, retryAfter?: string, latencyMs = REQUEST_MS): Step {
  return { latencyMs, response: { status, retryAfter: retryAfter ?? null, body: undefined } };
}

function hang(): Step {
  return 'hang';
}

const KEY = 'fonadalabs:v2:test';
const REQUEST_MS = 500;

type Step = { latencyMs: number; response: SendResponse } | 'network' | 'hang';

type Checkpoint = { chunkCount: number; texts: Record<number, string> };
