import { createAction, createPiece, OutputSchema, PieceAuth, PieceCategory, Property, Store, StoreScope } from '@activepieces/pieces-framework';
import { performance } from 'node:perf_hooks';
import { fonadaAudio } from './lib/audio';
import { fonadaClient } from './lib/client';
import { CheckpointStore, fonadaTranscription } from './lib/transcription';

const DEFAULT_CHUNK_SECONDS = 240;
const MIN_CHUNK_SECONDS = 30;
const MAX_CHUNK_SECONDS = 600;
const DEFAULT_CONCURRENCY = 4;
const MAX_CONCURRENCY = 4;
const DEFAULT_REQUEST_INTERVAL_SECONDS = 2;
const DEFAULT_ATTEMPT_TIMEOUT_SECONDS = 20;
const DEFAULT_UPSTREAM_RETRY_WAIT_SECONDS = 30;
const MAX_UPSTREAM_RETRIES = 6;
const DEFAULT_TIME_BUDGET_SECONDS = 540;
const MAX_RATE_LIMIT_RETRIES = 3;
const DOWNLOAD_TIMEOUT_MS = 600_000;

const outputSchema: OutputSchema = {
  fields: [
    { key: 'text', label: 'Transcript' },
    { key: 'chunks', label: 'Chunks', format: 'number' },
    { key: 'reusedChunks', label: 'Chunks Reused From Checkpoint', format: 'number' },
    { key: 'language_id', label: 'Language ID' },
    { key: 'model', label: 'Model' },
    {
      key: 'metrics',
      label: 'Run Metrics',
      children: [
        { key: 'sourceBytes', label: 'Source Bytes', format: 'number' },
        { key: 'mode', label: 'Chunking Mode' },
        { key: 'chunkCount', label: 'Chunk Count', format: 'number' },
        { key: 'requestAttempts', label: 'Request Attempts', format: 'number' },
        { key: 'conversionMs', label: 'Conversion (ms)', format: 'number' },
        { key: 'asrMs', label: 'Transcription Time (ms)', format: 'number' },
        { key: 'retryWaitMs', label: 'Retry Wait (ms)', format: 'number' },
        { key: 'totalMs', label: 'Total (ms)', format: 'number' },
      ],
    },
  ],
};

const fonadalabsAuth = PieceAuth.SecretText({
  displayName: 'FonadaLabs API Key',
  required: true,
  description: 'Bearer token from FonadaLabs.',
});

const transcribeAudio = createAction({
  displayName: 'Transcribe Audio',
  name: 'transcribe_audio',
  description: 'Transcribe audio with FonadaLabs Kapture V3 (cloud). Recordings above 24 MB (MP3 or WAV) are split into chunks, transcribed with bounded concurrency, and joined in the original order.',
  auth: fonadalabsAuth,
  outputSchema,
  props: {
    file: Property.File({ displayName: 'Audio or Video File', required: false }),
    audioUrl: Property.ShortText({
      displayName: 'Audio URL',
      description: 'Public or locally reachable URL for the audio/video file. Use this when no file is mapped from a previous step.',
      required: false,
    }),
    language: Property.ShortText({
      displayName: 'Language ID',
      description: 'FonadaLabs language identifier. Defaults to en.',
      required: false,
      defaultValue: 'en',
    }),
    chunkSeconds: Property.Number({
      displayName: 'Long audio chunk duration (seconds)',
      description: `For recordings above 24 MB. Default ${DEFAULT_CHUNK_SECONDS} s (about 2.9 MB per MP3 request at 16 kHz mono 96 kbps). Accepts ${MIN_CHUNK_SECONDS}–${MAX_CHUNK_SECONDS}. Longer chunks mean fewer requests, so fewer rate-limit and upstream waits.`,
      required: false,
      defaultValue: DEFAULT_CHUNK_SECONDS,
    }),
    concurrency: Property.Number({
      displayName: 'Concurrent requests',
      description: `Chunks transcribed in parallel (1–${MAX_CONCURRENCY}). Default ${DEFAULT_CONCURRENCY}. The transcript is always joined in the original order. Lower it if FonadaLabs returns more 429/502 errors.`,
      required: false,
      defaultValue: DEFAULT_CONCURRENCY,
    }),
    requestIntervalSeconds: Property.Number({
      displayName: 'Minimum seconds between request starts',
      description: `Shared pacing across all concurrent requests (start to start). Default ${DEFAULT_REQUEST_INTERVAL_SECONDS} s. Lower values risk HTTP 429 rate limits.`,
      required: false,
      defaultValue: DEFAULT_REQUEST_INTERVAL_SECONDS,
    }),
    attemptTimeoutSeconds: Property.Number({
      displayName: 'Attempt timeout (seconds)',
      description: `Abandon a request with no response after this many seconds plus 4 s per MB uploaded (default ${DEFAULT_ATTEMPT_TIMEOUT_SECONDS} s, about 32 s for a 240 s chunk). Successful chunks normally answer in about 1 s; FonadaLabs gateway failures otherwise hang for 60 s before HTTP 502. Retried only when upstream retries are enabled.`,
      required: false,
      defaultValue: DEFAULT_ATTEMPT_TIMEOUT_SECONDS,
    }),
    recoverUpstream: Property.Checkbox({
      displayName: 'Retry upstream failures',
      description: `Retry HTTP 502/503/504 and timed-out attempts up to ${MAX_UPSTREAM_RETRIES} times per chunk. Failed attempts may already be billed; enabling this can incur duplicate charges.`,
      required: false,
      defaultValue: false,
    }),
    upstreamRetryWaitSeconds: Property.Number({
      displayName: 'Upstream retry wait (seconds)',
      description: `First wait after an upstream failure (doubles per retry up to 120 s, ±20% jitter, shared by all concurrent requests). Default ${DEFAULT_UPSTREAM_RETRY_WAIT_SECONDS} s.`,
      required: false,
      defaultValue: DEFAULT_UPSTREAM_RETRY_WAIT_SECONDS,
    }),
    timeBudgetSeconds: Property.Number({
      displayName: 'Action time budget (seconds)',
      description: `Stop cleanly, keeping checkpoints, before this many seconds. Default ${DEFAULT_TIME_BUDGET_SECONDS} s, safe for the stock 600 s host flow timeout. For long recordings set the host AP_FLOW_TIMEOUT_SECONDS=3600 and this to about 3300.`,
      required: false,
      defaultValue: DEFAULT_TIME_BUDGET_SECONDS,
    }),
  },
  async run(context) {
    const startedAt = Date.now();
    const started = performance.now();
    const props = context.propsValue;
    const file = props.file;
    const audioUrl = props.audioUrl?.trim();
    if (!file && !audioUrl) throw new Error('Provide an audio file or Audio URL.');
    const language = props.language?.trim() || 'en';
    const chunkSeconds = clampInteger({ value: props.chunkSeconds, fallback: DEFAULT_CHUNK_SECONDS, min: MIN_CHUNK_SECONDS, max: MAX_CHUNK_SECONDS });
    const requestIntervalSeconds = clampInteger({ value: props.requestIntervalSeconds, fallback: DEFAULT_REQUEST_INTERVAL_SECONDS, min: 0, max: 120 });
    const upstreamRetryWaitSeconds = clampInteger({ value: props.upstreamRetryWaitSeconds, fallback: DEFAULT_UPSTREAM_RETRY_WAIT_SECONDS, min: 5, max: 300 });
    const concurrency = clampInteger({ value: props.concurrency, fallback: DEFAULT_CONCURRENCY, min: 1, max: MAX_CONCURRENCY });
    const attemptTimeoutSeconds = clampInteger({ value: props.attemptTimeoutSeconds, fallback: DEFAULT_ATTEMPT_TIMEOUT_SECONDS, min: 5, max: 600 });
    const timeBudgetSeconds = clampInteger({ value: props.timeBudgetSeconds, fallback: DEFAULT_TIME_BUDGET_SECONDS, min: 60, max: 86_400 });
    const recoverUpstream = props.recoverUpstream === true;
    const log = createLogger();

    const source = file
      ? { bytes: Buffer.from(file.data), filename: file.filename || 'audio.wav', downloadMs: 0 }
      : await downloadAudio({ audioUrl: audioUrl ?? '' });
    log('source_ready', { input: file ? 'file' : 'url', bytes: source.bytes.length, extension: source.filename.split('.').pop()?.toLowerCase() ?? '', downloadMs: source.downloadMs });

    const prepared = await fonadaAudio.prepareChunks({ bytes: source.bytes, filename: source.filename, chunkSeconds });
    const sizes = prepared.chunks.map((chunk) => chunk.bytes.length);
    log('chunks_prepared', { mode: prepared.mode, chunkCount: prepared.chunks.length, chunkSeconds, conversionMs: prepared.conversionMs, minChunkBytes: Math.min(...sizes), maxChunkBytes: Math.max(...sizes), totalChunkBytes: sizes.reduce((sum, size) => sum + size, 0) });

    const result = await fonadaTranscription.transcribeChunks({
      chunks: prepared.chunks,
      language,
      checkpointKey: fonadaTranscription.checkpointKeyFor({ bytes: source.bytes, language, chunkSeconds, mode: prepared.mode, apiKey: context.auth.secret_text }),
      store: projectStore(context.store),
      send: fonadaClient.createSender({ apiKey: context.auth.secret_text }),
      policy: {
        concurrency,
        requestIntervalMs: requestIntervalSeconds * 1000,
        attemptTimeoutMs: attemptTimeoutSeconds * 1000,
        recoverUpstream,
        upstreamRetryWaitMs: upstreamRetryWaitSeconds * 1000,
        maxUpstreamRetries: MAX_UPSTREAM_RETRIES,
        maxRateLimitRetries: MAX_RATE_LIMIT_RETRIES,
        deadlineAt: startedAt + timeBudgetSeconds * 1000,
      },
      clock: { now: () => Date.now(), sleep: (ms: number) => new Promise((resolve) => setTimeout(resolve, ms)) },
      log,
    });

    const metrics = {
      sourceBytes: source.bytes.length,
      downloadMs: source.downloadMs,
      mode: prepared.mode,
      chunkSeconds,
      conversionMs: prepared.conversionMs,
      chunkCount: result.chunks,
      maxChunkBytes: Math.max(...sizes),
      reusedChunks: result.reusedChunks,
      transcribedChunks: result.stats.transcribedChunks,
      retriedChunks: result.stats.retriedChunks,
      requestAttempts: result.stats.attempts,
      rateLimitRetries: result.stats.rateLimitRetries,
      upstreamRetries: result.stats.upstreamRetries,
      timeoutRetries: result.stats.timeoutRetries,
      concurrency: result.stats.concurrency,
      asrMs: result.stats.asrMs,
      maxAsrMs: result.stats.maxAsrMs,
      pacingWaitMs: result.stats.pacingWaitMs,
      retryWaitMs: result.stats.retryWaitMs,
      totalMs: Math.round(performance.now() - started),
    };
    log('run_completed', metrics);
    return { text: result.text, chunks: result.chunks, reusedChunks: result.reusedChunks, language_id: language, model: 'kapture-v3', metrics };
  },
});

async function downloadAudio({ audioUrl }: { audioUrl: string }): Promise<{ bytes: Buffer; filename: string; downloadMs: number }> {
  const parsed = parseUrl(audioUrl);
  const started = performance.now();
  const response = await fetch(parsed, { signal: AbortSignal.timeout(DOWNLOAD_TIMEOUT_MS) });
  if (!response.ok) throw new Error(`Could not download Audio URL (HTTP ${response.status}).`);
  const bytes = Buffer.from(await response.arrayBuffer());
  if (!bytes.length) throw new Error('Audio URL returned an empty file.');
  const filename = decodeURIComponent(parsed.pathname.split('/').pop() || '') || 'audio.wav';
  return { bytes, filename, downloadMs: Math.round(performance.now() - started) };
}

function parseUrl(audioUrl: string): URL {
  try {
    return new URL(audioUrl);
  } catch {
    throw new Error('Audio URL is not a valid absolute URL.');
  }
}

function projectStore(store: Store): CheckpointStore {
  return {
    get: (key: string) => store.get<unknown>(key, StoreScope.PROJECT),
    put: (key: string, value: unknown) => store.put(key, value, StoreScope.PROJECT),
    delete: (key: string) => store.delete(key, StoreScope.PROJECT),
  };
}

function createLogger(): (event: string, fields: Record<string, unknown>) => void {
  return (event, fields) => {
    console.log(`[fonadalabs] ${event} ${JSON.stringify(fields)}`);
  };
}

function clampInteger({ value, fallback, min, max }: { value: number | undefined | null; fallback: number; min: number; max: number }): number {
  const number = typeof value === 'number' && Number.isFinite(value) ? Math.floor(value) : fallback;
  return Math.max(min, Math.min(max, number));
}

export const fonadalabsPiece = createPiece({
  displayName: 'FonadaLabs',
  logoUrl: 'https://fonada.ai/favicon.ico',
  description: 'FonadaLabs speech-to-text using Kapture V3.',
  categories: [PieceCategory.ARTIFICIAL_INTELLIGENCE],
  minimumSupportedRelease: '0.30.0',
  authors: ['hiyaa-atFonada'],
  auth: fonadalabsAuth,
  actions: [transcribeAudio],
  triggers: [],
});
