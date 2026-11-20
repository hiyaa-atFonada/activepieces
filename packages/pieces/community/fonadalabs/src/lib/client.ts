import type { PreparedChunk } from './audio';
import type { Sender, SendResponse } from './transcription';

function createSender({ apiKey }: { apiKey: string }): Sender {
  return async ({ chunk, language, timeoutMs }: { chunk: PreparedChunk; language: string; timeoutMs: number }): Promise<SendResponse> => {
    const form = new FormData();
    form.append('file', new Blob([new Uint8Array(chunk.bytes)]), chunk.filename);
    form.append('language_id', language);
    const response = await fetch(TRANSCRIBE_URL, {
      method: 'POST',
      headers: { Authorization: `Bearer ${apiKey}`, Accept: 'application/json' },
      body: form,
      signal: AbortSignal.timeout(timeoutMs),
    });
    const raw = await response.text();
    return { status: response.status, retryAfter: response.headers.get('retry-after'), body: parseJson(raw) };
  };
}

function parseJson(raw: string): unknown {
  try {
    return JSON.parse(raw);
  } catch {
    return undefined;
  }
}

const TRANSCRIBE_URL = 'https://api.fonada.ai/v3/asr/transcribe';

export const fonadaClient = { createSender };
