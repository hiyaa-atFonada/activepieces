# FonadaLabs Activepieces piece

Transcribes customer-provided audio with FonadaLabs Kapture V3 (`POST https://api.fonada.ai/v3/asr/transcribe`, multipart fields `file` and `language_id`, Bearer API key).

## Cloud-processing disclosure

Audio is uploaded to, and transcribed in, the FonadaLabs cloud. Do not send recordings that must not leave your infrastructure. Each upload is a billable FonadaLabs request; a request that fails after reaching FonadaLabs may still be billed.

## Configuration

1. Create a **FonadaLabs** connection with the customer's API key.
2. In **Transcribe Audio**, map either **Audio or Video File** from an earlier step or a direct, downloadable **Audio URL**.
3. Set **Language ID** to a FonadaLabs-supported value such as `en` or `hi` (default `en`).

| Input | Default | Meaning |
| --- | --- | --- |
| Long audio chunk duration (seconds) | 240 | Chunk length for recordings above 24 MB (30–600). |
| Concurrent requests | 4 | Chunks in flight at once (1–4). Output order never changes. |
| Minimum seconds between request starts | 2 | Start-to-start pacing, shared by all concurrent requests (0–120). |
| Attempt timeout (seconds) | 20 | Abandon an attempt with no answer after this time plus 4 s per MB (≈32 s for a 240 s chunk). |
| Retry upstream failures | off | Retry HTTP 502/503/504 and timed-out attempts up to 6 times per chunk. May incur duplicate charges. |
| Upstream retry wait (seconds) | 30 | First wait after an upstream failure. It doubles per retry up to 120 s, with ±20% jitter, and pauses all workers. |
| Action time budget (seconds) | 540 | Stop cleanly, with checkpoints kept, before this time. The default is safe for the stock 600 s host timeout. For long recordings raise it to about 3300 together with `AP_FLOW_TIMEOUT_SECONDS=3600`. |

## Required Activepieces host setting for long recordings

Self-hosted Activepieces kills any flow run after `AP_FLOW_TIMEOUT_SECONDS` (default **600**). A long recording can exceed that. Set the following on the host and restart Activepieces:

```bash
AP_FLOW_TIMEOUT_SECONDS=3600
```

The API reads this value and passes it to workers in their settings, so one value covers both. Check it with `curl -s http://localhost:3017/api/v1/flags | grep -o '"FLOW_RUN_TIME_SECONDS":[0-9]*'`. Set **Action time budget** about 300 seconds below it (3300 for 3600). The piece cannot read the host limit, so the budget is a separate setting: with the default 540 and a stock host, a slow run stops with a clear message and its checkpoints instead of a generic `TIMEOUT`, and with 3300 on a stock host it would be killed at 600 s. Activepieces templates cannot carry this host setting, so state it in any setup guide.

## Supported audio and the 25 MB limit

FonadaLabs accepts at most **25 MB per request**.

- **24 MB or smaller, any format FonadaLabs supports:** uploaded unchanged as one request.
- **MP3 above 24 MB:** decoded continuously with the bundled `mpg123-decoder` (WebAssembly). The audio is mixed to mono, low-pass averaged and resampled to 16 kHz, then encoded as independent 96 kbps MP3 chunks. A 240-second chunk is about 2.9 MB. The MP3 is never split by raw byte ranges, because the MP3 bit reservoir makes byte slices undecodable. No system FFmpeg is needed.
- **WAV above 24 MB (standard RIFF/WAVE):** split on sample-frame boundaries into chunks of the configured duration, capped at 20 MB each.
- **Other formats above 24 MB, including video:** rejected. Convert or split them before this action.

## Chunking, ordering and checkpoints

Up to **Concurrent requests** chunks are transcribed at once. Each result is stored at its chunk position, and the transcript is joined with newlines in the original order, whatever order the responses arrive in. If one chunk fails, no new chunks are started. Requests already in flight finish and are checkpointed. The output contains plain text only. Timestamps, confidence scores and speaker labels are never added.

After each successful chunk, the transcripts completed so far are written to the Activepieces project store. If a run fails or times out, those checkpoints stay. Re-running the **same recording, language, chunk duration and connection** reuses them in order and uploads only the missing chunks. Checkpoints are deleted when a complete transcript is returned. Changing the chunk duration starts a new checkpoint.

A failed run names the failing chunk, the HTTP status (or "no HTTP response"), whether and why it retried, and how many chunk transcripts are checkpointed. For example:

```
FonadaLabs failed on chunk 7 of 12 (HTTP 502; retried: no (enable "Retry upstream failures" to retry HTTP 502)): FonadaLabs ASR upstream unavailable. 6 of 12 chunk transcripts are checkpointed (0 reused from an earlier run, 6 saved in this run). Re-run the same recording ...
```

Retry rules:

- **HTTP 429:** retried up to 3 times per chunk. Waits for `Retry-After` (seconds or HTTP date), and at least 60 s.
- **HTTP 502/503/504 and attempts that exceed the attempt timeout:** retried only when **Retry upstream failures** is on.
- **Network errors and other statuses:** not retried, because the request may already have been processed and billed.
- **Cooldown:** after a 429 or an upstream failure, no concurrent worker starts a new request until the cooldown ends.
- **Whole-flow retries:** leave Activepieces "Retry on failure" off for this step. Re-run the flow manually instead, so checkpoints are reused.

## Speed and reliability trade-offs

The output `metrics` object, and the `[fonadalabs]` lines in the worker log, show where time goes: `downloadMs`, `conversionMs`, `chunkCount`, `maxChunkBytes`, `asrMs`, `pacingWaitMs`, `retryWaitMs`, `rateLimitRetries`, `upstreamRetries` and `totalMs`.

Measured on a 47-minute, 38 MB MP3, before this change (120-second chunks): conversion took about 15 s and each successful ASR request under 1 s. About 23 of the 25.8 minutes were provider-failure waits plus fixed pacing. Total time is therefore driven by the **number of requests** and the **provider failure rate**, not by conversion.

- **Longer chunks** mean fewer requests, fewer waits and less pacing. 240 s halves the requests compared with 120 s and stays about 9× below the 25 MB limit. If a provider failure occurs, more audio has to be retried.
- **Shorter pacing** saves `interval × (chunks − 1)` but risks HTTP 429.
- **Observed provider behaviour (2026-11-20, sequential, 240 s chunks):** successful requests answered in 0.7–1.0 s. Failed requests hung for about 60.5 s and then returned HTTP 502. The first request after each success usually failed. Chunks completed about every 7 minutes, so the provider's failure behaviour, not the integration, set the speed.
- **Attempt timeout** cuts the 60 s gateway hang to about 32 s per failed attempt. The abandoned attempt may still be billed.
- **Concurrency (default 4)** helps only when FonadaLabs accepts parallel requests. If 502s rise with concurrency, set it to 1–2. The shared cooldown stops every worker from retrying into a failing gateway at once.
- **Upstream retries** turn intermittent 502s into completed runs, but each retried request may be billed.

## Testing

Unit tests make no network calls:

```bash
cd packages/pieces/community/fonadalabs
npm run test     # vitest: ordering, checkpoints, 429/502 policy, time budget, MP3/WAV chunking
npm run lint
npm run build
```

End-to-end with the local stack (paid FonadaLabs requests):

```bash
cd /path/to/Audios && python3 -m http.server 8765 --bind 0.0.0.0
curl -sS -X POST http://localhost:4200/api/v1/webhooks/<flowId> \
  -H 'Content-Type: application/json' \
  -d '{"audio_url":"http://<host-ip>:8765/<file>.mp3","language":"en","title":"Transcript title"}'
```

Follow the worker output for `[fonadalabs]` lines (`source_ready`, `chunks_prepared`, `chunk_attempt`, `chunk_retry_wait`, `run_completed` or `run_failed`). Logs never include the API key, the audio URL or transcript text.

## Example workflows

Import any of these from `examples/` with **Import flow**, then connect FonadaLabs (and the other apps) and publish. Each one starts with a webhook that receives JSON; `audio_url` must be reachable from the Activepieces worker.

| File | Flow | Webhook fields |
| --- | --- | --- |
| `audio-to-google-docs.json` | Transcript → Google Docs | `audio_url`, `language`, `title` |
| `audio-to-store.json` | Transcript → Store | `audio_url`, `language` |
| `audio-to-crm-hubspot.json` | Transcript → AI call note → HubSpot note on the contact | `audio_url`, `language`, `contact_email` |
| `audio-to-sop-google-docs.json` | Transcript → AI Standard Operating Procedure → Google Docs | `audio_url`, `language`, `title` |

Notes for the two AI flows:

- Choose an **AI model** in the **Ask AI** step. It is project-specific, so it cannot be shipped in the file. Until then the step is marked invalid.
- The prompts tell the model to use only what the transcript says and to write "Not stated" instead of guessing. Models can still make mistakes, so have a person review a note or SOP before relying on it.
- The transcript is sent to the AI provider you choose, in addition to FonadaLabs.
- The HubSpot flow needs an existing contact with that email. If none is found, **Attach call note** fails. The transcript checkpoint is deleted once transcription succeeds, so re-triggering the flow transcribes the recording again and FonadaLabs bills it again. Create the contact first.
- HubSpot notes have a size limit, so the flow attaches the AI summary, not the full transcript.
- Set the host `AP_FLOW_TIMEOUT_SECONDS` and the FonadaLabs **Action time budget** for long recordings (see above). The AI step runs after transcription, so leave a few minutes of headroom.

Flow structure and import logic were validated against the repository's template schema. The AI and HubSpot steps have not been run end to end.

## Template files

`templates/*.json` are the same four flows wrapped as `POST /v1/templates` request bodies (`name`, `summary`, `description`, `author`, `categories`, `tags`, `type`, `flows`). They are set to `"type": "CUSTOM"`; change it to `"SHARED"` for a shareable template. `OFFICIAL` templates cannot be created through the API of a self-hosted instance. The flows reference `@activepieces/piece-fonadalabs`, so the piece must be installed wherever the template is used. If you publish the piece under your own npm scope, change `pieceName` in the files to match.
