import { performance } from 'node:perf_hooks';

async function prepareChunks({ bytes, filename, chunkSeconds, singleUploadLimitBytes = SINGLE_UPLOAD_LIMIT_BYTES }: PrepareParams): Promise<PreparedAudio> {
  const started = performance.now();
  if (bytes.length <= singleUploadLimitBytes) {
    return { mode: 'single', chunks: [{ bytes, filename }], conversionMs: 0 };
  }
  const extension = filename.toLowerCase().split('.').pop();
  if (extension === 'mp3') {
    const chunks = await splitMp3({ bytes, chunkSeconds });
    return { mode: 'mp3-normalized', chunks, conversionMs: Math.round(performance.now() - started) };
  }
  if (extension === 'wav') {
    const chunks = splitWav({ bytes, chunkSeconds });
    return { mode: 'wav-split', chunks, conversionMs: Math.round(performance.now() - started) };
  }
  throw new Error(`Recording is ${formatMb(bytes.length)}, above the ${formatMb(singleUploadLimitBytes)} single-request limit. Automatic chunking supports MP3 and WAV only; convert or split other formats before this action.`);
}

async function splitMp3({ bytes, chunkSeconds }: { bytes: Buffer; chunkSeconds: number }): Promise<PreparedChunk[]> {
  const { MPEGDecoder } = await import('mpg123-decoder');
  const encodeMp3 = (await import('@audio/encode-mp3')).default;
  const decoder = new MPEGDecoder();
  await decoder.ready;
  const samplesPerChunk = NORMALIZED_SAMPLE_RATE * chunkSeconds;
  const chunks: PreparedChunk[] = [];
  let pcm = new Float32Array(samplesPerChunk);
  let filled = 0;
  let sourceRate = 0;
  let phase = 0;
  let sum = 0;
  let count = 0;
  let last = 0;
  try {
    for (let offset = 0; offset < bytes.length; offset += DECODE_BLOCK_BYTES) {
      const decoded = decoder.decode(bytes.subarray(offset, offset + DECODE_BLOCK_BYTES));
      if (decoded.errors.length) throw new Error('MP3 decoding failed; refusing to submit incomplete audio.');
      const ready: Float32Array[] = [];
      if (decoded.samplesDecoded) {
        if (sourceRate && sourceRate !== decoded.sampleRate) throw new Error('MP3 sample rate changed within the recording.');
        sourceRate = decoded.sampleRate;
        const channels = decoded.channelData;
        for (let i = 0; i < decoded.samplesDecoded; i++) {
          let mixed = 0;
          for (const channel of channels) mixed += channel[i];
          sum += mixed / channels.length;
          count++;
          phase += NORMALIZED_SAMPLE_RATE;
          while (phase >= sourceRate) {
            const value = count ? sum / count : last;
            last = value;
            sum = 0;
            count = 0;
            phase -= sourceRate;
            pcm[filled++] = value > 1 ? 1 : value < -1 ? -1 : value;
            if (filled === samplesPerChunk) {
              ready.push(pcm);
              pcm = new Float32Array(samplesPerChunk);
              filled = 0;
            }
          }
        }
      }
      for (const full of ready) {
        chunks.push(await encodeChunk({ encodeMp3, pcm: full, index: chunks.length }));
      }
    }
    if (filled) chunks.push(await encodeChunk({ encodeMp3, pcm: pcm.subarray(0, filled), index: chunks.length }));
    if (!chunks.length) throw new Error('MP3 contains no decodable audio.');
    return chunks;
  } finally {
    decoder.free();
  }
}

async function encodeChunk({ encodeMp3, pcm, index }: { encodeMp3: Mp3EncoderFactory; pcm: Float32Array; index: number }): Promise<PreparedChunk> {
  const encoder = await encodeMp3({ sampleRate: NORMALIZED_SAMPLE_RATE, channels: 1, bitrate: NORMALIZED_BITRATE_KBPS, frames: pcm.length });
  try {
    const body = Buffer.concat([Buffer.from(encoder.encode([pcm])), Buffer.from(encoder.flush())]);
    if (!body.length) throw new Error('MP3 encoder returned no audio bytes.');
    await new Promise((resolve) => setImmediate(resolve));
    return { bytes: body, filename: chunkFilename({ index, extension: 'mp3' }), audioSeconds: pcm.length / NORMALIZED_SAMPLE_RATE };
  } finally {
    encoder.free();
  }
}

function splitWav({ bytes, chunkSeconds }: { bytes: Buffer; chunkSeconds: number }): PreparedChunk[] {
  if (bytes.toString('ascii', 0, 4) !== 'RIFF' || bytes.toString('ascii', 8, 12) !== 'WAVE') {
    throw new Error('Oversized WAV must use the standard RIFF/WAVE format.');
  }
  let offset = 12;
  let formatChunk: Buffer | undefined;
  let audioStart = -1;
  let audioLength = 0;
  while (offset + 8 <= bytes.length) {
    const id = bytes.toString('ascii', offset, offset + 4);
    const size = bytes.readUInt32LE(offset + 4);
    const start = offset + 8;
    if (id === 'data') {
      audioStart = start;
      audioLength = Math.min(size, bytes.length - start);
      break;
    }
    if (start + size > bytes.length) throw new Error('WAV contains an incomplete chunk.');
    if (id === 'fmt ') formatChunk = bytes.subarray(offset, start + size + (size % 2));
    offset = start + size + (size % 2);
  }
  if (!formatChunk || audioStart < 0) throw new Error('WAV is missing its format or audio data chunk.');
  const byteRate = formatChunk.readUInt32LE(16);
  const blockAlign = formatChunk.readUInt16LE(20);
  if (blockAlign < 1 || byteRate < 1) throw new Error('WAV has an invalid sample frame size.');
  const targetBytes = Math.min(byteRate * chunkSeconds, WAV_MAX_CHUNK_BYTES);
  const bytesPerChunk = Math.max(blockAlign, Math.floor(targetBytes / blockAlign) * blockAlign);
  const chunks: PreparedChunk[] = [];
  for (let start = 0; start < audioLength; start += bytesPerChunk) {
    const audio = bytes.subarray(audioStart + start, audioStart + Math.min(start + bytesPerChunk, audioLength));
    const dataHeader = Buffer.alloc(8);
    dataHeader.write('data', 0, 'ascii');
    dataHeader.writeUInt32LE(audio.length, 4);
    const body = Buffer.concat([formatChunk, dataHeader, audio]);
    const header = Buffer.alloc(12);
    header.write('RIFF', 0, 'ascii');
    header.writeUInt32LE(body.length + 4, 4);
    header.write('WAVE', 8, 'ascii');
    chunks.push({ bytes: Buffer.concat([header, body]), filename: chunkFilename({ index: chunks.length, extension: 'wav' }), audioSeconds: audio.length / byteRate });
  }
  return chunks;
}

function chunkFilename({ index, extension }: { index: number; extension: string }): string {
  return `chunk-${String(index + 1).padStart(4, '0')}.${extension}`;
}

function formatMb(bytes: number): string {
  return `${(bytes / 1_000_000).toFixed(1)} MB`;
}

const NORMALIZED_SAMPLE_RATE = 16_000;
const NORMALIZED_BITRATE_KBPS = 96;
const DECODE_BLOCK_BYTES = 65_536;
const WAV_MAX_CHUNK_BYTES = 20_000_000;

export const PROVIDER_MAX_REQUEST_BYTES = 25_000_000;
export const SINGLE_UPLOAD_LIMIT_BYTES = 24_000_000;

export const fonadaAudio = { prepareChunks, formatMb };

type Mp3EncoderFactory = typeof import('@audio/encode-mp3').default;

type PrepareParams = {
  bytes: Buffer;
  filename: string;
  chunkSeconds: number;
  singleUploadLimitBytes?: number;
};

export type PreparedChunk = {
  bytes: Buffer;
  filename: string;
  audioSeconds?: number;
};

export type PreparedAudio = {
  mode: 'single' | 'mp3-normalized' | 'wav-split';
  chunks: PreparedChunk[];
  conversionMs: number;
};
