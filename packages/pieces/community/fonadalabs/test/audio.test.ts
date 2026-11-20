import { describe, expect, it } from 'vitest';
import { fonadaAudio } from '../src/lib/audio';

describe('fonadaAudio.prepareChunks', () => {
  it('passes recordings under the single-request limit through unchanged', async () => {
    const bytes = Buffer.from('small');
    const prepared = await fonadaAudio.prepareChunks({ bytes, filename: 'clip.m4a', chunkSeconds: 240 });
    expect(prepared.mode).toBe('single');
    expect(prepared.chunks).toEqual([{ bytes, filename: 'clip.m4a' }]);
  });

  it('rejects oversized formats it cannot split', async () => {
    await expect(fonadaAudio.prepareChunks({ bytes: Buffer.alloc(10), filename: 'clip.m4a', chunkSeconds: 240, singleUploadLimitBytes: 1 })).rejects.toThrow('MP3 and WAV only');
  });

  it('splits WAV by duration into independent, ordered RIFF files', async () => {
    const wav = makeWav({ seconds: 10, sampleRate: 16_000 });
    const prepared = await fonadaAudio.prepareChunks({ bytes: wav, filename: 'call.wav', chunkSeconds: 3, singleUploadLimitBytes: 1 });
    expect(prepared.chunks.map((chunk) => chunk.filename)).toEqual(['chunk-0001.wav', 'chunk-0002.wav', 'chunk-0003.wav', 'chunk-0004.wav']);
    expect(prepared.chunks.map((chunk) => chunk.audioSeconds)).toEqual([3, 3, 3, 1]);
    for (const chunk of prepared.chunks) {
      expect(chunk.bytes.toString('ascii', 0, 4)).toBe('RIFF');
      expect(chunk.bytes.readUInt32LE(4)).toBe(chunk.bytes.length - 8);
    }
  });

  it('normalizes an oversized MP3 into decodable 16 kHz mono chunks of the requested duration', async () => {
    const mp3 = await makeMp3({ seconds: 7, sampleRate: 44_100 });
    const prepared = await fonadaAudio.prepareChunks({ bytes: mp3, filename: 'talk.mp3', chunkSeconds: 3, singleUploadLimitBytes: 1 });
    expect(prepared.mode).toBe('mp3-normalized');
    expect(prepared.chunks.map((chunk) => chunk.filename)).toEqual(['chunk-0001.mp3', 'chunk-0002.mp3', 'chunk-0003.mp3']);
    const { MPEGDecoder } = await import('mpg123-decoder');
    for (const chunk of prepared.chunks) {
      const decoder = new MPEGDecoder();
      await decoder.ready;
      const decoded = decoder.decode(chunk.bytes);
      decoder.free();
      expect(decoded.errors).toHaveLength(0);
      expect(decoded.sampleRate).toBe(16_000);
      expect(mp3ChannelMode(chunk.bytes)).toBe('mono');
      expect(Math.abs(decoded.samplesDecoded / 16_000 - (chunk.audioSeconds ?? 0))).toBeLessThan(0.2);
    }
    const total = prepared.chunks.reduce((sum, chunk) => sum + (chunk.audioSeconds ?? 0), 0);
    expect(Math.abs(total - 7)).toBeLessThan(0.1);
  });
});

function mp3ChannelMode(bytes: Buffer): string {
  for (let i = 0; i + 3 < bytes.length; i++) {
    if (bytes[i] === 0xff && (bytes[i + 1] & 0xe0) === 0xe0) return (bytes[i + 3] >> 6) === 3 ? 'mono' : 'stereo';
  }
  return 'none';
}

function makeWav({ seconds, sampleRate }: { seconds: number; sampleRate: number }): Buffer {
  const samples = seconds * sampleRate;
  const data = Buffer.alloc(samples * 2);
  for (let i = 0; i < samples; i++) data.writeInt16LE(Math.round(Math.sin(i / 10) * 8000), i * 2);
  const fmt = Buffer.alloc(24);
  fmt.write('fmt ', 0, 'ascii');
  fmt.writeUInt32LE(16, 4);
  fmt.writeUInt16LE(1, 8);
  fmt.writeUInt16LE(1, 10);
  fmt.writeUInt32LE(sampleRate, 12);
  fmt.writeUInt32LE(sampleRate * 2, 16);
  fmt.writeUInt16LE(2, 20);
  fmt.writeUInt16LE(16, 22);
  const dataHeader = Buffer.alloc(8);
  dataHeader.write('data', 0, 'ascii');
  dataHeader.writeUInt32LE(data.length, 4);
  const header = Buffer.alloc(12);
  header.write('RIFF', 0, 'ascii');
  header.writeUInt32LE(4 + fmt.length + dataHeader.length + data.length, 4);
  header.write('WAVE', 8, 'ascii');
  return Buffer.concat([header, fmt, dataHeader, data]);
}

async function makeMp3({ seconds, sampleRate }: { seconds: number; sampleRate: number }): Promise<Buffer> {
  const encodeMp3 = (await import('@audio/encode-mp3')).default;
  const frames = seconds * sampleRate;
  const left = new Float32Array(frames);
  const right = new Float32Array(frames);
  for (let i = 0; i < frames; i++) {
    left[i] = Math.sin((2 * Math.PI * 440 * i) / sampleRate) * 0.3;
    right[i] = Math.sin((2 * Math.PI * 660 * i) / sampleRate) * 0.3;
  }
  const encoder = await encodeMp3({ sampleRate, channels: 2, bitrate: 128, frames });
  try {
    return Buffer.concat([Buffer.from(encoder.encode([left, right])), Buffer.from(encoder.flush())]);
  } finally {
    encoder.free();
  }
}
