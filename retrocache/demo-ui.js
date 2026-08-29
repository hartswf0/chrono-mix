/**
 * demo-ui.js — shared helpers for the retrocache sensor demo pages.
 * No dependencies; pairs with demo.css.
 */

/**
 * Monotonic microsecond timestamper. Browser event timestamps can repeat
 * (coarsened clocks), which the engine rejects; this nudges duplicates
 * forward by 1 µs.
 */
export function makeTimestamper() {
  let last = -Infinity;
  return () => {
    let t = performance.now() * 1000;
    if (t <= last) t = last + 1;
    last = t;
    return t;
  };
}

/**
 * Keep a state badge + stats line refreshed from an engine that may be
 * created lazily (after a permission grant).
 * @param {() => import('./retrocache.js').RetroCaptureEngine|null} getEngine
 * @param {HTMLElement} badgeEl
 * @param {HTMLElement} statsEl
 * @param {() => string} [extra] appended to the stats line
 */
export function wireStatus(getEngine, badgeEl, statsEl, extra) {
  function tick() {
    const engine = getEngine();
    if (engine) {
      const s = engine.stats;
      badgeEl.textContent = engine.state;
      badgeEl.className = 'state ' + engine.state;
      statsEl.textContent =
        `cache ${s.cacheDepth}/${engine.cacheCapacity} · queue ${s.liveQueueDepth} · ` +
        `persisted ${s.persistedCache + s.persistedLive} (${s.persistedCache} cache / ${s.persistedLive} live) · ` +
        `dropped ${s.dropped} · lookback ${engine.effectivePrerollSeconds().toFixed(2)}s` +
        (extra ? ' · ' + extra() : '');
    }
    requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
}

/** Decode a persisted frame's bytes as float32 channel values. */
export function frameFloats(frame) {
  return new Float32Array(frame.bytes.buffer, frame.bytes.byteOffset, frame.bytes.length >> 2);
}

/**
 * Stacked-lane line plot of persisted multi-channel frames.
 * Each lane is normalized to its own min/max. Cache portion draws teal,
 * live portion pink; drop gaps get a red tick at the lane top.
 *
 * @param {HTMLCanvasElement} canvas
 * @param {Array} frames MemorySink frames (chronological)
 * @param {Array<{label: string, index: number}>} lanes channel picks
 */
export function plotFrames(canvas, frames, lanes) {
  const ctx = canvas.getContext('2d');
  const W = canvas.width;
  const laneH = 56;
  canvas.height = Math.max(120, lanes.length * laneH);
  ctx.fillStyle = '#000';
  ctx.fillRect(0, 0, W, canvas.height);
  if (frames.length < 2) return;

  const t0 = frames[0].meta.timestampUs;
  const t1 = frames[frames.length - 1].meta.timestampUs;
  const span = Math.max(1, t1 - t0);
  const xOf = (f) => ((f.meta.timestampUs - t0) / span) * (W - 8) + 4;
  const decoded = frames.map(frameFloats);
  const boundary = frames.findIndex((f) => f.meta.origin === 'live');

  lanes.forEach((lane, li) => {
    const y0 = li * laneH;
    let lo = Infinity, hi = -Infinity;
    for (const d of decoded) {
      const v = d[lane.index];
      if (Number.isFinite(v)) { if (v < lo) lo = v; if (v > hi) hi = v; }
    }
    if (!(hi > lo)) { lo -= 1; hi += 1; }
    const pad = (hi - lo) * 0.12;
    lo -= pad; hi += pad;
    const yOf = (v) => y0 + laneH - 8 - ((v - lo) / (hi - lo)) * (laneH - 16);

    ctx.strokeStyle = '#1f2533';
    ctx.beginPath(); ctx.moveTo(0, y0 + laneH - 0.5); ctx.lineTo(W, y0 + laneH - 0.5); ctx.stroke();

    ctx.lineWidth = 1.5;
    for (let i = 1; i < frames.length; i++) {
      ctx.strokeStyle = frames[i].meta.origin === 'live' ? '#fb6597' : '#65fbd2';
      ctx.beginPath();
      ctx.moveTo(xOf(frames[i - 1]), yOf(decoded[i - 1][lane.index]));
      ctx.lineTo(xOf(frames[i]), yOf(decoded[i][lane.index]));
      ctx.stroke();
      if (frames[i].meta.gapBefore > 0) {
        ctx.fillStyle = '#ff3b3b';
        ctx.fillRect(xOf(frames[i]) - 1, y0 + 2, 2, 8);
      }
    }
    ctx.fillStyle = '#9aa0a6';
    ctx.font = '10px ui-monospace,monospace';
    ctx.fillText(`${lane.label}  [${fmt(lo + pad)} … ${fmt(hi - pad)}]`, 6, y0 + 12);
  });

  if (boundary > 0) {
    const x = xOf(frames[boundary]);
    ctx.strokeStyle = '#fb6597';
    ctx.setLineDash([4, 4]);
    ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, canvas.height); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = '#fb6597';
    ctx.fillText('T', Math.min(x + 4, W - 12), 12);
  }
}

function fmt(v) {
  const a = Math.abs(v);
  if (a >= 1000) return v.toFixed(0);
  if (a >= 1) return v.toFixed(2);
  return v.toPrecision(3);
}

/** Serialize persisted frames to CSV (float32 channel payloads). */
export function framesToCSV(frames, channelLabels) {
  const head = ['frame_id', 'timestamp_us', 'origin', 'gap_before', ...channelLabels];
  const rows = frames.map((f) => {
    const d = frameFloats(f);
    return [
      f.meta.frameId, f.meta.timestampUs.toFixed(0), f.meta.origin, f.meta.gapBefore,
      ...channelLabels.map((_, i) => d[i]),
    ].join(',');
  });
  return head.join(',') + '\n' + rows.join('\n') + '\n';
}

/** Offer a blob as a file download. */
export function download(name, blob) {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 10_000);
}

/** Encode mono float32 samples as a 16-bit PCM WAV blob. */
export function encodeWav(samples, sampleRate) {
  const buf = new ArrayBuffer(44 + samples.length * 2);
  const dv = new DataView(buf);
  const str = (off, s) => { for (let i = 0; i < s.length; i++) dv.setUint8(off + i, s.charCodeAt(i)); };
  str(0, 'RIFF'); dv.setUint32(4, 36 + samples.length * 2, true); str(8, 'WAVE');
  str(12, 'fmt '); dv.setUint32(16, 16, true); dv.setUint16(20, 1, true); dv.setUint16(22, 1, true);
  dv.setUint32(24, sampleRate, true); dv.setUint32(28, sampleRate * 2, true);
  dv.setUint16(32, 2, true); dv.setUint16(34, 16, true);
  str(36, 'data'); dv.setUint32(40, samples.length * 2, true);
  for (let i = 0; i < samples.length; i++) {
    const s = Math.max(-1, Math.min(1, samples[i]));
    dv.setInt16(44 + i * 2, s < 0 ? s * 0x8000 : s * 0x7fff, true);
  }
  return new Blob([buf], { type: 'audio/wav' });
}
