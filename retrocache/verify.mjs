/**
 * verify.mjs — verification harness for the retroactive capture engine.
 *
 * Run: node retrocache/verify.mjs
 *
 * Validates the formal invariants of the engine:
 *   1. Gapless temporal continuity across the trigger boundary.
 *   2. Exact [T - N, T] lookback coverage of the drained cache.
 *   3. Strict sequence monotonicity of the persisted stream.
 *   4. Early trigger before cache saturation.
 *   5. Deterministic drop accounting under sink backpressure.
 *   6. Zero-copy transfer: sink views alias the pre-allocated arenas.
 *   7. State-machine guards, including rejection during pending drains.
 *   8. Monotonic-clock rejection and runtime rate-drift detection.
 *   9. Bounded memory / synchronous O(1) ingest characteristics.
 */

import { RetroCaptureEngine, MemorySink, State, Origin } from './retrocache.js';

const US_PER_S = 1e6;
let passed = 0;
let failed = 0;

function check(name, condition, detail = '') {
  if (condition) {
    passed++;
    console.log(`  ok    ${name}`);
  } else {
    failed++;
    console.error(`  FAIL  ${name}${detail ? ` — ${detail}` : ''}`);
  }
}

function section(title) {
  console.log(`\n== ${title}`);
}

function makePayload(slotBytes, frameId) {
  // Deterministic pattern so payload integrity is checkable end to end.
  const p = new Uint8Array(slotBytes);
  for (let i = 0; i < p.length; i++) p[i] = (frameId + i) & 0xff;
  return p;
}

function assertMonotone(frames) {
  let idsOk = true;
  let tsOk = true;
  for (let i = 1; i < frames.length; i++) {
    if (!(frames[i].meta.frameId > frames[i - 1].meta.frameId)) idsOk = false;
    if (!(frames[i].meta.timestampUs >= frames[i - 1].meta.timestampUs)) tsOk = false;
  }
  return { idsOk, tsOk };
}

/* ------------------------------------------------------------------ */
section('1+2+3. Saturated pre-roll: continuity, lookback window, monotonicity');
{
  const R = 120;
  const N = 1; // 1 s pre-roll → 120-slot cache
  const slotBytes = 64;
  const sink = new MemorySink();
  const engine = new RetroCaptureEngine({ frameRate: R, prerollSeconds: N, slotBytes, sink });
  const dt = US_PER_S / R;

  engine.arm();
  // 3 s of standby — cache must retain only the trailing 1 s.
  const standbyFrames = 3 * R;
  for (let i = 0; i < standbyFrames; i++) {
    engine.ingest(i * dt, makePayload(slotBytes, i));
  }
  check('cache bounded to capacity', engine.stats.cacheDepth === engine.cacheCapacity);
  check(
    'effective pre-roll ≈ N seconds',
    Math.abs(engine.effectivePrerollSeconds() - (engine.cacheCapacity - 1) / R) < 1e-9,
    `got ${engine.effectivePrerollSeconds()}`,
  );

  const T = (standbyFrames - 1) * dt;
  const { cachedFramesPlanned } = engine.trigger(T);
  check('drain plans full cache window', cachedFramesPlanned === engine.cacheCapacity);

  // 0.5 s of live frames while the cache drains.
  const liveFrames = 60;
  for (let i = 0; i < liveFrames; i++) {
    engine.ingest(T + (i + 1) * dt, makePayload(slotBytes, standbyFrames + i));
  }
  await engine.stop();

  check('state returns to IDLE after flush', engine.state === State.IDLE);
  check(
    'persisted count = cache + live',
    sink.frames.length === engine.cacheCapacity + liveFrames,
    `got ${sink.frames.length}`,
  );

  const { idsOk, tsOk } = assertMonotone(sink.frames);
  check('frame ids strictly increasing', idsOk);
  check('timestamps non-decreasing', tsOk);

  // Gapless: consecutive ids with no holes across the whole persisted stream.
  let gapless = true;
  for (let i = 1; i < sink.frames.length; i++) {
    if (sink.frames[i].meta.frameId !== sink.frames[i - 1].meta.frameId + 1) gapless = false;
  }
  check('persisted sequence has zero id gaps', gapless);

  // Boundary: last cache frame is the trigger frame, first live frame follows it.
  const cacheOut = sink.frames.filter((f) => f.meta.origin === Origin.CACHE);
  const liveOut = sink.frames.filter((f) => f.meta.origin === Origin.LIVE);
  check('cache frames precede all live frames', sink.frames.indexOf(liveOut[0]) === cacheOut.length);
  check(
    'trigger boundary is contiguous',
    liveOut[0].meta.frameId === cacheOut[cacheOut.length - 1].meta.frameId + 1,
  );

  // Lookback: every cached frame timestamp lies in [T - N, T].
  const windowOk = cacheOut.every(
    (f) => f.meta.timestampUs >= T - N * US_PER_S && f.meta.timestampUs <= T,
  );
  check('all cache frames inside [T - N, T]', windowOk);
  check(
    'oldest persisted frame reaches back a full window',
    Math.abs(cacheOut[0].meta.timestampUs - (T - (engine.cacheCapacity - 1) * dt)) < 1e-6,
  );

  // Payload integrity across the copy into arena and out through the sink.
  const payloadOk = sink.frames.every((f) =>
    f.bytes.every((b, i) => b === ((f.meta.frameId + i) & 0xff)),
  );
  check('payload bytes intact end to end', payloadOk);
}

/* ------------------------------------------------------------------ */
section('4. Early trigger before cache saturation');
{
  const R = 30;
  const slotBytes = 16;
  const sink = new MemorySink();
  const engine = new RetroCaptureEngine({ frameRate: R, prerollSeconds: 2, slotBytes, sink });
  const dt = US_PER_S / R;

  engine.arm();
  const k = 5; // far below the 60-slot capacity
  for (let i = 0; i < k; i++) engine.ingest(i * dt, makePayload(slotBytes, i));
  const { cachedFramesPlanned } = engine.trigger();
  check('drain plans only the k available frames', cachedFramesPlanned === k);
  engine.ingest(k * dt, makePayload(slotBytes, k));
  await engine.stop();
  check('persisted = k cached + 1 live', sink.frames.length === k + 1);
  check('first persisted frame is frame 0', sink.frames[0].meta.frameId === 0);
}

/* ------------------------------------------------------------------ */
section('5. Sink backpressure: deterministic drops, annotated gaps');
{
  const R = 100;
  const slotBytes = 32;
  const sink = new MemorySink({ latencyMs: 2 }); // slower than ingest
  const engine = new RetroCaptureEngine({
    frameRate: R,
    prerollSeconds: 0.1, // 10-slot cache
    liveQueueSeconds: 0.05, // 5-slot live queue → forces overflow
    slotBytes,
    sink,
  });
  const dt = US_PER_S / R;

  engine.arm();
  for (let i = 0; i < 10; i++) engine.ingest(i * dt, makePayload(slotBytes, i));
  engine.trigger();

  // Burst 40 live frames synchronously — the pump cannot run at all during
  // this loop, so at most 5 fit in the queue and the rest must drop without
  // throwing or stalling.
  let allSynchronous = true;
  for (let i = 0; i < 40; i++) {
    const r = engine.ingest((10 + i) * dt, makePayload(slotBytes, 10 + i));
    if (r instanceof Promise) allSynchronous = false;
  }
  const droppedDuringBurst = engine.stats.dropped;
  check('ingest stays synchronous under backpressure', allSynchronous);
  check('burst overflow dropped deterministically', droppedDuringBurst === 40 - engine.liveQueueCapacity, `dropped ${droppedDuringBurst}`);

  // Let the pump make progress, then keep offering frames until one is
  // accepted — it must carry the accumulated gap annotation. Without this,
  // the drops sit at the stream tail and are visible only in stats.
  let followId = 50;
  let acceptedAfterBurst = false;
  while (!acceptedAfterBurst) {
    await new Promise((r) => setTimeout(r, 5));
    acceptedAfterBurst = engine.ingest((10 + followId) * dt, makePayload(slotBytes, 10 + followId));
    followId++;
  }
  await engine.stop();

  const { idsOk } = assertMonotone(sink.frames);
  check('ids still strictly increasing with drops', idsOk);

  // Every id hole in the persisted stream must be accounted for by the
  // gapBefore annotation on the frame that follows it.
  let annotatedGaps = 0;
  let holes = 0;
  for (let i = 1; i < sink.frames.length; i++) {
    const hole = sink.frames[i].meta.frameId - sink.frames[i - 1].meta.frameId - 1;
    holes += hole;
    annotatedGaps += sink.frames[i].meta.gapBefore;
    if (hole !== sink.frames[i].meta.gapBefore) {
      check('gap annotation matches id hole', false, `at index ${i}: hole ${hole} vs gapBefore ${sink.frames[i].meta.gapBefore}`);
    }
  }
  check('total id holes equal dropped count', holes === engine.stats.dropped, `holes ${holes} vs dropped ${engine.stats.dropped}`);
  check('total annotated gaps equal dropped count', annotatedGaps === engine.stats.dropped);
  check('cache portion persisted un-torn', sink.frames.filter((f) => f.meta.origin === Origin.CACHE).length === 10);
}

/* ------------------------------------------------------------------ */
section('6. Zero-copy transfer mechanics');
{
  const slotBytes = 128;
  const buffers = new Set();
  const sink = {
    async write(view, meta) {
      buffers.add(view.buffer);
      this.count = (this.count ?? 0) + 1;
      this.lastOffsetAligned = view.byteOffset % slotBytes === 0;
    },
  };
  const engine = new RetroCaptureEngine({ frameRate: 10, prerollSeconds: 1, slotBytes, sink });
  const dt = US_PER_S / 10;
  engine.arm();
  for (let i = 0; i < 15; i++) engine.ingest(i * dt, makePayload(slotBytes, i));
  engine.trigger();
  for (let i = 15; i < 20; i++) engine.ingest(i * dt, makePayload(slotBytes, i));
  await engine.stop();

  check('sink saw every frame', sink.count === 10 + 5);
  check('sink views alias exactly the two pre-allocated arenas', buffers.size === 2);
  check('views are slot-aligned into arena memory', sink.lastOffsetAligned === true);
}

/* ------------------------------------------------------------------ */
section('7. State-machine guards');
{
  const sink = new MemorySink({ latencyMs: 5 });
  const engine = new RetroCaptureEngine({ frameRate: 10, prerollSeconds: 1, slotBytes: 8, sink });

  let threw = false;
  try { engine.trigger(); } catch { threw = true; }
  check('trigger() rejected in IDLE', threw);

  threw = false;
  try { engine.stop().catch(() => {}); engine.disarm(); } catch { threw = true; }
  check('disarm() rejected in IDLE', threw);

  engine.arm();
  threw = false;
  try { engine.reconfigure({ frameRate: 20 }); } catch { threw = true; }
  check('reconfigure() rejected outside IDLE', threw);

  const dt = US_PER_S / 10;
  for (let i = 0; i < 10; i++) engine.ingest(i * dt, makePayload(8, i));
  engine.trigger();

  // Drain is pending (slow sink): stop, then immediately try to re-arm
  // before the flush resolves.
  const flushed = engine.stop();
  threw = false;
  try { engine.arm(); } catch { threw = true; }
  check('arm() rejected while drain pending', threw);
  await flushed;
  engine.arm(); // must succeed now
  check('arm() accepted after flush completes', engine.state === State.STANDBY_CACHING);
  engine.disarm();

  // Legal reconfigure in IDLE, within pre-allocated bounds.
  engine.reconfigure({ frameRate: 5, prerollSeconds: 2 });
  check('reconfigure() accepted in IDLE within capacity', engine.frameRate === 5 && engine.prerollSeconds === 2);
  threw = false;
  try { engine.reconfigure({ frameRate: 1000 }); } catch { threw = true; }
  check('reconfigure() beyond pre-allocated capacity rejected', threw);
}

/* ------------------------------------------------------------------ */
section('8. Clock violations and runtime rate drift');
{
  const warns = [];
  const sink = new MemorySink();
  const engine = new RetroCaptureEngine({
    frameRate: 30,
    prerollSeconds: 1,
    slotBytes: 8,
    sink,
    onWarn: (code, detail) => warns.push({ code, detail }),
  });
  engine.arm();
  const dt = US_PER_S / 30;
  engine.ingest(0, makePayload(8, 0));
  engine.ingest(dt, makePayload(8, 1));
  const accepted = engine.ingest(dt / 2, makePayload(8, 2)); // clock regression
  check('non-monotonic timestamp rejected', accepted === false && engine.stats.rejectedClock === 1);
  check('clock violation warned', warns.some((w) => w.code === 'CLOCK_VIOLATION'));

  // Feed at 2× nominal rate: pre-roll ring now holds < N seconds.
  for (let i = 0; i < 60; i++) engine.ingest(dt + (i + 1) * (dt / 2), makePayload(8, i));
  check('rate drift detected', warns.some((w) => w.code === 'RATE_DRIFT'));
  check(
    'effective pre-roll reported below nominal N',
    engine.effectivePrerollSeconds() < engine.prerollSeconds,
    `got ${engine.effectivePrerollSeconds()}`,
  );
  engine.disarm();
}

/* ------------------------------------------------------------------ */
section('9. Bounded memory and ingest cost');
{
  const R = 120;
  const slotBytes = 256;
  const sink = new MemorySink();
  const engine = new RetroCaptureEngine({ frameRate: R, prerollSeconds: 2, slotBytes, sink });
  const dt = US_PER_S / R;
  const payload = makePayload(slotBytes, 7); // reused: ingest itself must not allocate

  engine.arm();
  const frames = 200_000;
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < frames; i++) engine.ingest(i * dt, payload);
  const t1 = process.hrtime.bigint();
  const nsPerFrame = Number(t1 - t0) / frames;

  check('cache depth still bounded after 200k frames', engine.stats.cacheDepth === engine.cacheCapacity);
  check('no persistence occurred in standby', sink.frames.length === 0);
  console.log(`  info  ingest cost ≈ ${nsPerFrame.toFixed(0)} ns/frame over ${frames} frames (payload ${slotBytes} B)`);
  engine.disarm();
}

/* ------------------------------------------------------------------ */
console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
