/**
 * retrocache.js — retroactive stream capture engine.
 *
 * Models the pre-roll ("cache recording") architecture of digital cinema
 * firmware: a pre-allocated ring buffer continuously bounds memory to the
 * most recent N seconds of frames at target rate R; an asynchronous trigger
 * drains the cached window [T - N, T] to a persistent sink and then pipes
 * live frames behind it, without ever stalling the O(1) ingest path.
 *
 * State automaton:
 *
 *      arm()                trigger(T)
 *   IDLE ──────► STANDBY_CACHING ──────► ACTIVE_RECORDING
 *    ▲                │  ▲                      │
 *    │    disarm()    │  │                      │ stop() → flush
 *    └────────────────┘  └──── (cache evicts ───┘   completes
 *                               oldest, O(1))
 *
 * Invariants:
 *  - Cache depth ≤ N seconds at every instant (overwrite eviction, plus a
 *    timestamp filter at drain time for off-nominal rates).
 *  - Persisted output is strictly monotonic in frame id and non-decreasing
 *    in timestamp; the cache/live boundary is gapless. Gaps exist only
 *    where sink backpressure forced drops, and every gap is annotated on
 *    the next persisted frame (meta.gapBefore).
 *  - Zero allocation on the ingest hot path: all frame memory lives in two
 *    arenas (cache ring, live queue) allocated at construction; ingest only
 *    copies bytes into a pre-existing slot.
 *  - Sink receives zero-copy subarray views into arena memory; a slot is
 *    not recycled until the sink's write for it has resolved.
 *
 * Concurrency model: single-producer (ingest) / single-consumer (pump)
 * over index-based rings — the JS event loop stands in for the two
 * hardware threads, and the pump never blocks the producer because ingest
 * is fully synchronous and the pump only runs across awaits.
 */

export const State = Object.freeze({
  IDLE: 'IDLE',
  STANDBY_CACHING: 'STANDBY_CACHING',
  ACTIVE_RECORDING: 'ACTIVE_RECORDING',
});

export const Origin = Object.freeze({
  CACHE: 'cache',
  LIVE: 'live',
});

const US_PER_S = 1e6;

/**
 * Fixed-stride frame storage: one contiguous byte buffer plus parallel
 * typed-array metadata columns. Deterministic layout, no per-frame objects.
 */
class Arena {
  constructor(slots, slotBytes) {
    this.slots = slots;
    this.slotBytes = slotBytes;
    this.buffer = new ArrayBuffer(slots * slotBytes);
    this.bytes = new Uint8Array(this.buffer);
    this.timestampsUs = new Float64Array(slots);
    this.frameIds = new Float64Array(slots);
    this.lengths = new Uint32Array(slots);
    this.gapBefore = new Uint32Array(slots);
  }

  write(slot, timestampUs, frameId, payload, gapBefore) {
    this.bytes.set(payload, slot * this.slotBytes);
    this.timestampsUs[slot] = timestampUs;
    this.frameIds[slot] = frameId;
    this.lengths[slot] = payload.length;
    this.gapBefore[slot] = gapBefore;
  }

  view(slot) {
    const base = slot * this.slotBytes;
    return this.bytes.subarray(base, base + this.lengths[slot]);
  }
}

export class RetroCaptureEngine {
  #state = State.IDLE;

  // Cache ring (pre-roll window).
  #cache;
  #cacheCapacity;
  #cacheHead = 0; // next write slot
  #cacheCount = 0;

  // Live queue (SPSC ring between ingest and the sink pump).
  #live;
  #liveCapacity;
  #liveHead = 0; // producer cursor
  #liveTail = 0; // consumer cursor
  #liveCount = 0;

  // Drain plan for the frozen cache, pre-allocated so trigger() is
  // allocation-free too.
  #drainOrder;
  #drainLength = 0;
  #drainCursor = 0;
  #cacheDrainPending = false;

  #sink;
  #pumping = false;
  #stopRequested = false;
  #flushResolvers = [];

  #frameRate;
  #prerollSeconds;
  #windowUs;
  #nominalIntervalUs;

  #nextFrameId = 0;
  #lastTimestampUs = -Infinity;
  #triggerTimestampUs = NaN;
  #pendingGap = 0; // drops accumulated since the last accepted live frame

  // Rate-drift detection (standby only).
  #emaIntervalUs = NaN;
  #intervalSamples = 0;
  #rateWarned = false;
  #skipNextInterval = false;

  #onWarn;

  #stats = {
    ingested: 0,
    cached: 0,
    persistedCache: 0,
    persistedLive: 0,
    dropped: 0,
    rejectedClock: 0,
    ignored: 0,
  };

  /**
   * @param {object} opts
   * @param {number} opts.frameRate        nominal sensor rate R (fps), > 0
   * @param {number} opts.prerollSeconds   pre-roll depth N (s), > 0
   * @param {number} opts.slotBytes        max payload size per frame
   * @param {{write(view: Uint8Array, meta: object): Promise<void>}} opts.sink
   * @param {number} [opts.liveQueueSeconds] depth of the live SPSC queue
   * @param {(code: string, detail: object) => void} [opts.onWarn]
   */
  constructor({ frameRate, prerollSeconds, slotBytes, sink, liveQueueSeconds, onWarn }) {
    if (!(Number.isFinite(frameRate) && frameRate > 0)) throw new RangeError('frameRate must be > 0');
    if (!(Number.isFinite(prerollSeconds) && prerollSeconds > 0)) throw new RangeError('prerollSeconds must be > 0');
    if (!(Number.isInteger(slotBytes) && slotBytes > 0)) throw new RangeError('slotBytes must be a positive integer');
    if (!sink || typeof sink.write !== 'function') throw new TypeError('sink must implement write(view, meta)');

    this.#frameRate = frameRate;
    this.#prerollSeconds = prerollSeconds;
    this.#windowUs = prerollSeconds * US_PER_S;
    this.#nominalIntervalUs = US_PER_S / frameRate;
    this.#sink = sink;
    this.#onWarn = onWarn ?? (() => {});

    this.#cacheCapacity = Math.max(1, Math.ceil(frameRate * prerollSeconds));
    this.#liveCapacity = Math.max(1, Math.ceil(frameRate * (liveQueueSeconds ?? prerollSeconds)));
    this.#cache = new Arena(this.#cacheCapacity, slotBytes);
    this.#live = new Arena(this.#liveCapacity, slotBytes);
    this.#drainOrder = new Int32Array(this.#cacheCapacity);
  }

  get state() { return this.#state; }
  get frameRate() { return this.#frameRate; }
  get prerollSeconds() { return this.#prerollSeconds; }
  get cacheCapacity() { return this.#cacheCapacity; }
  get liveQueueCapacity() { return this.#liveCapacity; }
  get stats() { return { ...this.#stats, cacheDepth: this.#cacheCount, liveQueueDepth: this.#liveCount }; }

  /** Seconds of history actually held in the cache right now. */
  effectivePrerollSeconds() {
    if (this.#cacheCount < 2) return 0;
    const oldest = (this.#cacheHead - this.#cacheCount + this.#cacheCapacity) % this.#cacheCapacity;
    const newest = (this.#cacheHead - 1 + this.#cacheCapacity) % this.#cacheCapacity;
    return (this.#cache.timestampsUs[newest] - this.#cache.timestampsUs[oldest]) / US_PER_S;
  }

  /** IDLE → STANDBY_CACHING. Rejected while a previous drain is pending. */
  arm() {
    if (this.#state !== State.IDLE) {
      throw new Error(`arm() invalid in state ${this.#state}`);
    }
    if (this.#cacheDrainPending || this.#pumping || this.#liveCount > 0) {
      throw new Error('arm() rejected: previous drain still pending');
    }
    this.#cacheHead = 0;
    this.#cacheCount = 0;
    this.#emaIntervalUs = NaN;
    this.#intervalSamples = 0;
    this.#rateWarned = false;
    // The first interval after arming spans the idle gap; don't measure it.
    this.#skipNextInterval = this.#lastTimestampUs !== -Infinity;
    this.#state = State.STANDBY_CACHING;
  }

  /** STANDBY_CACHING → IDLE, discarding the cached window. */
  disarm() {
    if (this.#state !== State.STANDBY_CACHING) {
      throw new Error(`disarm() invalid in state ${this.#state}`);
    }
    this.#cacheCount = 0;
    this.#cacheHead = 0;
    this.#state = State.IDLE;
  }

  /**
   * Synchronous O(1) frame ingest. Never allocates, never awaits.
   * Returns true if the frame was retained (cached or queued), false if it
   * was ignored (IDLE / after stop), rejected (non-monotonic clock), or
   * dropped (live-queue backpressure).
   */
  ingest(timestampUs, payload) {
    if (payload.length > this.#cache.slotBytes) {
      throw new RangeError(`payload of ${payload.length} bytes exceeds slotBytes ${this.#cache.slotBytes}`);
    }
    if (this.#state === State.IDLE || this.#stopRequested) {
      this.#stats.ignored++;
      return false;
    }
    if (!(timestampUs > this.#lastTimestampUs)) {
      this.#stats.rejectedClock++;
      this.#onWarn('CLOCK_VIOLATION', { timestampUs, lastTimestampUs: this.#lastTimestampUs });
      return false;
    }

    if (this.#state === State.STANDBY_CACHING) {
      this.#observeInterval(timestampUs);
      this.#lastTimestampUs = timestampUs;
      this.#cache.write(this.#cacheHead, timestampUs, this.#nextFrameId++, payload, 0);
      this.#cacheHead = (this.#cacheHead + 1) % this.#cacheCapacity;
      if (this.#cacheCount < this.#cacheCapacity) this.#cacheCount++;
      this.#stats.ingested++;
      this.#stats.cached++;
      return true;
    }

    // ACTIVE_RECORDING: enqueue behind the (possibly still draining) cache.
    this.#lastTimestampUs = timestampUs;
    this.#stats.ingested++;
    if (this.#liveCount === this.#liveCapacity) {
      // Backpressure: drop-newest keeps the queued causal chain intact and
      // stays deterministic O(1). The gap is annotated on the next frame
      // that does get through.
      this.#stats.dropped++;
      this.#pendingGap++;
      this.#onWarn('BACKPRESSURE_DROP', { frameId: this.#nextFrameId, timestampUs });
      this.#nextFrameId++;
      return false;
    }
    this.#live.write(this.#liveHead, timestampUs, this.#nextFrameId++, payload, this.#pendingGap);
    this.#pendingGap = 0;
    this.#liveHead = (this.#liveHead + 1) % this.#liveCapacity;
    this.#liveCount++;
    this.#kick();
    return true;
  }

  /**
   * STANDBY_CACHING → ACTIVE_RECORDING. Freezes the cache, plans the drain
   * of every cached frame inside [T - N, T], and starts the async pump.
   * The cache arena is safe to drain lazily because ACTIVE_RECORDING
   * ingest writes only to the live arena.
   *
   * @param {number} [triggerTimestampUs] defaults to the newest cached
   *   frame's timestamp.
   */
  trigger(triggerTimestampUs) {
    if (this.#state !== State.STANDBY_CACHING) {
      throw new Error(`trigger() invalid in state ${this.#state}`);
    }
    const t = triggerTimestampUs ?? (this.#cacheCount > 0 ? this.#lastTimestampUs : 0);
    this.#triggerTimestampUs = t;
    const horizon = t - this.#windowUs;

    this.#drainLength = 0;
    this.#drainCursor = 0;
    const oldest = (this.#cacheHead - this.#cacheCount + this.#cacheCapacity) % this.#cacheCapacity;
    for (let i = 0; i < this.#cacheCount; i++) {
      const slot = (oldest + i) % this.#cacheCapacity;
      const ts = this.#cache.timestampsUs[slot];
      if (ts >= horizon && ts <= t) {
        this.#drainOrder[this.#drainLength++] = slot;
      }
    }
    this.#cacheDrainPending = this.#drainLength > 0;
    this.#pendingGap = 0;
    this.#state = State.ACTIVE_RECORDING;
    if (this.#cacheDrainPending) this.#kick();
    return { triggerTimestampUs: t, cachedFramesPlanned: this.#drainLength };
  }

  /**
   * Stop accepting live frames and resolve once every queued frame —
   * cached and live — has been persisted. Completes ACTIVE_RECORDING →
   * IDLE.
   */
  stop() {
    if (this.#state !== State.ACTIVE_RECORDING) {
      return Promise.reject(new Error(`stop() invalid in state ${this.#state}`));
    }
    this.#stopRequested = true;
    return new Promise((resolve) => {
      this.#flushResolvers.push(resolve);
      this.#maybeFinishFlush();
    });
  }

  /** Reconfigure rate/window. Guarded: only legal in IDLE with no pending drain. */
  reconfigure({ frameRate, prerollSeconds }) {
    if (this.#state !== State.IDLE || this.#cacheDrainPending || this.#pumping) {
      throw new Error('reconfigure() rejected: engine not idle');
    }
    const rate = frameRate ?? this.#frameRate;
    const preroll = prerollSeconds ?? this.#prerollSeconds;
    if (!(Number.isFinite(rate) && rate > 0)) throw new RangeError('frameRate must be > 0');
    if (!(Number.isFinite(preroll) && preroll > 0)) throw new RangeError('prerollSeconds must be > 0');
    const capacity = Math.max(1, Math.ceil(rate * preroll));
    if (capacity > this.#cacheCapacity) {
      throw new RangeError(
        `reconfigure() would need ${capacity} cache slots but only ${this.#cacheCapacity} were pre-allocated`,
      );
    }
    this.#frameRate = rate;
    this.#prerollSeconds = preroll;
    this.#windowUs = preroll * US_PER_S;
    this.#nominalIntervalUs = US_PER_S / rate;
  }

  #observeInterval(timestampUs) {
    if (this.#skipNextInterval) {
      this.#skipNextInterval = false;
      return;
    }
    if (this.#lastTimestampUs !== -Infinity) {
      const dt = timestampUs - this.#lastTimestampUs;
      this.#emaIntervalUs = Number.isNaN(this.#emaIntervalUs)
        ? dt
        : this.#emaIntervalUs * 0.9 + dt * 0.1;
      this.#intervalSamples++;
      if (!this.#rateWarned && this.#intervalSamples >= 30) {
        const drift = Math.abs(this.#emaIntervalUs - this.#nominalIntervalUs) / this.#nominalIntervalUs;
        if (drift > 0.25) {
          this.#rateWarned = true;
          this.#onWarn('RATE_DRIFT', {
            nominalIntervalUs: this.#nominalIntervalUs,
            observedIntervalUs: this.#emaIntervalUs,
            effectivePrerollSeconds: this.effectivePrerollSeconds(),
          });
        }
      }
    }
  }

  #kick() {
    if (!this.#pumping) {
      this.#pumping = true;
      // Detach from the caller's stack so ingest/trigger return immediately.
      queueMicrotask(() => this.#pump());
    }
  }

  async #pump() {
    try {
      for (;;) {
        if (this.#drainCursor < this.#drainLength) {
          const slot = this.#drainOrder[this.#drainCursor];
          await this.#sink.write(this.#cache.view(slot), {
            frameId: this.#cache.frameIds[slot],
            timestampUs: this.#cache.timestampsUs[slot],
            origin: Origin.CACHE,
            gapBefore: 0,
            triggerTimestampUs: this.#triggerTimestampUs,
          });
          this.#drainCursor++;
          this.#stats.persistedCache++;
          if (this.#drainCursor === this.#drainLength) {
            this.#cacheDrainPending = false;
            this.#cacheCount = 0;
            this.#cacheHead = 0;
          }
        } else if (this.#liveCount > 0) {
          const slot = this.#liveTail;
          await this.#sink.write(this.#live.view(slot), {
            frameId: this.#live.frameIds[slot],
            timestampUs: this.#live.timestampsUs[slot],
            origin: Origin.LIVE,
            gapBefore: this.#live.gapBefore[slot],
            triggerTimestampUs: this.#triggerTimestampUs,
          });
          // Slot is recycled only now that the sink write has resolved.
          this.#liveTail = (this.#liveTail + 1) % this.#liveCapacity;
          this.#liveCount--;
          this.#stats.persistedLive++;
        } else {
          break;
        }
      }
    } finally {
      this.#pumping = false;
      this.#maybeFinishFlush();
      // Frames may have arrived while the final awaits settled.
      if (this.#liveCount > 0 || this.#drainCursor < this.#drainLength) this.#kick();
    }
  }

  #maybeFinishFlush() {
    if (
      this.#stopRequested &&
      !this.#pumping &&
      !this.#cacheDrainPending &&
      this.#liveCount === 0 &&
      this.#drainCursor >= this.#drainLength
    ) {
      this.#stopRequested = false;
      this.#state = State.IDLE;
      this.#triggerTimestampUs = NaN;
      const resolvers = this.#flushResolvers;
      this.#flushResolvers = [];
      for (const resolve of resolvers) resolve();
    }
  }
}

/** In-memory sink for tests and demos; optional artificial write latency. */
export class MemorySink {
  constructor({ latencyMs = 0 } = {}) {
    this.latencyMs = latencyMs;
    this.frames = [];
  }

  async write(view, meta) {
    if (this.latencyMs > 0) {
      await new Promise((r) => setTimeout(r, this.latencyMs));
    }
    // A real sink DMAs the view out; here we copy so the record survives
    // slot recycling, but keep the source buffer identity for zero-copy
    // verification.
    this.frames.push({
      meta: { ...meta },
      bytes: Uint8Array.from(view),
      sourceBuffer: view.buffer,
    });
  }
}
