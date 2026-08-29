# retrocache — retroactive stream capture engine

A real-time pre-roll capture engine modeled on the cache-recording
architecture of digital cinema firmware (Sony FX6-style "record the past").
Continuous ingestion under fixed memory becomes an on-demand retroactive
persistent log by splitting the continuous FIFO cache update path from an
asynchronous flush-and-append storage pipe.

## State automaton

```
     arm()                trigger(T)
  IDLE ──────► STANDBY_CACHING ──────► ACTIVE_RECORDING
   ▲                │  ▲                      │
   │    disarm()    │  │                      │ stop() → flush
   └────────────────┘  └── cache overwrites ──┘   completes
                           oldest, O(1)
```

- **IDLE** — nothing is captured. The only state in which `reconfigure()`
  is legal. `arm()` is rejected while a previous drain is still pending.
- **STANDBY_CACHING** — every frame is written into a pre-allocated ring
  buffer sized `ceil(R × N)` slots; the oldest frame is overwritten in
  O(1). Nothing touches storage.
- **ACTIVE_RECORDING** — entered by `trigger(T)`. The cache is frozen and
  every cached frame with timestamp in `[T − N, T]` drains to the sink
  through an async pump; live frames are simultaneously queued behind it
  in a single-producer/single-consumer ring. Ingest never awaits.

## Memory layout

Two `Arena`s, allocated once at construction and never resized:

| region      | size                        | contents                          |
|-------------|-----------------------------|-----------------------------------|
| cache arena | `ceil(R·N) × slotBytes`     | pre-roll ring payload bytes       |
| live arena  | `ceil(R·Q) × slotBytes`     | SPSC live-queue payload bytes     |
| metadata    | 4 typed-array columns each  | timestampUs, frameId, length, gap |

The ingest hot path only copies bytes into an existing slot — no
allocation, no promise, no await (measured ≈60 ns/frame for 256-byte
payloads in the harness). The sink receives zero-copy `subarray` views
into arena memory; a live slot is recycled only after the sink's write for
it resolves, and the frozen cache is safe to drain lazily because
ACTIVE_RECORDING ingest writes exclusively to the live arena.

## Invariants

1. **Bounded lookback** — the cache never holds more than `ceil(R·N)`
   frames; at drain time a timestamp filter additionally clamps output to
   `[T − N, T]` even under off-nominal input rates.
2. **Gapless boundary** — persisted output is strictly monotonic in frame
   id and non-decreasing in timestamp; the last cache frame and the first
   live frame are consecutive ids.
3. **Accounted tears** — under sink backpressure the live queue drops the
   newest frame (deterministic O(1)); every id hole in the persisted
   stream equals the `gapBefore` annotation on the frame that follows it,
   and totals match `stats.dropped`.
4. **Guarded transitions** — `trigger()`/`stop()`/`arm()`/`reconfigure()`
   throw outside their legal states, and re-arming is rejected while a
   drain is pending.

## Edge cases handled

- **Early trigger** before cache saturation drains only the `k < capacity`
  frames that exist.
- **Backpressure** never stalls or throws on the ingest path; drops are
  counted, annotated, and surfaced via `onWarn('BACKPRESSURE_DROP')`.
- **Clock violations** (non-monotonic timestamps) are rejected and counted.
- **Runtime rate drift** (observed interval ≳25% off nominal) fires
  `onWarn('RATE_DRIFT')`, and `effectivePrerollSeconds()` always reports
  the lookback actually available.
- **Reconfiguration** is legal only in IDLE and only within the
  pre-allocated slot budget — capacity is never reallocated at runtime.

## Files

- `retrocache.js` — the engine (`RetroCaptureEngine`, `MemorySink`,
  `State`, `Origin`). ES module, browser- and Node-compatible, no
  dependencies.
- `verify.mjs` — verification harness: `node retrocache/verify.mjs`
  (38 checks over continuity, lookback, monotonicity, backpressure,
  zero-copy aliasing, state guards, clock/rate faults, bounded memory).
- `demo.html` — synthetic demo: a 30 fps canvas sensor with a 2 s pre-roll
  ring; no permissions needed anywhere.

## Sensor trials

`index.html` links one trial page per browser-accessible sensor feed, all
driving the same engine (serve the repo over HTTP — `python3 -m
http.server` — and open `/retrocache/`; on phones the sensor APIs require
HTTPS):

- `camera.html` — rear camera via `getUserMedia`, 24 fps thumbnails, 4 s
  pre-roll; scrub/replay the recovered frames.
- `audio.html` — microphone PCM in ~43 ms chunks, 10 s pre-roll; play back
  the recovered audio or download it as WAV.
- `motion.html` — IMU via `devicemotion`/`deviceorientation` (~60 Hz,
  12 channels), 15 s pre-roll; stacked-lane trace plot and CSV export.
  Handles the iOS motion-permission prompt on ARM and offers a synthetic
  source for desktops without an IMU.
- `sensors.html` — a source-adapter lab for everything else: pointer/touch,
  geolocation (`watchPosition`), Generic Sensor API feeds (gyroscope,
  magnetometer, ambient light) and battery. Unsupported sources on the
  current device are greyed out; each persists to the same trace plot +
  CSV.
- Shared chrome lives in `demo.css` and `demo-ui.js` (status wiring,
  monotonic µs timestamper, stacked-lane plotting, CSV/WAV encoding).

All trial pages were exercised headlessly (fake camera/microphone devices,
synthetic IMU, scripted pointer input) with a full
ARM → TRIGGER → STOP cycle and zero console errors.
