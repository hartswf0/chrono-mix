# chrono-mix

Time instruments for the browser: a family of retroactive capture devices
built on one engine, plus the original VJ turntables experiment that
started the repo.

Open `index.html` for the rack (serve over HTTP — `python3 -m http.server`
— or via GitHub Pages; phone sensors require HTTPS).

## Instruments (`retrocache/`)

Every instrument keeps the last N seconds of a sensor in a pre-allocated
ring buffer and persists nothing — until you hit TRIG, and the past you
already missed is recovered and piped ahead of the live stream. One
engine (`retrocache/retrocache.js`, verified by
`node retrocache/verify.mjs`), one hardware-style front panel per feed:

| unit | feed | pre-roll | export |
|------|------|----------|--------|
| RC-1 CAM | camera, 24 fps | 4 s | PNG |
| RC-2 MIC | microphone PCM | 10 s | WAV |
| RC-3 IMU | accel + gyro + orientation, ~60 Hz | 15 s | CSV |
| RC-4 LAB | pointer, GPS, gyro, magnetometer, lux, battery | per source | CSV |
| RC-5 MIX | the VJ-1 turntables mix screen, 20 fps | 8 s | PNG |
| RC-0 SYN | synthetic canvas, no permissions | 2 s | — |

See [retrocache/README.md](retrocache/README.md) for the architecture,
invariants, and verification harness.

## Legacy lab

`turntables.html` — **VJ-1**: the original dual scratchable VJ turntables
(canvas mixer, scratch decks, BPM metronome, 8 s forward recording).
Preserved unmodified as the repo's founding experiment — and wired into
the family as a signal source: **RC-5 MIX** runs it in a same-origin
iframe and taps its mix-screen canvas as a sensor feed, so you can scratch
a jam and then recover the 8 seconds you didn't think to record.
