# chrono-mix
Visualize and analyze your audio files with precision. Upload audio, zoom in for details, and download high-quality waveform images with time markers. Ideal for audio enthusiasts, podcasters, and developers.

## retrocache

`retrocache/` holds a retroactive stream capture engine — a pre-roll ring
buffer with an {IDLE, STANDBY_CACHING, ACTIVE_RECORDING} state machine that
records the N seconds *before* you hit the trigger, modeled on cinema-camera
cache recording. See [retrocache/README.md](retrocache/README.md); run
`node retrocache/verify.mjs` for the verification harness or serve the repo
and open `retrocache/` for trial pages covering every browser-accessible
sensor: camera, microphone, IMU/accelerometer, pointer, geolocation,
gyroscope, magnetometer, ambient light, and battery.
