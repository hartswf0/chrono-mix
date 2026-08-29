/**
 * instrument.js — panel wiring shared by the retro cache devices.
 * LEDs, LCD readouts, segment-bar meters, transport keys.
 */

/** 24-segment level bar. */
export function drawSegments(canvas, frac, onColor) {
  const ctx = canvas.getContext('2d');
  const n = 24, w = canvas.width, h = canvas.height, gap = 2;
  const sw = (w - gap * (n - 1)) / n;
  ctx.clearRect(0, 0, w, h);
  for (let i = 0; i < n; i++) {
    ctx.fillStyle = (i + 0.5) / n <= frac ? onColor : '#b7b4ad';
    ctx.fillRect(i * (sw + gap), 0, sw, h);
  }
}

/**
 * Keep panel indicators live. ui fields are all optional:
 *   stbyLed, recLed        — .led elements
 *   lookLcd, persLcd, dropLcd — <b> elements inside .lcd
 *   ringBar, queueBar      — <canvas> meters
 *   extra()                — page hook called each frame
 * Also mirrors engine state onto <body data-state> for scripting.
 */
export function wirePanel(getEngine, ui) {
  function tick() {
    const e = getEngine();
    if (e) {
      const s = e.stats;
      document.body.dataset.state = e.state;
      ui.stbyLed?.classList.toggle('on', e.state === 'STANDBY_CACHING');
      ui.recLed?.classList.toggle('on', e.state === 'ACTIVE_RECORDING');
      if (ui.lookLcd) ui.lookLcd.textContent = e.effectivePrerollSeconds().toFixed(1);
      if (ui.persLcd) ui.persLcd.textContent = s.persistedCache + s.persistedLive;
      if (ui.dropLcd) {
        ui.dropLcd.textContent = s.dropped;
        ui.dropLcd.style.color = s.dropped > 0 ? '#ff6a33' : '';
      }
      if (ui.ringBar) drawSegments(ui.ringBar, s.cacheDepth / e.cacheCapacity, '#12b06e');
      if (ui.queueBar) drawSegments(ui.queueBar, s.liveQueueDepth / e.liveQueueCapacity, '#ff4f00');
    } else {
      document.body.dataset.state = 'IDLE';
    }
    ui.extra?.();
    requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
}

/**
 * Wire ARM / TRIGGER / STOP keys around async page hooks.
 * onArm may await permissions; failures land in errLcd and re-enable ARM.
 * onStop runs after the engine flush completes.
 */
export function wireTransport({ arm, trig, stop, errLcd, onArm, onTrig, onStop }) {
  arm.onclick = async () => {
    arm.disabled = true;
    if (errLcd) errLcd.textContent = 'READY';
    try {
      await onArm();
      trig.disabled = false;
      stop.disabled = true;
    } catch (e) {
      if (errLcd) errLcd.textContent = `ERR ${e.message}`.toUpperCase();
      arm.disabled = false;
    }
  };
  trig.onclick = () => {
    onTrig();
    trig.disabled = true;
    stop.disabled = false;
  };
  stop.onclick = async () => {
    stop.disabled = true;
    await onStop();
    arm.disabled = false;
  };
}
