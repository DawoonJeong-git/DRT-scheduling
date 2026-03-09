export const START_HOUR = 8;
export const END_HOUR = 22;
export const STEP_MIN = 5;

// 1분당 픽셀 (가로 길이/가독성에 맞춰 조정)
export const PIXELS_PER_MIN = 4;

export function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

export function todayKSTYYYYMMDD() {
  const now = new Date();
  // KST로 "날짜"만 맞추기: UTC+9 보정
  const kst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  const y = kst.getUTCFullYear();
  const m = String(kst.getUTCMonth() + 1).padStart(2, "0");
  const d = String(kst.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

export function formatKSTDateYYYYMMDD(yyyy_mm_dd) {
  return yyyy_mm_dd;
}

export function kstDateStartMs(yyyy_mm_dd) {
  // yyyy-mm-dd를 KST 00:00 기준 epoch ms로 변환
  // 방법: "UTC 00:00"로 파싱 후 -9시간 하면 "KST 00:00"에 해당하는 UTC
  const [y, m, d] = yyyy_mm_dd.split("-").map(Number);
  const utcMs = Date.UTC(y, m - 1, d, 0, 0, 0);
  // KST 00:00 = UTC - 9h
  return utcMs - 9 * 60 * 60 * 1000;
}

export function windowStartMs(yyyy_mm_dd) {
  const day0 = kstDateStartMs(yyyy_mm_dd);
  return day0 + START_HOUR * 60 * 60 * 1000;
}

export function msToMinuteIndexFromStartWindow(ms, yyyy_mm_dd) {
  const start = windowStartMs(yyyy_mm_dd);
  return Math.floor((ms - start) / (60 * 1000));
}

export function buildTicks() {
  const totalMin = (END_HOUR - START_HOUR) * 60;
  const out = [];
  for (let min = 0; min <= totalMin; min += STEP_MIN) {
    const absMin = START_HOUR * 60 + min;
    const hh = Math.floor(absMin / 60);
    const mm = absMin % 60;

    const isHour = mm === 0;
    const hourLabel = isHour ? `${String(hh).padStart(2, "0")}:00` : "";

    // show minute labels only at 10-min marks (00/10/20/30/40/50)
    const showMinuteLabel = mm % 10 === 0;
    const minuteLabel = showMinuteLabel ? String(mm).padStart(2, "0") : "";

    out.push({ minuteIndex: min, isHour, hourLabel, showMinuteLabel, minuteLabel });
  }
  return out;
}

export function roundDownToStepMs(ms, stepMin) {
  const stepMs = stepMin * 60 * 1000;
  return Math.floor(ms / stepMs) * stepMs;
}

export function roundUpToStepMs(ms, stepMin) {
  const stepMs = stepMin * 60 * 1000;
  return Math.ceil(ms / stepMs) * stepMs;
}