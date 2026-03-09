import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  START_HOUR,
  END_HOUR,
  PIXELS_PER_MIN,
  buildTicks,
  msToMinuteIndexFromStartWindow,
  clamp,
  kstDateStartMs
} from "../utils/time.js";

const ROW_HEIGHT = 48;
const SIDEBAR_WIDTH = 320;
const HEADER_HEIGHT = 44;
const VSCROLL_W = 14;
const MAX_VIEWPORT_HEIGHT = 560;

function statusClass(status) {
  if (status === "IN_SERVICE") return "bar-inservice";
  if (status === "MOVING") return "bar-moving";
  if (status === "BOARDING") return "bar-boarding";
  return "bar-available";
}

function statusLabel(status) {
  if (status === "IN_SERVICE") return "In service";
  if (status === "MOVING") return "Moving";
  if (status === "BOARDING") return "Boarding/Alighting";
  return "Available";
}

// ✅ StationID와 (Name) 사이 공백 1칸
function formatStation(id, name) {
  const sid = (id ?? "").toString().trim();
  const sname = (name ?? "").toString().trim();
  if (!sid && !sname) return "-";
  if (sid && sname) return `${sid} (${sname})`;
  return sid || sname;
}

export default function GanttChart({ data, loading }) {
  const viewportRef = useRef(null);
  const sidebarScrollRef = useRef(null);
  const hScrollRef = useRef(null);
  const vScrollRef = useRef(null);

  const [scroll, setScroll] = useState({ x: 0, y: 0 });
  const scrollRef = useRef({ x: 0, y: 0 });
  const rafRef = useRef(0);

  const [showH, setShowH] = useState(false);
  const [showV, setShowV] = useState(false);

  const [tip, setTip] = useState(null);

  const ticks = useMemo(() => buildTicks(), []);
  const totalMinutes = (END_HOUR - START_HOUR) * 60;
  const timelineWidth = totalMinutes * PIXELS_PER_MIN;

  const vehicles = data?.vehicles ?? [];
  const intervals = data?.intervals ?? [];

  const vehicleIndex = useMemo(() => {
    const m = new Map();
    vehicles.forEach((v, idx) => m.set(v.vehicleID, idx));
    return m;
  }, [vehicles]);

  const dayStartMs = useMemo(
    () => (data?.date ? kstDateStartMs(data.date) : null),
    [data?.date]
  );

  // ✅ bar는 1분 단위로 스냅
  const normalizedIntervals = useMemo(() => {
    if (!dayStartMs) return [];
    const MIN_MS = 60_000;

    return intervals
      .map((it) => {
        const start = Math.floor(it.startMs / MIN_MS) * MIN_MS;
        const end = Math.ceil(it.endMs / MIN_MS) * MIN_MS;
        return { ...it, startMs: start, endMs: end };
      })
      .filter((it) => it.endMs > it.startMs && vehicleIndex.has(it.vehicleID));
  }, [intervals, dayStartMs, vehicleIndex]);

  const rowsHeight = vehicles.length * ROW_HEIGHT;
  const viewportHeight = Math.min(rowsHeight, MAX_VIEWPORT_HEIGHT);

  function getMaxScroll() {
    const vp = viewportRef.current;
    if (!vp) return { maxX: 0, maxY: 0 };
    const cw = Math.max(1, vp.clientWidth);
    const ch = Math.max(1, vp.clientHeight);
    return {
      maxX: Math.max(0, timelineWidth - cw),
      maxY: Math.max(0, rowsHeight - ch)
    };
  }

  function syncScroll(next, src) {
    const vp = viewportRef.current;
    const hs = hScrollRef.current;
    const vs = vScrollRef.current;
    const sb = sidebarScrollRef.current;

    const { maxX, maxY } = getMaxScroll();
    const x = clamp(next.x ?? 0, 0, maxX);
    const y = clamp(next.y ?? 0, 0, maxY);

    scrollRef.current = { x, y };
    setScroll({ x, y });

    if (vp && src !== "viewport") vp.scrollTo({ left: x, top: y });

    if (hs) {
      if (src !== "hscroll") hs.scrollLeft = x;
      else if (hs.scrollLeft !== x) hs.scrollLeft = x;
    }

    if (vs) {
      if (src !== "vscroll") vs.scrollTop = y;
      else if (vs.scrollTop !== y) vs.scrollTop = y;
    }

    if (sb) sb.style.transform = `translateY(${-y}px)`;
  }

  function onViewportScroll(e) {
    const el = e.currentTarget;
    cancelAnimationFrame(rafRef.current);
    rafRef.current = requestAnimationFrame(() =>
      syncScroll({ x: el.scrollLeft, y: el.scrollTop }, "viewport")
    );
  }

  function onHScroll(e) {
    const el = e.currentTarget;
    const { y } = scrollRef.current;
    cancelAnimationFrame(rafRef.current);
    rafRef.current = requestAnimationFrame(() => syncScroll({ x: el.scrollLeft, y }, "hscroll"));
  }

  function onVScroll(e) {
    const el = e.currentTarget;
    const { x } = scrollRef.current;
    cancelAnimationFrame(rafRef.current);
    rafRef.current = requestAnimationFrame(() => syncScroll({ x, y: el.scrollTop }, "vscroll"));
  }

  useEffect(() => {
    const update = () => {
      const vp = viewportRef.current;
      if (!vp) return;

      const cw = Math.max(1, vp.clientWidth);
      const ch = Math.max(1, vp.clientHeight);

      setShowH(timelineWidth > cw + 1);
      setShowV(rowsHeight > ch + 1);

      syncScroll({ ...scrollRef.current }, "resize");
    };

    update();
    const id = requestAnimationFrame(update);
    window.addEventListener("resize", update);
    return () => {
      cancelAnimationFrame(id);
      window.removeEventListener("resize", update);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [timelineWidth, rowsHeight, viewportHeight]);

  useEffect(() => {
    const sb = sidebarScrollRef.current;
    if (sb) sb.style.transform = `translateY(${-scroll.y}px)`;
  }, [scroll.y]);

  if (!data && loading) return <div className="placeholder">Loading…</div>;
  if (!data) return <div className="placeholder">No data.</div>;

  return (
    <div className="gantt-shell">
      <div
        className="gantt-grid"
        style={{
          gridTemplateColumns: `${SIDEBAR_WIDTH}px 1fr ${showV ? `${VSCROLL_W}px` : "0px"}`
        }}
      >
        {/* Sidebar */}
        <div className="sidebar" style={{ width: SIDEBAR_WIDTH }}>
          <div className="sidebar-header" style={{ height: HEADER_HEIGHT }}>
            Vehicle information
          </div>

          <div className="sidebar-body" style={{ height: viewportHeight }}>
            <div ref={sidebarScrollRef} className="sidebar-body-inner">
              {vehicles.map((v) => (
                <div key={v.vehicleID} className="sidebar-row" style={{ height: ROW_HEIGHT }}>
                  <div className="veh-id mono">{v.vehicleID}</div>
                  <div className="veh-meta">
                    <div className="veh-type">{v.vehicleType || "-"}</div>
                    <div className="veh-svc muted">{v.operationServiceType || "-"}</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Timeline */}
        <div className="timeline">
          <div className="timeline-header" style={{ height: HEADER_HEIGHT }}>
            <div
              className="ticks"
              style={{ width: timelineWidth, transform: `translateX(${-scroll.x}px)` }}
            >
              {ticks
                .filter((t) => t.isHour)
                .map((t) => (
                  <div
                    key={`h-${t.minuteIndex}`}
                    className="tick-hour"
                    style={{ left: (t.minuteIndex + 30) * PIXELS_PER_MIN }}
                  >
                    {t.hourLabel}
                  </div>
                ))}

              {ticks
                .filter((t) => t.showMinuteLabel)
                .map((t) => (
                  <div
                    key={`m-${t.minuteIndex}`}
                    className="tick-min"
                    style={{ left: t.minuteIndex * PIXELS_PER_MIN }}
                  >
                    {t.minuteLabel}
                  </div>
                ))}
            </div>
          </div>

          <div
            className="viewport"
            ref={viewportRef}
            onScroll={onViewportScroll}
            style={{ height: viewportHeight }}
          >
            <div className="canvas" style={{ width: timelineWidth, height: rowsHeight }}>
              {/* grid lines */}
              {ticks.map((t) => (
                <div
                  key={`grid-${t.minuteIndex}`}
                  className={t.isHour ? "vline-hour" : "vline"}
                  style={{ left: t.minuteIndex * PIXELS_PER_MIN, height: rowsHeight }}
                />
              ))}
              {vehicles.map((v, idx) => (
                <div
                  key={`h-${v.vehicleID}`}
                  className="hline"
                  style={{ top: idx * ROW_HEIGHT, width: timelineWidth }}
                />
              ))}
              <div className="hline" style={{ top: rowsHeight, width: timelineWidth }} />

              {/* bars */}
              {normalizedIntervals.map((it, i) => {
                const rowIdx = vehicleIndex.get(it.vehicleID);

                const startMinIdx = msToMinuteIndexFromStartWindow(it.startMs, data.date);
                const endMinIdx = msToMinuteIndexFromStartWindow(it.endMs, data.date);

                const x1 = clamp(startMinIdx, 0, totalMinutes) * PIXELS_PER_MIN;
                const x2 = clamp(endMinIdx, 0, totalMinutes) * PIXELS_PER_MIN;
                const w = Math.max(2, x2 - x1);

                const laneCount = Math.max(1, it.laneCount || 1);
                const laneIndex = clamp(it.laneIndex || 0, 0, laneCount - 1);

                const rowTop = rowIdx * ROW_HEIGHT;
                const rowBottom = rowTop + ROW_HEIGHT;

                // ✅ 안정화 포인트:
                // - laneTop/laneBottom을 round가 아니라 ceil/floor로 정수 스냅
                // - outer inset을 pooling(n>=2)일 때 더 크게(2px) 적용하여 경계선 침범 방지
                const laneH = ROW_HEIGHT / laneCount;

                const laneTop = rowTop + Math.ceil(laneIndex * laneH);
                const laneBottom = rowTop + Math.floor((laneIndex + 1) * laneH);

                const isPooling = laneCount > 1;
                const isInService = it.status === "IN_SERVICE";

                const OUTER_INSET = isPooling ? 2 : 1;          // ✅ 핵심
                const innerPad = (isPooling && isInService) ? 0 : 3;

                // 바깥 경계 인셋은 바깥쪽 lane에만 적용
                const topInset = laneIndex === 0 ? OUTER_INSET : 0;
                const bottomInset = laneIndex === laneCount - 1 ? OUTER_INSET : 0;

                let y = laneTop + innerPad + topInset;
                let bottom = laneBottom - innerPad - bottomInset;

                // row 경계 안으로 강제
                y = Math.max(y, rowTop + OUTER_INSET);
                bottom = Math.min(bottom, rowBottom - OUTER_INSET);

                const h = Math.max(2, bottom - y);

                const cls = `bar ${statusClass(it.status)}`;

                // tooltip: IN_SERVICE only
                const onMove = (e) => {
                  if (it.status !== "IN_SERVICE") return;

                  const shell = e.currentTarget.closest(".gantt-shell");
                  const rect = shell?.getBoundingClientRect();
                  const baseX = rect ? rect.left : 0;
                  const baseY = rect ? rect.top : 0;

                  const TIP_W = 320;
                  const TIP_H = 190;
                  const M = 10;

                  let tx = e.clientX - baseX + 12;
                  let ty = e.clientY - baseY + 12;

                  const maxX = (rect?.width || window.innerWidth) - TIP_W - M;
                  const maxY = (rect?.height || window.innerHeight) - TIP_H - M;

                  if (tx > maxX) tx = Math.max(M, e.clientX - baseX - TIP_W - 12);
                  if (ty > maxY) ty = Math.max(M, e.clientY - baseY - TIP_H - 12);

                  const title = `${it.vehicleID} • ${statusLabel(it.status)}`;
                  const lines = [];

                  if (it.operationID) lines.push(`operationID: ${it.operationID}`);

                  lines.push(`ReserveType: ${(it.reserveType ?? "").toString() || "-"}`);

                  const origin = formatStation(it.pickupStationID, it.pickupStationName);
                  const dest = formatStation(it.dropoffStationID, it.dropoffStationName);

                  lines.push(`OriginStation: ${origin}`);
                  lines.push(`DestinationStation: ${dest}`);

                  if (it.label) lines.push(it.label);

                  setTip({ x: tx, y: ty, title, lines });
                };

                return (
                  <div
                    key={`${it.vehicleID}-${it.startMs}-${i}`}
                    className={cls}
                    style={{ left: x1, top: y, width: w, height: h }}
                    onMouseEnter={it.status === "IN_SERVICE" ? onMove : undefined}
                    onMouseMove={it.status === "IN_SERVICE" ? onMove : undefined}
                    onMouseLeave={it.status === "IN_SERVICE" ? () => setTip(null) : undefined}
                  />
                );
              })}
            </div>
          </div>
        </div>

        {/* right-attached vscroll */}
        {showV ? (
          <div className="vscroll-wrap" style={{ height: HEADER_HEIGHT + viewportHeight }}>
            <div className="vscroll-spacer" style={{ height: HEADER_HEIGHT }} />
            <div
              className="vscroll"
              ref={vScrollRef}
              onScroll={onVScroll}
              style={{ height: viewportHeight }}
            >
              <div style={{ width: 1, height: rowsHeight }} />
            </div>
          </div>
        ) : (
          <div />
        )}
      </div>

      {/* bottom hscroll only */}
      {showH ? (
        <div className="hscroll-row" style={{ paddingLeft: SIDEBAR_WIDTH }}>
          <div className="hscroll" ref={hScrollRef} onScroll={onHScroll}>
            <div style={{ width: timelineWidth, height: 1 }} />
          </div>
        </div>
      ) : null}

      {/* tooltip */}
      {tip ? (
        <div className="tooltip" style={{ left: tip.x, top: tip.y }}>
          <div className="tooltip-title">{tip.title}</div>
          {tip.lines?.length ? (
            <div className="tooltip-lines">
              {tip.lines.map((t, idx) => (
                <div key={idx} className="tooltip-line">
                  {t}
                </div>
              ))}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}