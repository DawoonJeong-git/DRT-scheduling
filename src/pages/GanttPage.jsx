import React, { useEffect, useMemo, useRef, useState } from "react";
import { fetchGantt } from "../api/ganttApi.js";
import DatePickerField from "../components/DatePickerField.jsx";
import ThemeControls from "../components/ThemeControls.jsx";
import GanttChart from "../components/GanttChart.jsx";
import { formatKSTDateYYYYMMDD, todayKSTYYYYMMDD } from "../utils/time.js";


function Legend() {
  return (
    <div className="legend">
      <div className="legend-item">
        <span className="legend-swatch sw-available" />
        <span>Available</span>
      </div>
      <div className="legend-item">
        <span className="legend-swatch sw-moving" />
        <span>Moving</span>
      </div>
      <div className="legend-item">
        <span className="legend-swatch sw-boarding" />
        <span>Boarding</span>
      </div>
      <div className="legend-item">
        <span className="legend-swatch sw-inservice" />
        <span>In service</span>
      </div>
    </div>
  );
}

export default function GanttPage() {
  const [date, setDate] = useState(todayKSTYYYYMMDD());
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);

  const abortRef = useRef(null);

  const title = useMemo(() => {
    const pretty = formatKSTDateYYYYMMDD(date);
    return `Dispatch Gantt • ${pretty}`;
  }, [date]);

  async function load() {
    setLoading(true);
    setErr("");

    if (abortRef.current) abortRef.current.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    try {
      const res = await fetchGantt(date, { signal: controller.signal });
      setData(res);
      setLastUpdated(new Date());
    } catch (e) {
      if (e?.name === "AbortError") return;
      setErr(e?.message || "Failed to load gantt data.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [date]);

  useEffect(() => {
    if (!autoRefresh) return;
    const id = setInterval(() => void load(), 60_000);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRefresh, date]);

  return (
    <div className="app">
      <header className="topbar">
        <div className="topbar-left">
          <div className="title">{title}</div>

          <div className="controls-row">
            <DatePickerField value={date} onChange={setDate} />

            <button className="btn" onClick={() => void load()} disabled={loading}>
              {loading ? "Loading..." : "Refresh"}
            </button>

            <label className="toggle">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
              />
              <span>Auto refresh (60s)</span>
            </label>

            <Legend />

            <div className="muted">
              Last updated:{" "}
              {lastUpdated ? lastUpdated.toLocaleTimeString("en-GB", { hour12: false }) : "-"}
            </div>
          </div>
        </div>

        <div className="topbar-right">
          <ThemeControls />
        </div>
      </header>

      {err ? (
        <div className="error-banner">
          <div className="error-title">Error</div>
          <div className="error-msg">{err}</div>
        </div>
      ) : null}

      <main className="main">
        <GanttChart data={data} loading={loading} />
      </main>
    </div>
  );
}