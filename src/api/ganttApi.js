function normalizePayload(raw) {
  if (!raw || typeof raw !== "object") throw new Error("Invalid API response.");

  const vehicles = Array.isArray(raw.vehicles) ? raw.vehicles : [];
  const intervals = Array.isArray(raw.intervals) ? raw.intervals : [];

  return {
    date: raw.date,
    updatedAtMs: raw.updatedAtMs ?? Date.now(),
    timeWindow: raw.timeWindow ?? { start: "08:00", end: "22:00", stepMin: 5 },

    vehicles: vehicles.map((v) => ({
      vehicleID: String(v.vehicleID ?? ""),
      vehicleType: v.vehicleType ?? "",
      operationServiceType: v.operationServiceType ?? ""
    })),

    intervals: intervals.map((it) => ({
      vehicleID: String(it.vehicleID ?? ""),
      operationID: it.operationID ?? null,
      status: it.status ?? "MOVING",
      startMs: Number(it.startMs),
      endMs: Number(it.endMs),
      laneIndex: Number.isFinite(it.laneIndex) ? it.laneIndex : 0,
      laneCount: Number.isFinite(it.laneCount) && it.laneCount > 0 ? it.laneCount : 1,
      label: it.label ?? "",
      reserveType: it.reserveType ?? "",
      pickupStationID: it.pickupStationID ?? "",
      dropoffStationID: it.dropoffStationID ?? "",
      pickupStationName: it.pickupStationName ?? "",
      dropoffStationName: it.dropoffStationName ?? ""
    }))
  };
}

const API_BASE = (import.meta.env.VITE_API_BASE || "").replace(/\/$/, "");

export async function fetchGantt(date, { signal } = {}) {
  const url = `${API_BASE}/api/gantt?date=${encodeURIComponent(date)}`;
  const resp = await fetch(url, { method: "GET", signal });

  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`API error (${resp.status}): ${text || resp.statusText}`);
  }

  const json = await resp.json();
  return normalizePayload(json);
}