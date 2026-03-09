// src/api/ganttApi.js

// API contract (expected):
// {
//   date: "YYYY-MM-DD",
//   updatedAtMs: number,
//   timeWindow: { start: "08:00", end: "22:00", stepMin: 5 },
//   vehicles: [{ vehicleID, vehicleType, operationServiceType }],
//   intervals: [
//     {
//       vehicleID, operationID, status, startMs, endMs, laneIndex, laneCount, label,
//       pickupStationID, dropoffStationID, pickupStationName, dropoffStationName, reserveType
//     }
//   ]
// }

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

    // ✅ IMPORTANT: preserve extra fields for tooltip
    intervals: intervals.map((it) => ({
      vehicleID: String(it.vehicleID ?? ""),
      operationID: it.operationID ?? null,
      status: it.status ?? "MOVING",
      startMs: Number(it.startMs),
      endMs: Number(it.endMs),
      laneIndex: Number.isFinite(it.laneIndex) ? it.laneIndex : 0,
      laneCount: Number.isFinite(it.laneCount) && it.laneCount > 0 ? it.laneCount : 1,
      label: it.label ?? "",

      // --- keep dispatch-derived fields (may be undefined for non-IN_SERVICE) ---
      reserveType: it.reserveType ?? "",
      pickupStationID: it.pickupStationID ?? "",
      dropoffStationID: it.dropoffStationID ?? "",
      pickupStationName: it.pickupStationName ?? "",
      dropoffStationName: it.dropoffStationName ?? ""
    }))
  };
}

export async function fetchGantt(date, { signal } = {}) {
  const url = `/api/gantt?date=${encodeURIComponent(date)}`;
  const resp = await fetch(url, { method: "GET", signal });

  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`API error (${resp.status}): ${text || resp.statusText}`);
  }

  const json = await resp.json();
  return normalizePayload(json);
}