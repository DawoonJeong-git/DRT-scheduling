// MVP pooling lane rule (as requested):
// "Same operationID within IN_SERVICE routes: count of rows with dispatchIDs => laneCount."
// laneIndex is assigned by start time order within that operationID.

export function assignLanesByOperationId(intervals) {
  const byOp = new Map();

  for (const it of intervals) {
    if (it.status !== "IN_SERVICE") continue;
    const op = it.operationID ?? "__NO_OP__";
    if (!byOp.has(op)) byOp.set(op, []);
    byOp.get(op).push(it);
  }

  for (const [op, list] of byOp.entries()) {
    list.sort((a, b) => a.startMs - b.startMs);
    const laneCount = Math.max(1, list.length);
    list.forEach((it, idx) => {
      it.laneCount = laneCount;
      it.laneIndex = idx;
    });
  }

  // non in-service defaults
  for (const it of intervals) {
    if (it.status !== "IN_SERVICE") {
      it.laneCount = 1;
      it.laneIndex = 0;
    }
  }

  return intervals;
}