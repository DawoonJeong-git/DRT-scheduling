# server/gantt_builder.py
import json
from datetime import datetime, timezone, timedelta

from .db_client import (
    get_routes_for_day,
    get_operations_catalog,
    get_reservations_by_dispatch_ids,
    get_dispatches_by_dispatch_ids,
)

KST = timezone(timedelta(hours=9))

START_HOUR = 8
END_HOUR = 22
MINUTE_MS = 60_000


def _date_to_yyyymmdd(date_str: str) -> int:
    y, m, d = date_str.split("-")
    return int(f"{y}{m}{d}")


def _day_window_ms(date_str: str):
    y, m, d = map(int, date_str.split("-"))
    day0 = datetime(y, m, d, 0, 0, 0, tzinfo=KST)
    start = day0 + timedelta(hours=START_HOUR)
    end = day0 + timedelta(hours=END_HOUR)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _to_epoch_ms(x):
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "null":
        return None

    s2 = "".join(ch for ch in s if ch.isdigit())
    if s2 == "":
        return None

    if len(s2) in (12, 14) and s2.startswith(("19", "20")):
        fmt = "%Y%m%d%H%M%S" if len(s2) == 14 else "%Y%m%d%H%M"
        dt = datetime.strptime(s2, fmt).replace(tzinfo=KST)
        return int(dt.timestamp() * 1000)

    try:
        n = int(float(s2))
        if 10**9 <= n < 10**11:
            return n * 1000
        if 10**12 <= n < 10**14:
            return n
    except Exception:
        return None

    return None


def _normalize_dispatch_id(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    changed = True
    while changed and len(s) >= 2:
        changed = False
        if (s[0] == "'" and s[-1] == "'") or (s[0] == '"' and s[-1] == '"'):
            s = s[1:-1].strip()
            changed = True
    return s


def _has_dispatch(dispatch_ids):
    if dispatch_ids is None:
        return False
    if isinstance(dispatch_ids, str):
        s = dispatch_ids.strip()
        return s != "" and s != "[]" and s.lower() != "null"
    if isinstance(dispatch_ids, (list, tuple)):
        return len(dispatch_ids) > 0
    return bool(dispatch_ids)


def _parse_dispatch_ids(dispatch_ids):
    if dispatch_ids is None:
        return []

    if isinstance(dispatch_ids, list):
        out = []
        for x in dispatch_ids:
            nx = _normalize_dispatch_id(x)
            if nx:
                out.append(nx)
        return out

    if not isinstance(dispatch_ids, str):
        nx = _normalize_dispatch_id(dispatch_ids)
        return [nx] if nx else []

    s = dispatch_ids.strip()
    if s == "" or s.lower() == "null":
        return []

    if s.startswith("[") and s.endswith("]"):
        try:
            obj = json.loads(s)
            if isinstance(obj, list):
                out = []
                for x in obj:
                    nx = _normalize_dispatch_id(x)
                    if nx:
                        out.append(nx)
                return out
        except Exception:
            pass

    if "," in s:
        out = []
        for p in s.split(","):
            nx = _normalize_dispatch_id(p)
            if nx:
                out.append(nx)
        return out

    nx = _normalize_dispatch_id(s)
    return [nx] if nx else []


def _sum_passengers(res_rows):
    total_p = 0
    total_w = 0
    for r in res_rows:
        p = r.get("passengerCount", 0) or 0
        w = r.get("wheelchairCount", 0) or 0
        try:
            p = int(p)
        except Exception:
            p = 0
        try:
            w = int(w)
        except Exception:
            w = 0
        total_p += p
        total_w += w

    general = max(0, total_p - total_w)
    return general, total_w


def _label_for(vehicle_type: str, general: int, wheelchair: int):
    vt = (vehicle_type or "").strip()
    if vt == "carnivalWheel":
        return f"일반 {general}명 / 휠체어 {wheelchair}명"
    return f"일반 {general}명"


def _pick_dispatch_info(dispatch_rows, rr_rows):
    def first_nonempty_from(rows, key):
        for r in rows:
            v = r.get(key)
            if v is None:
                continue
            s = str(v).strip()
            if s != "" and s.lower() != "null":
                return s
        return ""

    pickupStationName = first_nonempty_from(dispatch_rows, "pickupStationName")
    dropoffStationName = first_nonempty_from(dispatch_rows, "dropoffStationName")
    pickupStationID = first_nonempty_from(dispatch_rows, "pickupStationID")
    dropoffStationID = first_nonempty_from(dispatch_rows, "dropoffStationID")
    reserveType = first_nonempty_from(dispatch_rows, "reserveType")

    if not reserveType:
        reserveType = first_nonempty_from(rr_rows, "reserveType")
    if not pickupStationID:
        pickupStationID = first_nonempty_from(rr_rows, "pickupStationID")
    if not dropoffStationID:
        dropoffStationID = first_nonempty_from(rr_rows, "dropoffStationID")

    return {
        "pickupStationName": pickupStationName,
        "dropoffStationName": dropoffStationName,
        "pickupStationID": pickupStationID,
        "dropoffStationID": dropoffStationID,
        "reserveType": reserveType,
    }


def _merge_intervals(intervals):
    xs = [(s, e) for s, e in intervals if e > s]
    xs.sort(key=lambda t: (t[0], t[1]))

    merged = []
    cur_s = cur_e = None
    for s, e in xs:
        if cur_s is None:
            cur_s, cur_e = s, e
            continue
        if s <= cur_e:
            cur_e = max(cur_e, e)
        else:
            merged.append((cur_s, cur_e))
            cur_s, cur_e = s, e

    if cur_s is not None:
        merged.append((cur_s, cur_e))
    return merged


def _intervals_overlap(a_s, a_e, b_s, b_e):
    return (a_s < b_e) and (b_s < a_e)


def _connected_components_overlaps(items):
    n = len(items)
    adj = [[] for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            if _intervals_overlap(items[i]["s"], items[i]["e"], items[j]["s"], items[j]["e"]):
                adj[i].append(j)
                adj[j].append(i)

    comps = []
    seen = [False] * n
    for i in range(n):
        if seen[i]:
            continue
        stack = [i]
        seen[i] = True
        comp = []
        while stack:
            u = stack.pop()
            comp.append(u)
            for v in adj[u]:
                if not seen[v]:
                    seen[v] = True
                    stack.append(v)
        comps.append(comp)
    return comps


def _minute_floor(ms: int) -> int:
    return (ms // MINUTE_MS) * MINUTE_MS


def _minute_cells_covered(start_ms: int, end_ms: int):
    if end_ms <= start_ms:
        return []
    start_cell = _minute_floor(start_ms)
    end_cell = _minute_floor(end_ms - 1)
    return list(range(start_cell, end_cell + MINUTE_MS, MINUTE_MS))


def _is_single_minute_interval(start_ms: int, end_ms: int) -> bool:
    return len(_minute_cells_covered(start_ms, end_ms)) == 1


def _overlap_ms_with_cell(start_ms: int, end_ms: int, cell_ms: int) -> int:
    cell_end = cell_ms + MINUTE_MS
    return max(0, min(end_ms, cell_end) - max(start_ms, cell_ms))


def _best_candidate_by_overlap(cands, cell_ms):
    if not cands:
        return None
    return max(
        cands,
        key=lambda c: (
            _overlap_ms_with_cell(c["startMs"], c["endMs"], cell_ms),
            c["endMs"] - c["startMs"],
        ),
    )


def _resolve_cell_status(candidates, cell_ms):
    """
    규칙:
    1) 한 셀에는 최종 1개 상태만 남긴다.
    2) 기본 우선순위: IN_SERVICE > MOVING
    3) 단, MOVING이 '1분짜리 interval'이고 IN_SERVICE는 다분 interval이면 MOVING 보호
    4) 단, IN_SERVICE와 MOVING이 둘 다 같은 1분 내 완료(single-minute interval)면 IN_SERVICE만 표출
    """
    in_cands = [c for c in candidates if c["status"] == "IN_SERVICE"]
    mv_cands = [c for c in candidates if c["status"] == "MOVING"]

    if in_cands and not mv_cands:
        return _best_candidate_by_overlap(in_cands, cell_ms)

    if mv_cands and not in_cands:
        return _best_candidate_by_overlap(mv_cands, cell_ms)

    if in_cands and mv_cands:
        in_single = any(c["singleMinute"] for c in in_cands)
        mv_single = any(c["singleMinute"] for c in mv_cands)
        in_multi = any(not c["singleMinute"] for c in in_cands)
        mv_multi = any(not c["singleMinute"] for c in mv_cands)

        # 둘 다 같은 1분 안 완료 -> IN_SERVICE만 표출
        if in_single and mv_single and not in_multi and not mv_multi:
            return _best_candidate_by_overlap(in_cands, cell_ms)

        # MOVING만 1분짜리이고 IN_SERVICE는 다분 구간 -> MOVING 보호
        if mv_single and in_multi:
            return _best_candidate_by_overlap(mv_cands, cell_ms)

        # 나머지는 기본 우선순위
        return _best_candidate_by_overlap(in_cands, cell_ms)

    return None


def _build_operations(routes, route_dispatch_map, rr_by_dispatch, dispatch_by_dispatch, win_start, win_end):
    ops = {}

    for idx, r in enumerate(routes):
        vehicle_id = str(r.get("vehicleID") or r.get("op_vehicleID") or "").strip()
        if not vehicle_id:
            continue

        op_id = str(r.get("operationID") or "").strip()
        if not op_id:
            continue

        s_raw = _to_epoch_ms(r.get("originDeptTime"))
        e_raw = _to_epoch_ms(r.get("destArrivalTime"))
        if not s_raw or not e_raw or e_raw <= s_raw:
            continue

        s = max(win_start, s_raw)
        e = min(win_end, e_raw)
        if e <= s:
            continue

        key = (vehicle_id, op_id)
        if key not in ops:
            ops[key] = {
                "vehicleID": vehicle_id,
                "operationID": op_id,
                "vehicleType": (r.get("vehicleType") or "").strip(),
                "total_s": s,
                "total_e": e,
                "svc_segments": [],
                "mov_segments": [],
            }
        else:
            ops[key]["total_s"] = min(ops[key]["total_s"], s)
            ops[key]["total_e"] = max(ops[key]["total_e"], e)

        if _has_dispatch(r.get("dispatchIDs")):
            dispatch_ids = route_dispatch_map[idx]

            merged_rr = []
            merged_dispatch = []
            for did in dispatch_ids:
                nd = _normalize_dispatch_id(did)
                merged_rr.extend(rr_by_dispatch.get(nd, []))
                merged_dispatch.extend(dispatch_by_dispatch.get(nd, []))

            general, wheelchair = _sum_passengers(merged_rr)
            label = _label_for(ops[key]["vehicleType"], general, wheelchair)
            dinfo = _pick_dispatch_info(merged_dispatch, merged_rr)

            ops[key]["svc_segments"].append(
                {
                    "startMs": s,
                    "endMs": e,
                    "label": label,
                    "dispatch_info": dinfo,
                    "dispatchIDs": dispatch_ids,
                    "sourceKey": f"{vehicle_id}|{op_id}|svc|{len(ops[key]['svc_segments'])}",
                }
            )
        else:
            ops[key]["mov_segments"].append(
                {
                    "startMs": s,
                    "endMs": e,
                    "sourceKey": f"{vehicle_id}|{op_id}|mov|{len(ops[key]['mov_segments'])}",
                }
            )

    return ops


def _append_interval_from_cells(intervals, vehicle_id, status, start_cell, end_cell_exclusive, meta=None):
    if end_cell_exclusive <= start_cell:
        return

    meta = meta or {}
    item = {
        "vehicleID": vehicle_id,
        "operationID": meta.get("operationID"),
        "status": status,
        "startMs": start_cell,
        "endMs": end_cell_exclusive,
        "laneIndex": 0,
        "laneCount": 1,
        "label": meta.get("label", ""),
    }

    if status == "IN_SERVICE":
        dinfo = meta.get("dispatch_info") or {}
        item.update(
            {
                "pickupStationName": dinfo.get("pickupStationName", ""),
                "dropoffStationName": dinfo.get("dropoffStationName", ""),
                "pickupStationID": dinfo.get("pickupStationID", ""),
                "dropoffStationID": dinfo.get("dropoffStationID", ""),
                "reserveType": dinfo.get("reserveType", ""),
            }
        )

    intervals.append(item)


def _build_component_cell_map(vehicle_id, comp_ops, drive_start, drive_end):
    """
    1) raw IN_SERVICE / MOVING interval을 minute cell로 펼침
    2) 같은 셀에서 충돌 해소
    3) drive range 내 비어 있는 셀은 BOARDING
    """
    cell_candidates = {}

    for d in comp_ops:
        op_id = d["operationID"]

        for seg in d.get("svc_segments", []):
            s = seg["startMs"]
            e = seg["endMs"]
            single = _is_single_minute_interval(s, e)
            for cell in _minute_cells_covered(s, e):
                cell_candidates.setdefault(cell, []).append(
                    {
                        "status": "IN_SERVICE",
                        "startMs": s,
                        "endMs": e,
                        "singleMinute": single,
                        "sourceKey": seg["sourceKey"],
                        "operationID": op_id,
                        "label": seg.get("label", ""),
                        "dispatch_info": seg.get("dispatch_info", {}),
                    }
                )

        for seg in d.get("mov_segments", []):
            s = seg["startMs"]
            e = seg["endMs"]
            single = _is_single_minute_interval(s, e)
            for cell in _minute_cells_covered(s, e):
                cell_candidates.setdefault(cell, []).append(
                    {
                        "status": "MOVING",
                        "startMs": s,
                        "endMs": e,
                        "singleMinute": single,
                        "sourceKey": seg["sourceKey"],
                        "operationID": op_id,
                        "label": "",
                    }
                )

    resolved = {}
    drive_cells = _minute_cells_covered(drive_start, drive_end)

    for cell in drive_cells:
        cands = cell_candidates.get(cell, [])
        chosen = _resolve_cell_status(cands, cell) if cands else None

        if chosen is not None:
            resolved[cell] = {
                "status": chosen["status"],
                "meta": {
                    "operationID": chosen.get("operationID"),
                    "label": chosen.get("label", ""),
                    "dispatch_info": chosen.get("dispatch_info", {}),
                    "sourceKey": chosen.get("sourceKey"),
                },
            }
        else:
            resolved[cell] = {
                "status": "BOARDING",
                "meta": {
                    "operationID": None,
                    "label": "",
                    "sourceKey": f"{vehicle_id}|boarding",
                },
            }

    return resolved


def _cells_to_intervals(vehicle_id, resolved_cells):
    """
    minute cell 결과를 연속 interval로 재구성.
    IN_SERVICE는 sourceKey가 같을 때만 merge.
    MOVING / BOARDING / AVAILABLE은 status 같으면 merge.
    """
    intervals = []
    if not resolved_cells:
        return intervals

    cells = sorted(resolved_cells.keys())
    cur_start = None
    cur_end = None
    cur_status = None
    cur_meta = None
    cur_group_key = None

    def flush():
        if cur_start is None:
            return
        _append_interval_from_cells(
            intervals=intervals,
            vehicle_id=vehicle_id,
            status=cur_status,
            start_cell=cur_start,
            end_cell_exclusive=cur_end,
            meta=cur_meta,
        )

    for cell in cells:
        status = resolved_cells[cell]["status"]
        meta = resolved_cells[cell]["meta"]

        if status == "IN_SERVICE":
            group_key = ("IN_SERVICE", meta.get("sourceKey"))
        else:
            group_key = (status, None)

        if cur_start is None:
            cur_start = cell
            cur_end = cell + MINUTE_MS
            cur_status = status
            cur_meta = meta
            cur_group_key = group_key
            continue

        if cell == cur_end and group_key == cur_group_key:
            cur_end = cell + MINUTE_MS
        else:
            flush()
            cur_start = cell
            cur_end = cell + MINUTE_MS
            cur_status = status
            cur_meta = meta
            cur_group_key = group_key

    flush()
    return intervals


def build_gantt_payload(date_str: str):
    date_yyyymmdd = _date_to_yyyymmdd(date_str)
    win_start, win_end = _day_window_ms(date_str)

    ops_catalog = get_operations_catalog()
    vehicles = []
    seen = set()
    for o in ops_catalog:
        vid = str(o.get("vehicleID") or "").strip()
        if not vid or vid in seen:
            continue
        seen.add(vid)
        vehicles.append(
            {
                "vehicleID": vid,
                "vehicleType": o.get("vehicleType") or "",
                "operationServiceType": o.get("operationServiceType") or "",
            }
        )
    vehicles.sort(key=lambda x: x["vehicleID"])

    routes = get_routes_for_day(date_yyyymmdd)

    all_dispatch_ids = []
    route_dispatch_map = []
    for r in routes:
        if not _has_dispatch(r.get("dispatchIDs")):
            route_dispatch_map.append([])
            continue
        dlist = _parse_dispatch_ids(r.get("dispatchIDs"))
        route_dispatch_map.append(dlist)
        all_dispatch_ids.extend(dlist)

    uniq_dispatch = []
    seen_d = set()
    for did in all_dispatch_ids:
        nd = _normalize_dispatch_id(did)
        if not nd or nd in seen_d:
            continue
        seen_d.add(nd)
        uniq_dispatch.append(nd)

    rr_rows = get_reservations_by_dispatch_ids(uniq_dispatch)
    rr_by_dispatch = {}
    for rr in rr_rows:
        did = _normalize_dispatch_id(rr.get("dispatchID"))
        if did:
            rr_by_dispatch.setdefault(did, []).append(rr)

    d_rows = get_dispatches_by_dispatch_ids(uniq_dispatch)
    dispatch_by_dispatch = {}
    for dr in d_rows:
        did = _normalize_dispatch_id(dr.get("dispatchID"))
        if did:
            dispatch_by_dispatch.setdefault(did, []).append(dr)

    op_map = _build_operations(
        routes,
        route_dispatch_map,
        rr_by_dispatch,
        dispatch_by_dispatch,
        win_start,
        win_end,
    )

    ops_by_vehicle = {}
    for (vid, _opid), d in op_map.items():
        ops_by_vehicle.setdefault(vid, []).append(d)

    intervals = []

    for v in vehicles:
        vid = v["vehicleID"]
        op_list = ops_by_vehicle.get(vid, [])

        # vehicle 전체 minute-cell 결과
        vehicle_cells = {}

        if op_list:
            items = [{"s": d["total_s"], "e": d["total_e"], "ref": d} for d in op_list]
            comps = _connected_components_overlaps(items)

            for comp in comps:
                comp_ops = [items[i]["ref"] for i in comp]
                drive_start = min(d["total_s"] for d in comp_ops)
                drive_end = max(d["total_e"] for d in comp_ops)

                comp_cells = _build_component_cell_map(
                    vehicle_id=vid,
                    comp_ops=comp_ops,
                    drive_start=drive_start,
                    drive_end=drive_end,
                )
                vehicle_cells.update(comp_cells)

        # AVAILABLE 채우기: window 안에서 아직 배정 안 된 minute cell
        for cell in _minute_cells_covered(win_start, win_end):
            if cell not in vehicle_cells:
                vehicle_cells[cell] = {
                    "status": "AVAILABLE",
                    "meta": {
                        "operationID": None,
                        "label": "",
                        "sourceKey": f"{vid}|available",
                    },
                }

        intervals.extend(_cells_to_intervals(vid, vehicle_cells))

    intervals = [it for it in intervals if it["endMs"] > it["startMs"]]

    return {
        "date": date_str,
        "updatedAtMs": int(datetime.now(tz=KST).timestamp() * 1000),
        "timeWindow": {"start": "08:00", "end": "22:00"},
        "vehicles": vehicles,
        "intervals": intervals,
        "debug": {
            "routes": len(routes),
            "intervals": len(intervals),
            "dispatch_ids": len(uniq_dispatch),
            "dispatch_rows": len(d_rows),
            "reservation_rows": len(rr_rows),
            "ops": len(op_map),
        },
    }