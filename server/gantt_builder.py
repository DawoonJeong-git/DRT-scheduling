# server/gantt_builder.py
import os
import json
from datetime import datetime, timezone, timedelta

from db_client import (
    get_routes_for_day,
    get_operations_catalog,
    get_reservations_by_dispatch_ids,
    get_dispatches_by_dispatch_ids,
)

KST = timezone(timedelta(hours=9))

START_HOUR = 8
END_HOUR = 22


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
    """
    dispatch 테이블 우선:
      - pickupStationName/dropoffStationName 있음
      - reserveType 있음
    reservation_request는 예비(혹시 dispatch row가 없을 때 ID/reserveType만이라도)
    """
    def first_nonempty_from(rows, key):
        for r in rows:
            v = r.get(key)
            if v is None:
                continue
            s = str(v).strip()
            if s != "" and s.lower() != "null":
                return s
        return ""

    # dispatch 우선
    pickupStationName = first_nonempty_from(dispatch_rows, "pickupStationName")
    dropoffStationName = first_nonempty_from(dispatch_rows, "dropoffStationName")
    pickupStationID = first_nonempty_from(dispatch_rows, "pickupStationID")
    dropoffStationID = first_nonempty_from(dispatch_rows, "dropoffStationID")
    reserveType = first_nonempty_from(dispatch_rows, "reserveType")

    # fallback to reservation_request if still empty
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


def _complement_in_range(range_s, range_e, occ_merged):
    gaps = []
    cur = range_s
    for s, e in occ_merged:
        s = max(range_s, s)
        e = min(range_e, e)
        if e <= range_s or s >= range_e:
            continue
        if s > cur:
            gaps.append((cur, s))
        cur = max(cur, e)
    if cur < range_e:
        gaps.append((cur, range_e))
    return [(s, e) for s, e in gaps if e > s]


def _subtract_merged(base_s, base_e, cut_merged):
    pieces = [(base_s, base_e)]
    for cs, ce in cut_merged:
        new_p = []
        for ps, pe in pieces:
            if pe <= cs or ps >= ce:
                new_p.append((ps, pe))
                continue
            if ps < cs:
                new_p.append((ps, cs))
            if pe > ce:
                new_p.append((ce, pe))
        pieces = new_p
        if not pieces:
            break
    return [(s, e) for s, e in pieces if e > s]


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


def _build_operations(routes, route_dispatch_map, rr_by_dispatch, dispatch_by_dispatch, win_start, win_end):
    ops = {}  # (vehicleID, operationID) -> dict

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
                "svc_segments": [],  # (s,e,label)
                "mov_segments": [],  # (s,e)
            }
        else:
            ops[key]["total_s"] = min(ops[key]["total_s"], s)
            ops[key]["total_e"] = max(ops[key]["total_e"], e)

        if _has_dispatch(r.get("dispatchIDs")):
            dispatch_ids = route_dispatch_map[idx]

            # passenger label은 reservation_request로
            merged_rr = []
            merged_dispatch = []
            for did in dispatch_ids:
                nd = _normalize_dispatch_id(did)
                merged_rr.extend(rr_by_dispatch.get(nd, []))
                merged_dispatch.extend(dispatch_by_dispatch.get(nd, []))

            general, wheelchair = _sum_passengers(merged_rr)
            label = _label_for(ops[key]["vehicleType"], general, wheelchair)

            dinfo = _pick_dispatch_info(merged_dispatch, merged_rr)

            # pooling에서도 비어있는 dinfo로 고정되지 않게: "값이 있으면 갱신"
            prev = ops[key].get("dispatch_info") or {}
            prev_has = any(str(prev.get(k, "")).strip() for k in dinfo.keys())
            new_has = any(str(dinfo.get(k, "")).strip() for k in dinfo.keys())

            if (not prev_has) and new_has:
                ops[key]["dispatch_info"] = dinfo
            else:
                ops[key].setdefault("dispatch_info", dinfo)

            ops[key]["svc_segments"].append((s, e, label))
        else:
            ops[key]["mov_segments"].append((s, e))

    for _, d in ops.items():
        d["svc_merged"] = _merge_intervals([(s, e) for s, e, _ in d["svc_segments"]])
        d["mov_merged"] = _merge_intervals(d["mov_segments"])
        d["label_rep"] = d["svc_segments"][0][2] if d["svc_segments"] else ""
        d.setdefault(
            "dispatch_info",
            {
                "pickupStationName": "",
                "dropoffStationName": "",
                "pickupStationID": "",
                "dropoffStationID": "",
                "reserveType": "",
            },
        )

    return ops


def _op_inservice_span(d):
    svc = d.get("svc_merged") or []
    if not svc:
        return None
    s = min(x[0] for x in svc)
    e = max(x[1] for x in svc)
    if e <= s:
        return None
    return (s, e)


def _assign_fixed_lanes_for_spans(spans):
    spans = [x for x in spans if x["e"] > x["s"]]
    spans.sort(key=lambda x: (x["s"], x["e"]))

    lanes_end = []
    for it in spans:
        s, e = it["s"], it["e"]
        best_lane = None
        best_end = None
        for li, lend in enumerate(lanes_end):
            if lend <= s:
                if best_end is None or lend < best_end:
                    best_end = lend
                    best_lane = li
        if best_lane is None:
            best_lane = len(lanes_end)
            lanes_end.append(e)
        else:
            lanes_end[best_lane] = e
        it["laneIndex"] = best_lane

    lane_count = max(1, len(lanes_end))
    for it in spans:
        it["laneCount"] = lane_count
    return spans


def build_gantt_payload(date_str: str):
    from db_client import DB_SCHEMA
    schema = DB_SCHEMA
    date_yyyymmdd = _date_to_yyyymmdd(date_str)
    win_start, win_end = _day_window_ms(date_str)

    ops_catalog = get_operations_catalog(schema)
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

    routes = get_routes_for_day(schema, date_yyyymmdd)

    all_dispatch_ids = []
    route_dispatch_map = []
    for r in routes:
        if not _has_dispatch(r.get("dispatchIDs")):
            route_dispatch_map.append([])
            continue
        dlist = _parse_dispatch_ids(r.get("dispatchIDs"))
        route_dispatch_map.append(dlist)
        all_dispatch_ids.extend(dlist)

    # normalize + unique
    uniq_dispatch = []
    seen_d = set()
    for did in all_dispatch_ids:
        nd = _normalize_dispatch_id(did)
        if not nd or nd in seen_d:
            continue
        seen_d.add(nd)
        uniq_dispatch.append(nd)

    # 1) reservation_request rows (passengers)
    rr_rows = get_reservations_by_dispatch_ids(schema, uniq_dispatch)
    rr_by_dispatch = {}
    for rr in rr_rows:
        did = _normalize_dispatch_id(rr.get("dispatchID"))
        if did:
            rr_by_dispatch.setdefault(did, []).append(rr)

    # 2) dispatch rows (station names + reserveType)
    d_rows = get_dispatches_by_dispatch_ids(schema, uniq_dispatch)
    dispatch_by_dispatch = {}
    for dr in d_rows:
        did = _normalize_dispatch_id(dr.get("dispatchID"))
        if did:
            dispatch_by_dispatch.setdefault(did, []).append(dr)

    # operations
    op_map = _build_operations(routes, route_dispatch_map, rr_by_dispatch, dispatch_by_dispatch, win_start, win_end)

    ops_by_vehicle = {}
    for (vid, opid), d in op_map.items():
        ops_by_vehicle.setdefault(vid, []).append(d)

    intervals = []
    drive_ranges_by_vehicle = {v["vehicleID"]: [] for v in vehicles}

    for v in vehicles:
        vid = v["vehicleID"]
        op_list = ops_by_vehicle.get(vid, [])
        if not op_list:
            continue

        items = [{"s": d["total_s"], "e": d["total_e"], "ref": d} for d in op_list]
        comps = _connected_components_overlaps(items)

        for comp in comps:
            comp_ops = [items[i]["ref"] for i in comp]
            drive_start = min(d["total_s"] for d in comp_ops)
            drive_end = max(d["total_e"] for d in comp_ops)
            drive_ranges_by_vehicle[vid].append((drive_start, drive_end))

            drive_inservice_merged = _merge_intervals([iv for d in comp_ops for iv in d["svc_merged"]])

            drive_moving_merged = _merge_intervals([iv for d in comp_ops for iv in d["mov_merged"]])
            moving_no_overlap = []
            for ms, me in drive_moving_merged:
                moving_no_overlap.extend(_subtract_merged(ms, me, drive_inservice_merged))
            drive_moving_merged = _merge_intervals(moving_no_overlap)

            # MOVING
            for s, e in drive_moving_merged:
                intervals.append(
                    {
                        "vehicleID": vid,
                        "operationID": None,
                        "status": "MOVING",
                        "startMs": s,
                        "endMs": e,
                        "laneIndex": 0,
                        "laneCount": 1,
                        "label": "",
                    }
                )

            # BOARDING = drive range - (MOVING ∪ IN_SERVICE)
            occ = _merge_intervals(drive_inservice_merged + drive_moving_merged)
            boarding = _merge_intervals(_complement_in_range(drive_start, drive_end, occ))
            for s, e in boarding:
                intervals.append(
                    {
                        "vehicleID": vid,
                        "operationID": None,
                        "status": "BOARDING",
                        "startMs": s,
                        "endMs": e,
                        "laneIndex": 0,
                        "laneCount": 1,
                        "label": "",
                    }
                )

            # IN_SERVICE: one bar per operationID + fixed lanes
            spans = []
            for d in comp_ops:
                span = _op_inservice_span(d)
                if not span:
                    continue
                s, e = span
                spans.append(
                    {
                        "opKey": (vid, d["operationID"]),
                        "s": s,
                        "e": e,
                        "label": d.get("label_rep") or "",
                        "dispatch_info": d.get("dispatch_info") or {},
                    }
                )

            spans = _assign_fixed_lanes_for_spans(spans)
            for it in spans:
                opid = it["opKey"][1]
                dinfo = it.get("dispatch_info") or {}
                intervals.append(
                    {
                        "vehicleID": vid,
                        "operationID": opid,
                        "status": "IN_SERVICE",
                        "startMs": it["s"],
                        "endMs": it["e"],
                        "laneIndex": it["laneIndex"],
                        "laneCount": it["laneCount"],
                        "label": it["label"],

                        # ✅ tooltip fields (from dispatch table)
                        "pickupStationName": dinfo.get("pickupStationName", ""),
                        "dropoffStationName": dinfo.get("dropoffStationName", ""),
                        "pickupStationID": dinfo.get("pickupStationID", ""),
                        "dropoffStationID": dinfo.get("dropoffStationID", ""),
                        "reserveType": dinfo.get("reserveType", ""),
                    }
                )

    # AVAILABLE: day window - union(all drive ranges)
    for v in vehicles:
        vid = v["vehicleID"]
        drives = _merge_intervals(drive_ranges_by_vehicle.get(vid, []))
        gaps = _complement_in_range(win_start, win_end, drives)
        for s, e in gaps:
            intervals.append(
                {
                    "vehicleID": vid,
                    "operationID": None,
                    "status": "AVAILABLE",
                    "startMs": s,
                    "endMs": e,
                    "laneIndex": 0,
                    "laneCount": 1,
                    "label": "",
                }
            )

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


