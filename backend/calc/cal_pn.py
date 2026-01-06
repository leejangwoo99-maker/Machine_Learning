def key_char_from_barcode(barcode_information: str) -> str | None:
    if not barcode_information:
        return None
    # "18번째 자리 문자" = index 17 :contentReference[oaicite:4]{index=4}
    return barcode_information[17] if len(barcode_information) > 17 else None

def apply_pn_ct(rows: list[dict], pn_ct_map: dict[str, tuple[str, float]]) -> list[dict]:
    """
    pn_ct_map: key_char -> (pn, ct)
    """
    out = []
    for r in rows:
        bi = r.get("barcode_information") or ""
        k = key_char_from_barcode(bi)
        pn, ct = pn_ct_map.get(k, (None, None))
        rr = dict(r)
        rr["key_char"] = k
        rr["pn"] = pn
        rr["ct"] = ct
        out.append(rr)
    return out

def normalize_ct_map(ct_rows: list[dict]) -> dict[str, tuple[str, float]]:
    """
    ct_info 테이블 컬럼명이 확정되지 않았을 수 있으므로,
    row에서 (key_char, pn, ct)를 최대한 찾아 매핑을 만든다.
    """
    out: dict[str, tuple[str, float]] = {}
    for r in ct_rows:
        # 후보 컬럼명들
        k = r.get("key_char") or r.get("barcode_key") or r.get("barcode_information") or r.get("key")
        pn = r.get("pn") or r.get("product_number")
        ct = r.get("ct") or r.get("cycle_time")

        if k is None:
            continue

        # barcode_information이 들어온 경우(문자열 길면 18번째 문자로 축약)
        if isinstance(k, str) and len(k) > 1:
            k2 = k[17] if len(k) > 17 else None
            if k2:
                k = k2

        try:
            ct_f = float(ct) if ct is not None else None
        except Exception:
            ct_f = None

        if isinstance(k, str) and len(k) == 1 and pn and ct_f is not None:
            out[k] = (str(pn), ct_f)
    return out
