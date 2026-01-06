import os
from sqlalchemy import text
from sqlalchemy.orm import Session

T_CT_INFO = "g_production_film.ct_info"

def require_admin(password: str):
    admin_pw = os.getenv("ADMIN_PASSWORD", "")
    if not admin_pw:
        raise RuntimeError("ADMIN_PASSWORD 환경변수가 설정되지 않았습니다.")
    if password != admin_pw:
        raise PermissionError("Invalid admin password.")

def upsert_ct_map_row(db: Session, password: str, key_char: str, pn: str, ct: float):
    """
    Front에서 pn/ct 수정 가능(비번 필요) 요구사항 대응. :contentReference[oaicite:3]{index=3}
    테이블 키 구조는 현장마다 다를 수 있어, 여기서는 (key_char) unique가 있다고 가정한 upsert 예시.
    실제 컬럼명이 다르면 여기만 바꾸면 됨.
    """
    require_admin(password)

    # 아래는 "key_char" 컬럼이 있다고 가정한 예시. 없으면 ct_info 테이블 구조에 맞춰 수정 필요.
    sql = text(f"""
        INSERT INTO {T_CT_INFO} (key_char, pn, ct)
        VALUES (:key_char, :pn, :ct)
        ON CONFLICT (key_char) DO UPDATE
        SET pn = EXCLUDED.pn,
            ct = EXCLUDED.ct
    """)
    db.execute(sql, {"key_char": key_char, "pn": pn, "ct": ct})
    db.commit()
