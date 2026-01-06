# dbcore/dbsql.py
from sqlalchemy import text
from sqlalchemy.orm import Session

# ====== 테이블 경로(설계서 기준) ======
T_VISION_HIST = "a1_fct_vision_testlog_txt_processing_history.fct_vision_testlog_txt_processing_history"
T_FCT_TABLE   = "a2_fct_table.fct_table"
T_VISION_TABLE = "a3_vision_table.vision_table"
T_CT_INFO     = "g_production_film.ct_info"

# ★ 스크린샷 기준: d1_machine_log 스키마 안 테이블명이 "Main_machine_log" (대문자 M)
# PostgreSQL에서 대소문자 유지 테이블은 반드시 " " 로 감싸야 합니다.
T_MAIN_LOG    = 'd1_machine_log."Main_machine_log"'


def select_ct_map(db: Session) -> list[dict]:
    """
    ct_info에서 (key_char -> pn, ct) 매핑 데이터를 읽는다.
    컬럼 구조는 현장마다 다를 수 있어 전체 row를 가져오고,
    실제 매핑 구성은 calc.cal_pn.normalize_ct_map()에서 유연하게 처리한다.
    """
    sql = text(f"SELECT * FROM {T_CT_INFO}")
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def select_vision_history_for_prod_day(db: Session, prod_day: str, next_day: str) -> list[dict]:
    """
    Vision1~2 생산기준(주/야) 범위의 로그를 로드한다.
    - prod_day 08:30~23:59
    - next_day 00:00~08:29
    """
    sql = text(f"""
        SELECT end_day, end_time, station, result, barcode_information
        FROM {T_VISION_HIST}
        WHERE station IN ('Vision1','Vision2')
          AND (
                (end_day = :prod_day AND end_time >= '08:30:00' AND end_time <= '23:59:59')
             OR (end_day = :next_day AND end_time >= '00:00:00' AND end_time <  '08:30:00')
          )
    """)
    rows = db.execute(sql, {"prod_day": prod_day, "next_day": next_day}).fetchall()
    return [dict(r._mapping) for r in rows]


def select_yield_sources_for_prod_day(db: Session, prod_day: str, next_day: str) -> list[dict]:
    """
    양품률 산출용 source:
    station IN ('FCT1','FCT2','FCT3','FCT4','Vision1','Vision2')
    """
    sql = text(f"""
        SELECT end_day, end_time, station, result
        FROM {T_VISION_HIST}
        WHERE station IN ('FCT1','FCT2','FCT3','FCT4','Vision1','Vision2')
          AND (
                (end_day = :prod_day AND end_time >= '08:30:00' AND end_time <= '23:59:59')
             OR (end_day = :next_day AND end_time >= '00:00:00' AND end_time <  '08:30:00')
          )
    """)
    rows = db.execute(sql, {"prod_day": prod_day, "next_day": next_day}).fetchall()
    return [dict(r._mapping) for r in rows]


def select_fct_fail_rows_for_prod_day(db: Session, prod_day: str, next_day: str) -> list[dict]:
    """
    FCT FAIL 분석: result='FAIL' row만 로드
    """
    sql = text(f"""
        SELECT end_day, end_time, station, result, barcode_information, step_description
        FROM {T_FCT_TABLE}
        WHERE result = 'FAIL'
          AND (
                (end_day = :prod_day AND end_time >= '08:30:00' AND end_time <= '23:59:59')
             OR (end_day = :next_day AND end_time >= '00:00:00' AND end_time <  '08:30:00')
          )
    """)
    rows = db.execute(sql, {"prod_day": prod_day, "next_day": next_day}).fetchall()
    return [dict(r._mapping) for r in rows]


def select_vision_fail_rows_for_prod_day(db: Session, prod_day: str, next_day: str) -> list[dict]:
    """
    Vision FAIL 분석: result='FAIL' row만 로드
    """
    sql = text(f"""
        SELECT end_day, end_time, station, result, barcode_information, step_description
        FROM {T_VISION_TABLE}
        WHERE result = 'FAIL'
          AND (
                (end_day = :prod_day AND end_time >= '08:30:00' AND end_time <= '23:59:59')
             OR (end_day = :next_day AND end_time >= '00:00:00' AND end_time <  '08:30:00')
          )
    """)
    rows = db.execute(sql, {"prod_day": prod_day, "next_day": next_day}).fetchall()
    return [dict(r._mapping) for r in rows]


def select_mastersample_window(db: Session, prod_day: str, next_day: str) -> bool:
    """
    Main_machine_log에서 'Mastersample All OK' 메시지가
    prod_day~next_day 주/야 window 범위 내에 존재하면 True.

    주의:
    - 테이블명이 "Main_machine_log" 형태(대소문자 포함)이므로 반드시 따옴표로 감싸야 함.
    - 쿼리 실패 후 fallback 실행 전에 rollback을 해야 트랜잭션 중지 상태를 피할 수 있음.
    """
    try:
        sql = text(f"""
            SELECT 1
            FROM {T_MAIN_LOG}
            WHERE contents LIKE '%Mastersample All OK%'
              AND (
                    (end_day = :prod_day AND end_time >= '08:30:00' AND end_time <= '23:59:59')
                 OR (end_day = :next_day AND end_time >= '00:00:00' AND end_time <  '08:30:00')
              )
            LIMIT 1
        """)
        v = db.execute(sql, {"prod_day": prod_day, "next_day": next_day}).scalar()
        return bool(v)

    except Exception:
        # ★ 핵심: 실패한 트랜잭션을 정리하지 않으면 다음 쿼리도 InFailedSqlTransaction 으로 같이 죽음
        db.rollback()

        # fallback: 시간 컬럼이 없거나 컬럼명이 다르면 메시지 존재 여부만 확인
        sql = text(f"""
            SELECT 1
            FROM {T_MAIN_LOG}
            WHERE contents LIKE '%Mastersample All OK%'
            LIMIT 1
        """)
        v = db.execute(sql).scalar()
        return bool(v)
