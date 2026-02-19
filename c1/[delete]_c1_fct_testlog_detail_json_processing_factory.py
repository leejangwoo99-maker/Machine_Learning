import re
from pathlib import Path
from datetime import datetime
import time
from multiprocessing import Pool, freeze_support

import psycopg2

# =========================
# 기본 경로 / DB 설정
# =========================
BASE_LOG_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\FCT")

DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

# a. 비전 JSON 테이블 (station, result 매핑용)
SCHEMA_VISION = "a2_fct_table"
TABLE_VISION = "fct_table"

# 처리 이력 테이블
SCHEMA_PROCESSING = "c1_fct_testlog_detail_jason_processing"
TABLE_PROCESSING = "fct_testlog_detail_jason_processing"

# b. 결과 테이블
SCHEMA_RESULT = "c1_fct_testlog_detail_result"
TABLE_RESULT = "fct_testlog_detail_result"

NUM_WORKERS = 2


# =====================================================================
# 1. 로그 파싱 유틸
# =====================================================================
def read_lines_with_encodings(file_path: Path):
    for enc in ("cp949", "utf-8", "utf-8-sig"):
        try:
            with file_path.open("r", encoding=enc) as f:
                return f.readlines()
        except UnicodeDecodeError:
            continue
    with file_path.open("r", encoding="utf-8", errors="ignore") as f:
        return f.readlines()


def extract_yyyymmdd_from_name(name: str) -> str:
    candidates = re.findall(r"(20\d{6})", name)
    if candidates:
        return candidates[-1]

    dash_pos = name.find("-")
    if dash_pos != -1:
        underscore_pos = name.find("_", dash_pos + 1)
        if underscore_pos != -1 and underscore_pos > dash_pos + 1:
            candidate = name[dash_pos + 1:underscore_pos]
            if candidate.isdigit() and len(candidate) == 8:
                return candidate
    return ""


def parse_filename(filepath: Path):
    name = filepath.stem
    if "_" in name:
        barcode = name.split("_", 1)[0]
    else:
        barcode = name
    yyyymmdd = extract_yyyymmdd_from_name(name)
    return barcode, yyyymmdd


def parse_time_line(line: str):
    m = re.search(r"\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s+(.*)", line)
    if not m:
        return None, None, None
    time_str = m.group(1)
    raw = m.group(2)
    test_item = re.sub(r"\s{2,}", " ", raw).strip()
    return time_str, test_item, time_str


def time_to_seconds(time_str: str) -> float:
    t = datetime.strptime(time_str, "%H:%M:%S.%f")
    return t.hour * 3600 + t.minute * 60 + t.second + t.microsecond / 1_000_000


def get_end_time_str(last_time_str: str) -> str:
    t = datetime.strptime(last_time_str, "%H:%M:%S.%f")
    return t.strftime("%H:%M:%S")


def is_valid_fct_path(p: Path) -> bool:
    # """
    # BASE_LOG_DIR 기준으로
    #   FCT\YYYY\MM\DD\...\*.txt 구조만 허용.
    # 예) C:\...\FCT\2025\10\01\FCT1\xxxx.txt  → OK
    #     C:\...\FCT\2025\10\20251022_FCT4... → 제외
    # """
    try:
        rel = p.relative_to(BASE_LOG_DIR)
    except ValueError:
        return False

    parts = rel.parts  # ('2025','10','01','FCT1','file.txt') 등
    if len(parts) < 4:
        return False

    year, month, day = parts[0], parts[1], parts[2]
    if not (year.isdigit() and len(year) == 4):
        return False
    if not (month.isdigit() and len(month) == 2):
        return False
    if not (day.isdigit() and len(day) == 2):
        return False

    return True


def process_one_file(file_path_str: str):
    """
    한 파일 파싱 → (file_path, rows, error)

    rows 컬럼:
      - file_path
      - end_day (YYYYMMDD)
      - end_time (HH:MM:SS)
      - barcode_information
      - test_item
      - test_time
      - test_item_ct
    """
    file_path = Path(file_path_str)
    try:
        barcode, yyyymmdd = parse_filename(file_path)
        lines = read_lines_with_encodings(file_path)

        events = []
        for line in lines:
            time_str, test_item, test_time = parse_time_line(line)
            if time_str is None:
                continue
            events.append((time_str, test_item, test_time))

        if not events:
            return file_path_str, [], None

        last_time_str = events[-1][0]
        end_time = get_end_time_str(last_time_str)

        rows = []
        prev_sec = None
        for time_str, test_item, test_time in events:
            cur_sec = time_to_seconds(time_str)
            if prev_sec is None:
                ct_value = None
            else:
                diff = cur_sec - prev_sec
                if diff < 0:
                    diff += 24 * 3600
                ct_value = round(diff, 2)
            prev_sec = cur_sec

            rows.append(
                {
                    "file_path": file_path_str,
                    "end_day": yyyymmdd,
                    "end_time": end_time,
                    "barcode_information": barcode,
                    "test_item": test_item,
                    "test_time": test_time,
                    "test_item_ct": ct_value,
                }
            )

        return file_path_str, rows, None

    except Exception as e:
        return file_path_str, [], str(e)


# =====================================================================
# 2. PostgreSQL 유틸
# =====================================================================
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema_and_tables(conn):
    """
    - 처리 이력 스키마/테이블 생성
    - 결과 스키마/테이블 생성 + 컬럼 구조 보정
    """
    with conn.cursor() as cur:
        # 스키마
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_PROCESSING};")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RESULT};")

        # 처리 이력 테이블
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_PROCESSING}.{TABLE_PROCESSING} (
                id BIGSERIAL PRIMARY KEY,
                file_path TEXT UNIQUE,
                processed_time TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            );
            """
        )

        # 결과 테이블 기본 골격
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_RESULT}.{TABLE_RESULT} (
                id BIGSERIAL PRIMARY KEY
            );
            """
        )

        # 1) yyyymmdd → end_day 자동 변경 (있을 경우만)
        cur.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = '{SCHEMA_RESULT}'
                      AND table_name   = '{TABLE_RESULT}'
                      AND column_name  = 'yyyymmdd'
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = '{SCHEMA_RESULT}'
                      AND table_name   = '{TABLE_RESULT}'
                      AND column_name  = 'end_day'
                ) THEN
                    EXECUTE 'ALTER TABLE {SCHEMA_RESULT}.{TABLE_RESULT} RENAME COLUMN yyyymmdd TO end_day';
                END IF;
            END
            $$;
            """
        )

        # 2) 필요한 컬럼들 추가
        cur.execute(
            f"""
            ALTER TABLE {SCHEMA_RESULT}.{TABLE_RESULT}
                ADD COLUMN IF NOT EXISTS file_path TEXT,
                ADD COLUMN IF NOT EXISTS end_day VARCHAR(8),
                ADD COLUMN IF NOT EXISTS end_time VARCHAR(8),
                ADD COLUMN IF NOT EXISTS barcode_information TEXT,
                ADD COLUMN IF NOT EXISTS station TEXT,
                ADD COLUMN IF NOT EXISTS remark TEXT,
                ADD COLUMN IF NOT EXISTS result TEXT,
                ADD COLUMN IF NOT EXISTS test_item TEXT,
                ADD COLUMN IF NOT EXISTS test_time VARCHAR(12),
                ADD COLUMN IF NOT EXISTS test_item_ct DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS "group" BIGINT,
                ADD COLUMN IF NOT EXISTS problem1 TEXT,
                ADD COLUMN IF NOT EXISTS problem2 TEXT,
                ADD COLUMN IF NOT EXISTS problem3 TEXT,
                ADD COLUMN IF NOT EXISTS fail_test_item TEXT,
                ADD COLUMN IF NOT EXISTS processed_time TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW();
            """
        )

    conn.commit()


def get_processed_file_paths(conn):
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT file_path FROM {SCHEMA_PROCESSING}.{TABLE_PROCESSING};"
        )
        rows = cur.fetchall()
    return {r[0] for r in rows}


def insert_results_and_history(conn, file_path, rows):
    if not rows:
        return

    with conn.cursor() as cur:
        insert_sql = f"""
            INSERT INTO {SCHEMA_RESULT}.{TABLE_RESULT} (
                file_path,
                end_day,
                end_time,
                barcode_information,
                test_item,
                test_time,
                test_item_ct
            )
            VALUES (%(file_path)s, %(end_day)s, %(end_time)s,
                    %(barcode_information)s, %(test_item)s,
                    %(test_time)s, %(test_item_ct)s);
        """
        cur.executemany(insert_sql, rows)

        cur.execute(
            f"""
            INSERT INTO {SCHEMA_PROCESSING}.{TABLE_PROCESSING} (file_path, processed_time)
            VALUES (%s, NOW())
            ON CONFLICT (file_path) DO NOTHING;
            """,
            (file_path,),
        )

    conn.commit()


# =====================================================================
# 3. 후처리 (station/result/remark/group/problem 매핑)
# =====================================================================
def postprocess_result_table(conn):
    with conn.cursor() as cur:
        # 1) (삭제) end_day, end_time, barcode_information, test_item 기준 중복 삭제
        #    → 파일별로 file_path 유니크하게 관리하고 있으므로 여기서는 중복 삭제를 하지 않는다.
        # print("[POST] 중복 행 삭제 스킵")

        # 2) Vision JSON에서 station, result 매핑
        cur.execute(
            f"""
            UPDATE {SCHEMA_RESULT}.{TABLE_RESULT} b
            SET station = a.station,
                result  = a.result
            FROM {SCHEMA_VISION}.{TABLE_VISION} a
            WHERE a.barcode_information = b.barcode_information
              AND b.station IS NULL;
            """
        )
        print("[POST] station / result 매핑 완료")

        # 3) remark = PD / Non-PD
        cur.execute(
            f"""
            UPDATE {SCHEMA_RESULT}.{TABLE_RESULT}
            SET remark =
                CASE
                    WHEN substring(barcode_information FROM 18 FOR 1) IN ('J','S')
                        THEN 'PD'
                    ELSE 'Non-PD'
                END;
            """
        )
        print("[POST] remark 채우기 완료")

        # 4) group 번호 부여
        cur.execute(
            f"""
            WITH group_map AS (
                SELECT
                    id,
                    DENSE_RANK() OVER (
                        ORDER BY end_day, end_time, barcode_information
                    ) AS grp
                FROM {SCHEMA_RESULT}.{TABLE_RESULT}
            )
            UPDATE {SCHEMA_RESULT}.{TABLE_RESULT} t
            SET "group" = g.grp
            FROM group_map g
            WHERE t.id = g.id;
            """
        )
        print("[POST] group 번호 부여 완료")

        # 5) problem1/2/3, fail_test_item 전체 초기화
        cur.execute(
            f"""
            UPDATE {SCHEMA_RESULT}.{TABLE_RESULT}
            SET problem1 = NULL,
                problem2 = NULL,
                problem3 = NULL,
                fail_test_item = NULL;
            """
        )
        print("[POST] problem1/2/3, fail_test_item 초기화 완료")

        # 6) remark = 'PD' 인 경우 매핑
        cur.execute(
            f"""
            WITH candidates AS (
                SELECT
                    id,
                    "group",
                    ROW_NUMBER() OVER (
                        PARTITION BY "group"
                        ORDER BY test_time, id
                    ) AS seq
                FROM {SCHEMA_RESULT}.{TABLE_RESULT}
                WHERE remark = 'PD'
                  AND test_item IN ('테스트 결과 : OK', '테스트 결과 : NG')
            ),
            mapping AS (
                SELECT * FROM (VALUES
                    (1 , 'dmm'       , NULL     , '1.00_dmm_c_rng_set'),
                    (2 , 'relay_board', NULL    , '1.00_d_sig_val_090_set'),
                    (3 , 'load_c'    , NULL     , '1.00_load_c_cc_rng_set'),
                    (4 , 'relay_board', NULL    , '1.00_d_sig_val_000_set'),
                    (5 , 'dmm'       , NULL     , '1.00_dmm_dc_v_set'),
                    (6 , 'dmm'       , NULL     , '1.00_dmm_0.6ac_set'),
                    (7 , 'ps'        , NULL     , '1.00_ps_14.7v_3.0c_set'),
                    (8 , 'dmm'       , NULL     , '1.00_dmm_dc_c_set'),
                    (9 , 'ps'        , NULL     , '1.00_ps_14.7v_set'),
                    (10, 'dmm'       , NULL     , '1.00_dmm_0.6ac_set'),
                    (11, 'power'     , 'ps'     , '1.01_input_14.7v'),
                    (12, 'usb_c'     , 'pm'     , '1.02_usb_c_pm1'),
                    (13, 'usb_c'     , 'pm'     , '1.03_usb_c_pm2'),
                    (14, 'usb_c'     , 'pm'     , '1.04_usb_c_pm3'),
                    (15, 'usb_c'     , 'pm'     , '1.05_usb_c_pm4'),
                    (16, 'usb_a'     , 'pm'     , '1.06_usb_a_pm1'),
                    (17, 'usb_a'     , 'pm'     , '1.07_usb_a_pm2'),
                    (18, 'usb_a'     , 'pm'     , '1.08_usb_a_pm3'),
                    (19, 'usb_a'     , 'pm'     , '1.09_usb_a_pm4'),
                    (20, 'mini_b'    , 'linux'  , '1.10_fw_ver_check'),
                    (21, 'mini_b'    , 'linux'  , '1.11_chip_ver_check'),
                    (22, 'mini_b'    , 'usb_c'  , '1.12_usb_c_carplay'),
                    (23, 'mini_b'    , 'usb_a'  , '1.13_usb_a_carplay'),
                    (24, 'mini_b'    , 'pd_board', '1.14_pd_count'),
                    (25, 'dmm'       , NULL     , '1.14_1_dmm_c_rng_set'),
                    (26, 'dmm'       , NULL     , '1.14_2_load_a_cc_set'),
                    (27, 'dmm'       , NULL     , '1.14_3_load_a_rng_set'),
                    (28, 'load_c'    , NULL     , '1.14_4_load_c_cc_set'),
                    (29, 'load_c'    , NULL     , '1.14_5_load_c_rng_set'),
                    (30, 'dmm'       , NULL     , '1.14_6_dmm_regi_set'),
                    (31, 'dmm'       , NULL     , '1.14_7_dmm_regi_0.6ac_set'),
                    (32, 'relay_board', NULL    , '1.14_8_d_sig_val_000_set'),
                    (33, 'power'     , 'dmm'    , '1.15_pin12_short_check'),
                    (34, 'power'     , 'dmm'    , '1.16_pin23_short_check'),
                    (35, 'power'     , 'dmm'    , '1.17_pin34_short_check'),
                    (36, 'dmm'       , NULL     , '1.17_1_dmm_dc_v_set'),
                    (37, 'dmm'       , NULL     , '1.17_2_dmm_0.6ac_set'),
                    (38, 'dmm'       , NULL     , '1.17_3_dmm_c_set'),
                    (39, 'load_a'    , NULL     , '1.17_4_load_a_sensing_on'),
                    (40, 'load_c'    , NULL     , '1.17_5_load_c_sensing_on'),
                    (41, 'ps'        , NULL     , '1.17_6_ps_18v_set'),
                    (42, 'ps'        , NULL     , '1.17_7_ps_18v_on'),
                    (43, 'dmm'       , NULL     , '1.17_8_dmm_0.6ac_set'),
                    (44, 'power'     , 'ps'     , '1.18_input_18v'),
                    (45, 'power'     , 'dmm'    , '1.19_idle_check'),
                    (46, 'usb_c'     , 'load_c' , '1.20_noload_usb_c'),
                    (47, 'usb_c'     , 'load_a' , '1.21_noload_usb_a'),
                    (48, 'dmm'       , NULL     , '1.21_1_dmm_3c_rng_set'),
                    (49, 'load_a'    , NULL     , '1.21_2_load_a_5c_set'),
                    (50, 'load_a'    , NULL     , '1.21_3_load_a_on'),
                    (51, 'dmm'       , NULL     , '1.22_usb_a_curr_check'),
                    (52, 'dmm'       , NULL     , '1.23_usb_a_vol_check'),
                    (53, 'usb_c'     , 'load_c' , '1.24_usb_c_v_check'),
                    (54, 'load_a'    , NULL     , '1.24_1_load_a_off'),
                    (55, 'load_c'    , NULL     , '1.24_2_load_c_5c_set'),
                    (56, 'load_c'    , NULL     , '1.24_3_load_c_on'),
                    (57, 'dmm'       , NULL     , '1.25_usb_c_curr_check'),
                    (58, 'dmm'       , NULL     , '1.26_usb_c_vol_check'),
                    (59, 'usb_a'     , 'dmm'    , '1.27_usb_a_vol_check'),
                    (60, 'load_c'    , NULL     , '1.27_1_load_c_off'),
                    (61, 'LOAD_A'    , NULL     , '1.27_2_load_a_2.4c_set'),
                    (62, 'load_c'    , NULL     , '1.27_3_load_c_3c_set'),
                    (63, 'load_a'    , NULL     , '1.27_4_load_a_on'),
                    (64, 'load_c'    , NULL     , '1.27_5_load_c_on'),
                    (65, 'usb_c'     , 'dmm'    , '1.28_usb_c_3c_check'),
                    (66, 'usb_c'     , 'dmm'    , '1.29_usb_c_5v_check'),
                    (67, 'usb_a'     , 'dmm'    , '1.30_usb_a_2.4a_check'),
                    (68, 'usb_a'     , 'dmm'    , '1.31_usb_a_5v_check'),
                    (69, 'load_c'    , NULL     , '1.31_1_load_c_1.3c_set'),
                    (70, 'usb_c'     , 'pd_board', '1.32_pdo4_set'),
                    (71, 'usb_c'     , 'dmm'    , '1.33_usb_c_1.35c_check'),
                    (72, 'usb_c'     , 'dmm'    , '1.34_usb_c_vol_check'),
                    (73, 'usb_c'     , 'pd_board', '1.35_cc_check'),
                    (74, 'load_c'    , NULL     , '1.35_1_load_c_off'),
                    (75, 'dmm'       , NULL     , '1.35_2_dmm_0.6ac_set'),
                    (76, 'dmm'       , NULL     , '1.35_3_dmm_c_rng_set'),
                    (77, 'product'   , 'dmm'    , '1.36_dark_curr_check')
                ) AS m(seq, problem1, problem2, fail_test_item)
            )
            UPDATE {SCHEMA_RESULT}.{TABLE_RESULT} t
            SET problem1       = m.problem1,
                problem2       = m.problem2,
                fail_test_item = m.fail_test_item
            FROM candidates c
            JOIN mapping   m ON c.seq = m.seq
            WHERE t.id = c.id;
            """
        )
        print("[POST] PD 매핑 완료")

        # 7) remark = 'Non-PD' 인 경우 매핑
        cur.execute(
            f"""
            WITH candidates AS (
                SELECT
                    id,
                    "group",
                    ROW_NUMBER() OVER (
                        PARTITION BY "group"
                        ORDER BY test_time, id
                    ) AS seq
                FROM {SCHEMA_RESULT}.{TABLE_RESULT}
                WHERE remark = 'Non-PD'
                  AND test_item IN ('테스트 결과 : OK', '테스트 결과 : NG')
            ),
            mapping AS (
                SELECT * FROM (VALUES
                    (1 , 'relay_board', NULL    , '1.00_d_sig_val_090_set'),
                    (2 , 'load_a'    , NULL     , '1.00_load_a_cc_set'),
                    (3 , 'dmm'       , NULL     , '1.00_load_a_rng_set'),
                    (4 , 'load_c'    , NULL     , '1.00_load_c_cc_set'),
                    (5 , 'dmm'       , NULL     , '1.00_load_c_rng_set'),
                    (6 , 'dmm'       , NULL     , '1.00_dmm_regi_set'),
                    (7 , 'dmm'       , NULL     , '1.00_dmm_regi_ac_set'),
                    (8 , 'mini_b'    , 'dmm'    , '1.00_mini_b_short_check'),
                    (9 , 'usb_a'     , 'dmm'    , '1.01_usb_a_short_check'),
                    (10, 'usb_c'     , 'dmm'    , '1.02_usb_c_short_check'),
                    (11, 'relay_board', NULL    , '1.02_1_d_sig_val_000_set'),
                    (12, 'dmm'       , NULL     , '1.02_2_dmm_regi_set'),
                    (13, 'dmm'       , NULL     , '1.02_2_dmm_regi_ac_set'),
                    (14, 'power'     , 'dmm'    , '1.03_pin12_short_check'),
                    (15, 'power'     , 'dmm'    , '1.04_pin23_short_check'),
                    (16, 'power'     , 'dmm'    , '1.05_pin34_short_check'),
                    (17, 'dmm'       , NULL     , '1.05_1_dmm_dc_v_set'),
                    (18, 'dmm'       , NULL     , '1.05_2_dmm_0.6ac_set'),
                    (19, 'dmm'       , NULL     , '1.05_3_dmm_c_set'),
                    (20, 'load_a'    , NULL     , '1.05_4_load_a_sensing_on'),
                    (21, 'load_c'    , NULL     , '1.05_5_load_c_sensing_on'),
                    (22, 'ps'        , NULL     , '1.05_6_ps_16.5v_3.0c_set'),
                    (23, 'ps'        , NULL     , '1.05_7_ps_on'),
                    (24, 'dmm'       , NULL     , '1.05_8_dmm_0.6ac_set'),
                    (25, 'power'     , 'ps'     , '1.06_input_16.5v'),
                    (26, 'power'     , 'dmm'    , '1.07_idle_check'),
                    (27, 'mini_b B'  , 'linux'  , '1.08_fw_ver_check'),
                    (28, 'mini_b B'  , 'linux'  , '1.09_chip_ver_check'),
                    (29, 'dmm'       , NULL     , '1.09_1__dmm_3c_rng_set'),
                    (30, 'load_a'    , NULL     , '1.09_2_load_a_5.5c_set'),
                    (31, 'load_a'    , NULL     , '1.09_3_load_a_on'),
                    (32, 'dmm'       , NULL     , '1.10_usb_a_vol_check'),
                    (33, 'dmm'       , NULL     , '1.11_usb_a_curr_check'),
                    (34, 'usb_c'     , 'dmm'    , '1.12_usb_c_vol_check'),
                    (35, 'load_a'    , NULL     , '1.12_1_load_a_off'),
                    (36, 'load_a'    , NULL     , '1.12_2_load_c_5.5c_set'),
                    (37, 'load_c'    , NULL     , '1.12_3_load_c_on'),
                    (38, 'dmm'       , NULL     , '1.13_usb_c_vol_check'),
                    (39, 'dmm'       , NULL     , '1.14_usb_c_curr_check'),
                    (40, 'usb_a'     , 'dmm'    , '1.15_usb_a_vol_check'),
                    (41, 'load_c'    , NULL     , '1.15_1_load_c_off'),
                    (42, 'ps'        , NULL     , '1.15_2_dut_reset'),
                    (43, 'usb_c'     , 'pd_board', '1.16_cc1_check'),
                    (44, 'usb_c'     , 'pd_board', '1.17_cc2_check'),
                    (45, 'load_a'    , NULL     , '1.17_1_load_a_2.4c_set'),
                    (46, 'load_c'    , NULL     , '1.17_2_load_c_3c_set'),
                    (47, 'load_a'    , NULL     , '1.17_3_load_a_on'),
                    (48, 'load_c'    , NULL     , '1.17_4_load_c_on'),
                    (49, 'usb_a'     , 'dmm'    , '1.18_usb_a_vol_check'),
                    (50, 'usb_a'     , 'dmm'    , '1.19_load_a_check'),
                    (51, 'usb_c'     , 'dmm'    , '1.20_usb_c_vol_check'),
                    (52, 'usb_c'     , 'dmm'    , '1.21_load_c_check'),
                    (53, 'load_a'    , NULL     , '1.21_1_load_a_off'),
                    (54, 'load_c'    , NULL     , '1.21_2_load_c_off'),
                    (55, 'usb_c'     , 'linux'  , '1.22_usb_c_carplay'),
                    (56, 'usb_a'     , 'linux'  , '1.23_usb_a_carplay'),
                    (57, 'usb_c'     , 'pm'     , '1.24_usb_c_pm1'),
                    (58, 'usb_c'     , 'pm'     , '1.25_usb_c_pm2'),
                    (59, 'usb_c'     , 'pm'     , '1.26_usb_c_pm3'),
                    (60, 'usb_c'     , 'pm'     , '1.27_usb_c_pm4'),
                    (61, 'usb_a'     , 'pm'     , '1.28_usb_a_pm1'),
                    (62, 'usb_a'     , 'pm'     , '1.29_usb_a_pm2'),
                    (63, 'usb_a'     , 'pm'     , '1.30_usb_a_pm3'),
                    (64, 'usb_a'     , 'pm'     , '1.31_usb_a_pm4'),
                    (65, 'dmm'       , NULL     , '1.31_1_dmm_0.6ac_set'),
                    (66, 'dmm'       , NULL     , '1.31_2_dmm_c_rng_set'),
                    (67, 'product'   , 'dmm'    , '1.32_dark_curr_check')
                ) AS m(seq, problem1, problem2, fail_test_item)
            )
            UPDATE {SCHEMA_RESULT}.{TABLE_RESULT} t
            SET problem1       = m.problem1,
                problem2       = m.problem2,
                fail_test_item = m.fail_test_item
            FROM candidates c
            JOIN mapping   m ON c.seq = m.seq
            WHERE t.id = c.id;
            """
        )
        print("[POST] Non-PD 매핑 완료")

    conn.commit()

# =====================================================================
# 4. 메인 실행
# =====================================================================
def run_once():
    print("\n================ run_once 시작 ================")
    print(f"[DEBUG] BASE_LOG_DIR: {BASE_LOG_DIR}")

    conn = get_connection()
    ensure_schema_and_tables(conn)

    # 1) 전체 txt 파일 스캔 후 FCT\YYYY\MM\DD 구조만 남김
    all_found_txt_files = list(BASE_LOG_DIR.rglob("*.txt"))
    print(f"1) TXT 파일 전체 스캔 완료 → {len(all_found_txt_files)}개")

    valid_files = [p for p in all_found_txt_files if is_valid_fct_path(p)]
    print(f"2) YYYY/MM/DD 구조 필터링 후 파일 수 → {len(valid_files)}개")

    # 2) 이미 처리한 파일 목록
    processed = get_processed_file_paths(conn)
    print(f"3) 이미 처리된 파일 수: {len(processed)}개")

    target_files = [p for p in valid_files if str(p) not in processed]
    total = len(target_files)
    print(f"4) 새로 처리할 파일 수: {total}개")

    if total == 0:
        conn.close()
        print("   → 새로 처리할 파일이 없습니다.")
        print("=============== run_once 종료 ===============\n")
        return

    print("5) 멀티프로세스 파싱 시작...")
    print(f"   → 워커 수: {NUM_WORKERS}개")

    start_ts = time.time()

    with Pool(processes=NUM_WORKERS) as pool:
        for idx, (file_path_str, rows, err) in enumerate(
            pool.imap_unordered(process_one_file, [str(p) for p in target_files]),
            start=1,
        ):
            if err:
                print(f"   [ERROR] {file_path_str} 처리 중 오류: {err}")
                continue
            if not rows:
                continue

            insert_results_and_history(conn, file_path_str, rows)

            if (idx % 100 == 0) or (idx == total):
                print(f"   → {idx}/{total} 파일 처리 및 DB 저장 완료")

    elapsed = time.time() - start_ts
    print(f"   → 파싱 및 저장 소요 시간: {elapsed:.1f}초")

    # 파싱 후 후처리
    postprocess_result_table(conn)

    conn.close()
    print("=============== run_once 종료 ===============\n")


def main():
    freeze_support()
    while True:
        try:
            run_once()
        except Exception as e:
            print(f"[MAIN ERROR] run_once 수행 중 오류: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
