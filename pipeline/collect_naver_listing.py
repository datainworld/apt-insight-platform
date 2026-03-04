"""
네이버 부동산 매물 데이터 수집 스크립트
- 서울/경기/인천 아파트 매물을 증분 방식으로 수집
- 네이버 단지 정보 + 카카오 API 행정동 변환
- PostgreSQL DB 저장 (text-to-SQL 최적화)
- 수집 완료 후 상세 결과보고서 생성

사용법:
    python collect_naver_listing.py full               # 최초 전체 수집
    python collect_naver_listing.py daily              # 매일 증분 수집
    python collect_naver_listing.py daily --resume     # 중단 지점부터 이어받기
    python collect_naver_listing.py full --skip-db     # DB 저장 생략
    python collect_naver_listing.py full --test        # 테스트 모드
"""

import os
import re
import sys
import json
import time
import math
import random
import argparse
import threading
import pandas as pd
import requests as std_requests
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

from curl_cffi import requests as curl_requests
from sqlalchemy import text

from utils import get_db_engine, get_latest_file, get_today_str, DATA_DIR, KAKAO_API_KEY


# ==============================================================================
# 상수 및 설정
# ==============================================================================

# 네이버 부동산 API 기본 URL
BASE_URL = "https://new.land.naver.com/api"

# HTTP 요청 헤더
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Referer": "https://new.land.naver.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "sec-ch-ua": '"Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

# 대상 지역 시도 코드 (네이버 cortarNo)
SIDO_CODES = {
    "서울특별시": "1100000000",
    "경기도": "4100000000",
    "인천광역시": "2800000000",
}

# 거래 유형
TRADE_TYPES = {
    "A1": "매매",
    "B1": "전세",
    "B2": "월세",
}

# Rate Limiting 설정
MIN_DELAY = 0.8   # 최소 대기 시간 (초) - 적응형 딜레이 하한
MAX_DELAY = 3.5   # 최대 대기 시간 (초) - 적응형 딜레이 상한
MAX_RETRIES = 5   # 최대 재시도 횟수
MAX_WORKERS = 5   # 매물 수집 병렬 워커 수
CHECKPOINT_INTERVAL = 200  # 체크포인트 저장 주기 (단지 수)

# 적응형 딜레이 상태 (스레드 안전)
_delay_lock = threading.Lock()
_current_delay = 1.5  # 현재 딜레이 (초)


# ==============================================================================
# HTTP 요청 유틸리티
# ==============================================================================

# 세션 객체 (curl_cffi - Chrome TLS fingerprint 모방)
_session = curl_requests.Session(impersonate="chrome")
_session.headers.update(HEADERS)

# 세션 초기화 플래그
_session_initialized = False


def _init_session():
    """네이버 부동산 API 세션 초기화"""
    global _session_initialized
    if _session_initialized:
        return

    print("  세션 초기화 중 (Chrome TLS fingerprint 모방)...")

    try:
        resp = _session.get("https://new.land.naver.com/", timeout=30)
        print(f"    메인 페이지: {resp.status_code}")

        html = resp.text
        token_match = re.search(r'"token"\s*:\s*"([^"]+)"', html)
        if token_match:
            token = token_match.group(1)
            _session.headers["authorization"] = f"Bearer {token}"
            print(f"    JWT 토큰 획득: {token[:50]}...")
        else:
            print("    ⚠ JWT 토큰을 HTML에서 찾지 못했습니다.")

        time.sleep(2)
    except Exception as e:
        print(f"    메인 페이지 방문 실패: {e}")

    cookie_str = os.getenv("NAVER_LAND_COOKIE", "")
    if cookie_str:
        for part in cookie_str.split(";"):
            part = part.strip()
            if "=" in part:
                key, _, val = part.partition("=")
                _session.cookies.set(key.strip(), val.strip())

    print(f"    쿠키: {list(_session.cookies.keys())}")
    _session_initialized = True
    print("  세션 초기화 완료")


def _adjust_delay(success):
    """적응형 딜레이 조정: 성공 시 감소, 429 시 증가"""
    global _current_delay
    with _delay_lock:
        if success:
            _current_delay = max(MIN_DELAY, _current_delay * 0.95)
        else:
            _current_delay = min(MAX_DELAY, _current_delay * 1.5)


def _request_with_retry(url, params=None, retries=MAX_RETRIES):
    """네이버 부동산 API 요청 (재시도 + 지수 백오프 + 적응형 딜레이)"""
    _init_session()

    for attempt in range(retries):
        try:
            with _delay_lock:
                delay = _current_delay
            time.sleep(delay + random.uniform(0, 0.3))

            resp = _session.get(url, params=params, timeout=30)

            if resp.status_code == 429:
                _adjust_delay(False)
                wait = (2 ** attempt) * 5
                print(f"  Rate limit (429). {attempt+1}/{retries}회 재시도, {wait}초 대기...")
                time.sleep(wait)
                continue

            if resp.status_code == 404:
                _adjust_delay(True)
                return None

            resp.raise_for_status()
            _adjust_delay(True)

            try:
                return resp.json()
            except ValueError:
                return None

        except Exception as e:
            if attempt < retries - 1:
                wait = (2 ** attempt) * 2
                print(f"  요청 실패 ({attempt+1}/{retries}): {e}, {wait}초 후 재시도")
                time.sleep(wait)
            else:
                print(f"  최종 실패: {url}")
                return None

    return None


# ==============================================================================
# 문자열 유틸리티
# ==============================================================================

def _parse_price(price_str):
    """네이버 가격 문자열을 만원 단위 정수로 변환"""
    if not price_str or not isinstance(price_str, str):
        return 0
    price_str = price_str.strip().replace(',', '')

    total = 0
    eok_match = re.search(r'(\d+)억', price_str)
    if eok_match:
        total += int(eok_match.group(1)) * 10000

    remaining = re.sub(r'\d+억\s*', '', price_str).strip()
    if remaining:
        num_match = re.search(r'(\d+)', remaining)
        if num_match:
            total += int(num_match.group(1))
    elif not eok_match:
        num_match = re.search(r'(\d+)', price_str)
        if num_match:
            total = int(num_match.group(1))

    return total


# ==============================================================================
# Step 1: 지역코드 수집
# ==============================================================================

def get_cortars():
    """서울/경기/인천의 시군구 → 읍면동 코드를 계층적으로 수집"""
    print("\n[Step 1] 지역코드(cortarNo) 수집")
    all_dongs = []
    sgg_count = 0

    for sido_name, sido_code in SIDO_CODES.items():
        print(f"  {sido_name} ({sido_code})...")
        data = _request_with_retry(
            f"{BASE_URL}/regions/list", params={"cortarNo": sido_code}
        )
        if not data or "regionList" not in data:
            print(f"    시군구 목록 조회 실패")
            continue

        sgg_list = data["regionList"]
        sgg_count += len(sgg_list)
        print(f"    시군구 {len(sgg_list)}개 발견")

        for sgg in sgg_list:
            sgg_code = sgg.get("cortarNo", "")
            sgg_name = sgg.get("cortarName", "")

            dong_data = _request_with_retry(
                f"{BASE_URL}/regions/list", params={"cortarNo": sgg_code}
            )
            if not dong_data or "regionList" not in dong_data:
                continue

            for dong in dong_data["regionList"]:
                all_dongs.append({
                    "sido_name": sido_name,
                    "sgg_name": sgg_name,
                    "sgg_code": sgg_code,
                    "dong_name": dong.get("cortarName", ""),
                    "dong_code": dong.get("cortarNo", ""),
                    "center_lat": dong.get("centerLat", 0),
                    "center_lon": dong.get("centerLon", 0),
                })

    print(f"  총 {len(all_dongs)}개 읍면동 수집 완료 (시군구 {sgg_count}개)")
    return all_dongs, sgg_count


# ==============================================================================
# Step 2: 단지 목록 수집
# ==============================================================================

def get_active_complexes(dong_list, test_mode=False):
    """동별 단지 목록 수집 (/api/regions/complexes)"""
    print("\n[Step 2] 단지 목록 수집 (/api/regions/complexes)")
    complexes = {}  # complexNo → 정보 딕셔너리 (중복 제거)

    targets = dong_list[:10] if test_mode else dong_list

    for i, dong in enumerate(targets):
        if (i + 1) % 50 == 0 or i == 0:
            print(f"  진행: {i+1}/{len(targets)} ({dong['sido_name']} {dong['sgg_name']} {dong['dong_name']})")

        data = _request_with_retry(
            f"{BASE_URL}/regions/complexes",
            params={
                "cortarNo": dong["dong_code"],
                "realEstateType": "APT",
                "order": "",
            }
        )

        if not data:
            continue

        complex_list = data.get("complexList", [])
        if not isinstance(complex_list, list):
            continue

        for cpx in complex_list:
            complex_no = str(cpx.get("complexNo", ""))
            if not complex_no:
                continue

            complexes[complex_no] = {
                "complex_no": complex_no,
                "complex_name": cpx.get("complexName", ""),
                "sido_name": dong["sido_name"],
                "sgg_name": dong["sgg_name"],
                "latitude": float(cpx.get("latitude", 0)),
                "longitude": float(cpx.get("longitude", 0)),
            }

    print(f"  수집된 단지: {len(complexes)}개")
    return complexes


# ==============================================================================
# Step 3: 행정동 변환 (카카오 API)
# ==============================================================================

def _get_admin_dong(lat, lon):
    """카카오 좌표→행정동 API 호출 (단건)"""
    if not KAKAO_API_KEY or not lat or not lon:
        return None
    try:
        resp = std_requests.get(
            "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json",
            headers={"Authorization": f"KakaoAK {KAKAO_API_KEY}"},
            params={"x": lon, "y": lat},
            timeout=5, verify=False,
        )
        if resp.status_code == 200:
            for region in resp.json().get("documents", []):
                if region["region_type"] == "H":
                    return region["region_3depth_name"]
    except Exception:
        pass
    return None


def convert_to_admin_dong(complexes):
    """위경도 → 카카오 API로 행정동 일괄 변환 (병렬 20워커)"""
    print("\n[Step 3] 행정동 변환 (카카오 API)")
    if not KAKAO_API_KEY:
        print("  ⚠ KAKAO_API_KEY가 없습니다. 행정동 변환을 건너뜁니다.")
        for info in complexes.values():
            info["dong_name"] = None
        return complexes

    total = len(complexes)
    success = 0

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {}
        for cno, info in complexes.items():
            future = executor.submit(_get_admin_dong, info["latitude"], info["longitude"])
            futures[future] = cno

        for future in as_completed(futures):
            cno = futures[future]
            try:
                admin_dong = future.result()
            except Exception:
                admin_dong = None

            complexes[cno]["dong_name"] = admin_dong
            if admin_dong:
                success += 1

    print(f"  행정동 변환 완료: {success}/{total}건 성공")
    return complexes


def sync_complexes(new_complexes, existing_df):
    """기존 단지와 비교하여 신규 단지만 행정동 변환 후 병합 (daily용)

    Returns:
        dict: 병합된 전체 complexes 딕셔너리
        pd.DataFrame: 갱신된 naver_complex DataFrame
        int: 신규 단지 수
    """
    print("\n[Step 2b] 단지 동기화 (신규 단지 감지)")

    existing_nos = set(existing_df["complex_no"].astype(str))
    new_only = {k: v for k, v in new_complexes.items() if k not in existing_nos}

    print(f"  기존 단지: {len(existing_nos)}개, 신규 단지: {len(new_only)}개")

    if new_only:
        # 신규 단지만 카카오 API로 행정동 변환
        print(f"  신규 {len(new_only)}개 단지 행정동 변환 중...")
        convert_to_admin_dong(new_only)

        # 신규 단지를 DataFrame에 추가
        new_rows = pd.DataFrame(new_only.values())
        existing_df = pd.concat([existing_df, new_rows], ignore_index=True)
        existing_df.drop_duplicates(subset=["complex_no"], keep="last", inplace=True)

    # 기존 단지 정보를 complexes 딕셔너리로 복원
    all_complexes = {}
    for _, row in existing_df.iterrows():
        cno = str(row["complex_no"])
        all_complexes[cno] = {
            "complex_no": cno,
            "complex_name": row.get("complex_name", ""),
            "sido_name": row.get("sido_name", ""),
            "sgg_name": row.get("sgg_name", ""),
            "dong_name": row.get("dong_name", None),
            "latitude": row.get("latitude", 0),
            "longitude": row.get("longitude", 0),
        }

    return all_complexes, existing_df, len(new_only)


# ==============================================================================
# Step 4: 매물 증분 수집
# ==============================================================================

def _fetch_articles(complex_no, trade_type):
    """특정 단지·거래유형의 매물 목록을 모두 가져옴 (페이지네이션)"""
    articles = []
    page = 1
    max_pages = 10

    while page <= max_pages:
        data = _request_with_retry(
            f"{BASE_URL}/articles/complex/{complex_no}",
            params={
                "realEstateType": "APT",
                "tradeType": trade_type,
                "page": page,
                "sameAddressGroup": "false",
            }
        )

        if not data:
            break

        article_list = data.get("articleList", [])
        if not article_list:
            break

        articles.extend(article_list)

        is_more = data.get("isMoreData", False)
        if not is_more:
            break
        page += 1

    return articles


def _parse_article(article, complex_no, trade_type, today_str):
    """API 응답의 개별 매물을 표준 형식으로 변환 (슬림화)"""
    article_no = str(article.get("articleNo", ""))
    if not article_no:
        return None

    area2 = article.get("area2", article.get("exclusiveArea", 0))
    try:
        exclusive_area = int(float(area2))
    except (ValueError, TypeError):
        exclusive_area = 0

    price = _parse_price(str(article.get("dealOrWarrantPrc", "0")))
    rent = _parse_price(str(article.get("rentPrc", "0"))) if trade_type == "B2" else 0

    confirm_ymd = article.get("articleConfirmYmd", "")
    confirm_date = None
    if confirm_ymd and len(confirm_ymd) == 8:
        try:
            confirm_date = f"{confirm_ymd[:4]}-{confirm_ymd[4:6]}-{confirm_ymd[6:8]}"
        except (ValueError, IndexError):
            pass

    return {
        "article_no": article_no,
        "complex_no": complex_no,
        "trade_type": trade_type,
        "exclusive_area": exclusive_area,
        "initial_price": price,
        "current_price": price,
        "rent_price": rent,
        "floor_info": str(article.get("floorInfo", "")).strip() or None,
        "direction": str(article.get("direction", "")).strip() or None,
        "confirm_date": confirm_date,
        "first_seen_date": today_str,
        "last_seen_date": today_str,
        "is_active": True,
    }


# 체크포인트 함수들

def _get_checkpoint_path():
    return os.path.join(DATA_DIR, f"checkpoint_listing_{get_today_str()}.json")


def _save_checkpoint(processed_complexes, listings, stats):
    checkpoint = {
        "date": get_today_str(),
        "processed_complexes": list(processed_complexes),
        "listings": listings,
        "stats": stats,
    }
    path = _get_checkpoint_path()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False)
    print(f"  💾 체크포인트 저장: {len(processed_complexes)}단지, {len(listings)}건 매물")


def _load_checkpoint():
    path = _get_checkpoint_path()
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"  ⚠ 체크포인트 로드 실패: {e}")
        return None


def _clear_checkpoint():
    path = _get_checkpoint_path()
    if os.path.exists(path):
        os.remove(path)
        print(f"  체크포인트 삭제: {path}")


def _collect_one_complex(cno, today_date):
    """단지 1개의 모든 거래유형 매물 수집 (병렬 워커용)"""
    results = []
    for trade_type in TRADE_TYPES.keys():
        articles = _fetch_articles(cno, trade_type)
        for art in articles:
            parsed = _parse_article(art, cno, trade_type, today_date)
            if parsed:
                results.append(parsed)
    return cno, results


def collect_listings_incremental(complexes, test_mode=False, resume=False):
    """단지별 매물을 병렬 수집하고 체크포인트로 중간 저장"""
    print("\n[Step 4] 매물 증분 수집")
    today_date = datetime.now().strftime("%Y-%m-%d")

    # 기존 매물 데이터 로드 (증분 비교용)
    existing_articles = {}
    prev_file = get_latest_file("naver_listing_*.csv", exclude_today=True)
    if prev_file:
        try:
            df_prev = pd.read_csv(prev_file, dtype={"article_no": str})
            df_active = df_prev[df_prev["is_active"] == True]
            for _, row in df_active.iterrows():
                existing_articles[str(row["article_no"])] = row.to_dict()
            print(f"  기존 활성 매물: {len(existing_articles)}건 (from {os.path.basename(prev_file)})")
        except Exception as e:
            print(f"  기존 데이터 로드 실패: {e}")

    # 수집 대상 단지 목록
    complex_list = list(complexes.keys())
    if test_mode:
        complex_list = complex_list[:20]

    # 체크포인트 복원
    all_listings = []
    processed_set = set()
    stats = {"sale": 0, "jeonse": 0, "monthly": 0, "new": 0, "updated": 0}

    if resume:
        ckpt = _load_checkpoint()
        if ckpt and ckpt.get("date") == get_today_str():
            processed_set = set(ckpt["processed_complexes"])
            all_listings = ckpt["listings"]
            stats = ckpt["stats"]
            print(f"  ✅ 체크포인트 복원: {len(processed_set)}단지 완료, {len(all_listings)}건 매물")

    remaining = [c for c in complex_list if c not in processed_set]
    total_all = len(complex_list)
    print(f"  수집 대상: {len(remaining)}단지 (전체 {total_all}, 완료 {total_all - len(remaining)})")

    # 병렬 수집
    batch_count = 0
    seen_article_nos = set(item.get("article_no", "") for item in all_listings)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for cno in remaining:
            future = executor.submit(_collect_one_complex, cno, today_date)
            futures[future] = cno

        for future in as_completed(futures):
            cno = futures[future]
            try:
                _, results = future.result()
            except Exception as e:
                print(f"  ⚠ {cno} 수집 오류: {e}")
                continue

            for parsed in results:
                ano = parsed["article_no"]
                seen_article_nos.add(ano)

                tt = parsed["trade_type"]
                if tt == "A1":
                    stats["sale"] += 1
                elif tt == "B1":
                    stats["jeonse"] += 1
                else:
                    stats["monthly"] += 1

                if ano in existing_articles:
                    prev = existing_articles[ano]
                    parsed["first_seen_date"] = prev.get("first_seen_date", today_date)
                    parsed["initial_price"] = prev.get("initial_price", parsed["current_price"])
                    if parsed["current_price"] != prev.get("current_price"):
                        stats["updated"] += 1
                else:
                    stats["new"] += 1

                all_listings.append(parsed)

            processed_set.add(cno)
            batch_count += 1

            done = len(processed_set)
            if done % 100 == 0 or done == total_all:
                print(f"  진행: {done}/{total_all} 단지 ({stats['new']}건 신규, {stats['updated']}건 갱신)")

            if batch_count % CHECKPOINT_INTERVAL == 0:
                _save_checkpoint(processed_set, all_listings, stats)

    # 종료 처리: 기존에 active였지만 오늘 안 나온 매물
    closed_count = 0
    for ano, prev in existing_articles.items():
        if ano not in seen_article_nos:
            prev_copy = dict(prev)
            prev_copy["is_active"] = False
            prev_copy["last_seen_date"] = prev.get("last_seen_date", today_date)
            all_listings.append(prev_copy)
            closed_count += 1

    stats["closed"] = closed_count
    stats["total"] = len(all_listings)

    print(f"\n  수집 완료:")
    print(f"    매매: {stats['sale']}건, 전세: {stats['jeonse']}건, 월세: {stats['monthly']}건")
    print(f"    신규: {stats['new']}건, 가격변경: {stats['updated']}건, 종료: {stats['closed']}건")
    print(f"    총: {stats['total']}건")

    _clear_checkpoint()

    df_listing = pd.DataFrame(all_listings) if all_listings else pd.DataFrame()
    return df_listing, stats


# ==============================================================================
# Step 5: CSV 저장
# ==============================================================================

def save_results_csv(df_complex, df_listing):
    """단지/매물 데이터를 CSV로 저장"""
    print("\n[Step 5] CSV 저장")
    today = get_today_str()
    files = {}

    # 단지 저장
    complex_file = os.path.join(DATA_DIR, f"naver_complex_{today}.csv")
    df_complex.to_csv(complex_file, index=False, encoding="utf-8-sig")
    print(f"  단지: {complex_file} ({len(df_complex)}건)")
    files["complex"] = complex_file

    # 매물 저장
    if not df_listing.empty:
        listing_file = os.path.join(DATA_DIR, f"naver_listing_{today}.csv")
        df_listing.to_csv(listing_file, index=False, encoding="utf-8-sig")
        print(f"  매물: {listing_file} ({len(df_listing)}건)")
        files["listing"] = listing_file

    return files


# ==============================================================================
# Step 6: DB 저장
# ==============================================================================

def create_naver_schema(engine):
    """네이버 매물 관련 PostgreSQL 테이블 생성 (full: DROP+CREATE, daily: CREATE IF NOT EXISTS)"""
    print("\n[Step 6-1] DB 스키마 생성 (초기화)")
    with engine.begin() as conn:
        conn.execute(text("""
            DROP TABLE IF EXISTS naver_listing CASCADE;
            DROP TABLE IF EXISTS naver_complex CASCADE;

            CREATE TABLE naver_complex (
                complex_no VARCHAR(20) PRIMARY KEY,
                complex_name VARCHAR(100),
                sido_name VARCHAR(20),
                sgg_name VARCHAR(50),
                dong_name VARCHAR(50),
                latitude FLOAT,
                longitude FLOAT
            );

            CREATE TABLE naver_listing (
                article_no VARCHAR(20) PRIMARY KEY,
                complex_no VARCHAR(20),
                trade_type VARCHAR(10),
                exclusive_area INTEGER,
                initial_price INTEGER,
                current_price INTEGER,
                rent_price INTEGER,
                floor_info VARCHAR(10),
                direction VARCHAR(10),
                confirm_date DATE,
                first_seen_date DATE,
                last_seen_date DATE,
                is_active BOOLEAN DEFAULT true
            );
        """))
        _add_table_comments(conn)

    print("  스키마 생성 완료 (DROP + CREATE)")


def ensure_naver_schema(engine):
    """테이블이 없을 때만 생성 (daily용 — 기존 데이터 보존)"""
    print("\n[Step 6-1] DB 스키마 확인")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS naver_complex (
                complex_no VARCHAR(20) PRIMARY KEY,
                complex_name VARCHAR(100),
                sido_name VARCHAR(20),
                sgg_name VARCHAR(50),
                dong_name VARCHAR(50),
                latitude FLOAT,
                longitude FLOAT
            );

            CREATE TABLE IF NOT EXISTS naver_listing (
                article_no VARCHAR(20) PRIMARY KEY,
                complex_no VARCHAR(20),
                trade_type VARCHAR(10),
                exclusive_area INTEGER,
                initial_price INTEGER,
                current_price INTEGER,
                rent_price INTEGER,
                floor_info VARCHAR(10),
                direction VARCHAR(10),
                confirm_date DATE,
                first_seen_date DATE,
                last_seen_date DATE,
                is_active BOOLEAN DEFAULT true
            );
        """))
        _add_table_comments(conn)

    print("  스키마 확인 완료 (기존 데이터 보존)")


def _add_table_comments(conn):
    """테이블/컬럼 COMMENT 추가 (내부 헬퍼)"""
    conn.execute(text("""
        COMMENT ON TABLE naver_complex IS
            '네이버 부동산 아파트 단지 정보 (Naver Complex Info)';
        COMMENT ON COLUMN naver_complex.complex_no IS
            '네이버 단지 고유번호 (Naver Complex ID, PK)';
        COMMENT ON COLUMN naver_complex.complex_name IS
            '네이버 등록 단지명 (Complex Name)';
        COMMENT ON COLUMN naver_complex.sido_name IS
            '시도명 (Sido Name, e.g. 서울특별시)';
        COMMENT ON COLUMN naver_complex.sgg_name IS
            '시군구명 (District Name, e.g. 강남구)';
        COMMENT ON COLUMN naver_complex.dong_name IS
            '행정동명 - 카카오 API 변환 (Administrative Dong, e.g. 개포2동)';
        COMMENT ON COLUMN naver_complex.latitude IS
            '위도 (Latitude)';
        COMMENT ON COLUMN naver_complex.longitude IS
            '경도 (Longitude)';
        COMMENT ON TABLE naver_listing IS
            '네이버 부동산 아파트 매물 정보 - 증분 수집 (Naver Listings, Incremental)';
        COMMENT ON COLUMN naver_listing.article_no IS
            '네이버 매물 고유번호 (Naver Article ID, PK)';
        COMMENT ON COLUMN naver_listing.complex_no IS
            '네이버 단지번호 - naver_complex와 조인 (Naver Complex ID, FK)';
        COMMENT ON COLUMN naver_listing.trade_type IS
            '거래 유형: A1=매매, B1=전세, B2=월세 (Trade Type)';
        COMMENT ON COLUMN naver_listing.exclusive_area IS
            '전용 면적 (㎡, 정수) (Exclusive Area)';
        COMMENT ON COLUMN naver_listing.initial_price IS
            '최초 등록 호가, 만원 (Initial Asking Price)';
        COMMENT ON COLUMN naver_listing.current_price IS
            '현재 호가, 만원 (Current Asking Price)';
        COMMENT ON COLUMN naver_listing.rent_price IS
            '월세, 만원 - B2인 경우만 (Monthly Rent)';
        COMMENT ON COLUMN naver_listing.floor_info IS
            '층 정보 (Floor Info)';
        COMMENT ON COLUMN naver_listing.direction IS
            '향 (Direction/Facing)';
        COMMENT ON COLUMN naver_listing.confirm_date IS
            '매물 확인 일자 (Confirmation Date)';
        COMMENT ON COLUMN naver_listing.first_seen_date IS
            '최초 수집 일자 (First Seen Date)';
        COMMENT ON COLUMN naver_listing.last_seen_date IS
            '마지막 확인 일자 (Last Seen Date)';
        COMMENT ON COLUMN naver_listing.is_active IS
            '현재 노출 중 여부: true=활성, false=종료 (Is Active)';
    """))


def save_to_db(engine, df_complex, df_listing):
    """단지/매물 데이터를 PostgreSQL에 적재 (UPSERT)"""
    print("\n[Step 6-2] DB 데이터 적재")
    results = {}

    # naver_complex UPSERT (신규 단지 INSERT, 기존 단지 UPDATE)
    chunk_size = 5000
    total_complex = 0
    with engine.begin() as conn:
        for i in range(0, len(df_complex), chunk_size):
            chunk = df_complex.iloc[i:i+chunk_size]
            chunk.to_sql("naver_complex_staging", conn,
                         if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO naver_complex (complex_no, complex_name, sido_name, sgg_name, dong_name, latitude, longitude)
                SELECT 
                    CAST(complex_no AS VARCHAR), 
                    CAST(complex_name AS VARCHAR), 
                    CAST(sido_name AS VARCHAR), 
                    CAST(sgg_name AS VARCHAR), 
                    CAST(dong_name AS VARCHAR), 
                    CAST(latitude AS DOUBLE PRECISION), 
                    CAST(longitude AS DOUBLE PRECISION)
                FROM naver_complex_staging
                ON CONFLICT (complex_no) DO UPDATE SET
                    complex_name = EXCLUDED.complex_name,
                    sido_name = EXCLUDED.sido_name,
                    sgg_name = EXCLUDED.sgg_name,
                    dong_name = EXCLUDED.dong_name,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude;
            """))
            conn.execute(text("DROP TABLE IF EXISTS naver_complex_staging;"))
            total_complex += len(chunk)
    results["complex"] = total_complex
    print(f"  단지 적재: {total_complex}건 (UPSERT)")

    if not df_listing.empty:
        # DB 적재 시 ON CONFLICT (article_no) 처리를 위해 중복 데이터 제거
        df_listing = df_listing.drop_duplicates(subset=["article_no"], keep="last")
        
        for col in ["confirm_date", "first_seen_date", "last_seen_date"]:
            if col in df_listing.columns:
                df_listing[col] = pd.to_datetime(df_listing[col], errors="coerce")

        chunk_size = 5000
        total_loaded = 0

        with engine.begin() as conn:
            for i in range(0, len(df_listing), chunk_size):
                chunk = df_listing.iloc[i:i+chunk_size]
                chunk.to_sql("naver_listing_staging", conn,
                             if_exists="replace", index=False)

                conn.execute(text("""
                    INSERT INTO naver_listing (
                        article_no, complex_no, trade_type, exclusive_area, 
                        initial_price, current_price, rent_price, floor_info, 
                        direction, confirm_date, first_seen_date, last_seen_date, is_active
                    )
                    SELECT 
                        CAST(article_no AS VARCHAR),
                        CAST(complex_no AS VARCHAR),
                        CAST(trade_type AS VARCHAR),
                        CAST(exclusive_area AS INTEGER),
                        CAST(initial_price AS INTEGER),
                        CAST(current_price AS INTEGER),
                        CAST(rent_price AS INTEGER),
                        CAST(floor_info AS VARCHAR),
                        CAST(direction AS VARCHAR),
                        CAST(confirm_date AS DATE),
                        CAST(first_seen_date AS DATE),
                        CAST(last_seen_date AS DATE),
                        CAST(is_active AS BOOLEAN)
                    FROM naver_listing_staging
                    ON CONFLICT (article_no) DO UPDATE SET
                        current_price = EXCLUDED.current_price,
                        rent_price = EXCLUDED.rent_price,
                        last_seen_date = EXCLUDED.last_seen_date,
                        is_active = EXCLUDED.is_active;
                """))
                conn.execute(text("DROP TABLE IF EXISTS naver_listing_staging;"))
                total_loaded += len(chunk)
                print(".", end="", flush=True)

        print(f"\n  매물 적재: {total_loaded}건")
        results["listing"] = total_loaded
    else:
        results["listing"] = 0

    return results


def create_naver_indices(engine):
    """성능 최적화 인덱스 생성"""
    print("\n[Step 6-3] 인덱스 생성")
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_naver_listing_active "
            "ON naver_listing (is_active, last_seen_date);"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_naver_listing_trade "
            "ON naver_listing (trade_type);"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_naver_listing_complex "
            "ON naver_listing (complex_no);"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_naver_complex_sgg "
            "ON naver_complex (sgg_name, dong_name);"
        ))
    print("  인덱스 4개 생성 완료")


# ==============================================================================
# Step 7: 디스크 정리
# ==============================================================================

def cleanup_old_files():
    """증분 완료 후 이전 날짜의 CSV 파일 삭제"""
    print("\n[Step 7] 디스크 정리")
    import glob
    today = get_today_str()
    patterns = ["naver_complex_*.csv", "naver_listing_*.csv", "checkpoint_listing_*.json"]
    removed = 0

    for pattern in patterns:
        for f in glob.glob(os.path.join(DATA_DIR, pattern)):
            if today not in os.path.basename(f):
                os.remove(f)
                removed += 1
                print(f"  삭제: {os.path.basename(f)}")

    if removed == 0:
        print("  정리할 파일 없음")
    else:
        print(f"  {removed}개 파일 삭제 완료")


# ==============================================================================
# Step 8: 결과보고서
# ==============================================================================

def generate_report(results):
    """수집 완료 후 상세 결과보고서 생성"""
    print("\n[Step 8] 결과보고서 생성")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = get_today_str()
    r = results

    report = f"""{'='*60}
  네이버 부동산 매물 수집 결과 보고서
  수집 일시: {now}
  수집 모드: {r.get('mode', '-')}
{'='*60}

[1. 지역코드 수집]
  시도: {r.get('sido_count', 0)}개
  시군구: {r.get('sgg_count', '-')}개
  읍면동: {r.get('dong_count', '-')}개

[2. 단지 수집]
  전체 단지: {r.get('total_complexes', 0)}개
  신규 단지: {r.get('new_complexes', '-')}개

[3. 매물 수집 결과]
  매매(A1): {r.get('sale_count', 0):>8}건
  전세(B1): {r.get('jeonse_count', 0):>8}건
  월세(B2): {r.get('monthly_count', 0):>8}건
  총 매물:  {r.get('total_listings', 0):>8}건
  ┌──────────────────────────────────┐
  │ 증분 처리                        │
  │   신규 등록: {r.get('new_count', 0):>8}건          │
  │   가격 변경: {r.get('updated_count', 0):>8}건          │
  │   종료 처리: {r.get('closed_count', 0):>8}건          │
  └──────────────────────────────────┘

[4. DB 저장 결과]
  naver_complex: {r.get('db_complex', '-')}건
  naver_listing: {r.get('db_listing', '-')}건

[5. 파일 저장]
  {r.get('complex_file', '-')}
  {r.get('listing_file', '-')}
{'='*60}
"""

    print(report)

    report_file = os.path.join(DATA_DIR, f"naver_collection_report_{today}.txt")
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"  보고서 저장: {report_file}")

    return report_file


# ==============================================================================
# 메인 실행
# ==============================================================================

def main(mode="full", skip_db=False, test_mode=False, resume=False):
    """네이버 부동산 매물 수집 메인 함수"""
    print("=" * 60)
    print(f"  네이버 부동산 매물 데이터 수집 시작 [{mode.upper()} 모드]")
    if test_mode:
        print("  ⚠ 테스트 모드: 소규모 수집")
    if resume:
        print("  ♻ 이어받기 모드: 체크포인트에서 복원")
    print("=" * 60)

    results = {"sido_count": len(SIDO_CODES), "mode": mode}

    # --- Step 1~3: 단지 정보 수집 ---
    # full/daily 모두 지역코드+단지목록 수집 (daily는 신규 단지 감지용)
    dong_list, sgg_count = get_cortars()
    results["sgg_count"] = sgg_count
    results["dong_count"] = len(dong_list)

    if not dong_list:
        print("지역코드 수집 실패. 종료합니다.")
        return

    complexes = get_active_complexes(dong_list, test_mode=test_mode)
    results["total_complexes"] = len(complexes)

    if not complexes:
        print("단지 수집 실패. 종료합니다.")
        return

    if mode == "daily":
        # daily: 기존 naver_complex CSV 로드 → 신규 단지 diff
        complex_file = get_latest_file("naver_complex_*.csv")
        if not complex_file:
            print("  ⚠ 기존 naver_complex 파일이 없습니다. 'full' 모드를 먼저 실행하세요.")
            return

        existing_df = pd.read_csv(complex_file)
        complexes, df_complex, new_count = sync_complexes(complexes, existing_df)
        results["new_complexes"] = new_count

    else:
        # full: 전체 단지 행정동 변환
        complexes = convert_to_admin_dong(complexes)
        df_complex = pd.DataFrame(complexes.values())
        results["new_complexes"] = len(complexes)

    # --- Step 4: 매물 증분 수집 ---
    df_listing, stats = collect_listings_incremental(
        complexes, test_mode=test_mode, resume=resume
    )
    results["sale_count"] = stats.get("sale", 0)
    results["jeonse_count"] = stats.get("jeonse", 0)
    results["monthly_count"] = stats.get("monthly", 0)
    results["total_listings"] = stats.get("total", 0)
    results["new_count"] = stats.get("new", 0)
    results["updated_count"] = stats.get("updated", 0)
    results["closed_count"] = stats.get("closed", 0)

    # --- Step 5: CSV 저장 ---
    files = save_results_csv(df_complex, df_listing)
    results["complex_file"] = files.get("complex", "-")
    results["listing_file"] = files.get("listing", "-")

    # --- Step 6: DB 저장 ---
    if not skip_db:
        try:
            engine = get_db_engine()
            if mode == "full":
                create_naver_schema(engine)
            else:
                ensure_naver_schema(engine)
            db_results = save_to_db(engine, df_complex, df_listing)
            create_naver_indices(engine)
            results["db_complex"] = db_results.get("complex", 0)
            results["db_listing"] = db_results.get("listing", 0)
        except Exception as e:
            print(f"\n  ⚠ DB 저장 실패: {e}")
            results["db_complex"] = "실패"
            results["db_listing"] = "실패"
    else:
        print("\n  (DB 저장 건너뛰기)")
        results["db_complex"] = "건너뜀"
        results["db_listing"] = "건너뜀"

    # --- Step 7: 디스크 정리 ---
    cleanup_old_files()

    # --- Step 8: 결과보고서 ---
    report_file = generate_report(results)
    results["report_file"] = report_file

    print("\n" + "=" * 60)
    print("  수집 완료!")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="네이버 부동산 매물 수집")
    sub = parser.add_subparsers(dest="mode", help="수집 모드")
    sub.required = True

    p_full = sub.add_parser("full", help="최초 전체 수집 (지역코드→단지→행정동→매물)")
    p_full.add_argument("--skip-db", action="store_true", help="DB 저장 생략")
    p_full.add_argument("--test", action="store_true", help="테스트 모드 (소규모)")
    p_full.add_argument("--resume", action="store_true", help="체크포인트 이어받기")

    p_daily = sub.add_parser("daily", help="매일 증분 수집 (신규 단지 자동 반영)")
    p_daily.add_argument("--skip-db", action="store_true", help="DB 저장 생략")
    p_daily.add_argument("--test", action="store_true", help="테스트 모드 (소규모)")
    p_daily.add_argument("--resume", action="store_true", help="체크포인트 이어받기")

    args = parser.parse_args()

    main(
        mode=args.mode,
        skip_db=args.skip_db,
        test_mode=args.test,
        resume=args.resume,
    )
