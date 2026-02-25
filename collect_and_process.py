"""
데이터 수집 및 가공 스크립트

사용법:
    python collect_and_process.py --regions 11 28 41 --months 36

매개변수:
    --regions : 시도 코드 목록 (기본값: 11 28 41 = 서울, 인천, 경기)
    --months  : 수집 기간 (개월, 기본값: 36)
    --skip-code : 단지 코드 수집 건너뛰기
    --skip-basic : 기본/상세 정보 수집 건너뛰기
    --skip-trade : 매매/전월세 수집 건너뛰기
"""

import argparse
import os
import re
import time
import numpy as np
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import (
    fetch_data, save_to_csv, get_today_str, get_latest_file,
    DATA_DIR, KAKAO_API_KEY
)


# ==============================================================================
# 1. 수집: 아파트 단지 코드
# ==============================================================================

def collect_apt_codes(sido_codes):
    """시도 코드 목록에 대해 아파트 단지 코드를 수집합니다."""
    url = "https://apis.data.go.kr/1613000/AptListService3/getSidoAptList3"
    total_data = []

    for sido_code in sido_codes:
        page = 1
        rows = 1000
        all_items = []
        print(f"단지 코드 수집: 시도 {sido_code}")

        while True:
            params = {"sidoCode": sido_code, "pageNo": page, "numOfRows": rows}
            data = fetch_data(url, params)
            if not data:
                break

            try:
                response = data.get('response', {})
                header = response.get('header', {})
                if header.get('resultCode') != '00':
                    print(f"API Error: {header.get('resultMsg')}")
                    break

                body = response.get('body', {})
                items = body.get('items')
                if not items:
                    break

                if isinstance(items, list):
                    item_list = items
                elif isinstance(items, dict):
                    item_list = items.get('item', [])
                else:
                    item_list = []

                if isinstance(item_list, dict):
                    item_list = [item_list]
                if not item_list:
                    break

                all_items.extend(item_list)
                print(f"  Page {page}: {len(item_list)} items")

                if len(item_list) < rows:
                    break
                page += 1

            except AttributeError as e:
                print(f"Parsing error: {e}")
                break

        total_data.extend(all_items)

    filename = f"apt_code_{get_today_str()}.csv"
    save_to_csv(total_data, filename)
    return os.path.join(DATA_DIR, filename)


# ==============================================================================
# 2. 수집: 아파트 기본/상세 정보
# ==============================================================================

def _get_basic_info(kapt_code):
    """단일 아파트의 기본 정보를 수집합니다."""
    url = "https://apis.data.go.kr/1613000/AptBasisInfoServiceV4/getAphusBassInfoV4"
    params = {"kaptCode": kapt_code}
    data = fetch_data(url, params)
    if data and isinstance(data, dict):
        try:
            return data.get('response', {}).get('body', {}).get('item', {})
        except:
            return None
    return None


def _get_detail_info(kapt_code):
    """단일 아파트의 상세 정보를 수집합니다."""
    url = "https://apis.data.go.kr/1613000/AptBasisInfoServiceV4/getAphusDtlInfoV4"
    params = {"kaptCode": kapt_code}
    data = fetch_data(url, params)
    if data and isinstance(data, dict):
        try:
            return data.get('response', {}).get('body', {}).get('item', {})
        except:
            return None
    return None


def collect_all_info(apt_code_file, max_items=10000):
    """아파트 코드 파일에서 모든 단지의 기본/상세 정보를 수집합니다. (일일 API 제한 고려)"""
    df = pd.read_csv(apt_code_file)
    if 'kaptCode' not in df.columns:
        print("kaptCode 컬럼을 찾을 수 없습니다.")
        return

    all_kapt_codes = df['kaptCode'].unique()
    print(f"수집 대상: {len(all_kapt_codes)}개 단지")

    today_str = get_today_str()
    basic_file = os.path.join(DATA_DIR, f"apt_basic_info_{today_str}.csv")
    detail_file = os.path.join(DATA_DIR, f"apt_detail_info_{today_str}.csv")

    basic_infos = []
    detail_infos = []
    processed_codes = set()

    # 이어받기: 기존 파일이 있으면 진행 상황 복원
    if os.path.exists(basic_file):
        try:
            existing = pd.read_csv(basic_file)
            basic_infos = existing.to_dict('records')
            if 'kaptCode' in existing.columns:
                processed_codes.update(existing['kaptCode'].astype(str).unique())
            print(f"이어받기: {len(processed_codes)}개 처리 완료된 코드 발견")
        except Exception as e:
            print(f"기존 파일 읽기 오류: {e}")

    if os.path.exists(detail_file):
        try:
            existing = pd.read_csv(detail_file)
            detail_infos = existing.to_dict('records')
            if 'kaptCode' in existing.columns:
                processed_codes.update(existing['kaptCode'].astype(str).unique())
        except:
            pass

    codes_to_process = [c for c in all_kapt_codes if str(c) not in processed_codes]
    if not codes_to_process:
        print("모든 코드가 이미 처리 완료.")
        return basic_file, detail_file

    print(f"남은 수집 대상: {len(codes_to_process)}개 (이번 실행에서는 최대 {max_items}개만 수집)")
    count = 0
    total = min(len(codes_to_process), max_items)

    try:
        for code in codes_to_process:
            if count >= max_items:
                print(f"\\n[제한 도달] 설정된 최대 수집 건수({max_items}건)에 도달하여 수집을 일시 중지합니다.")
                break
                
            count += 1
            if count % 10 == 0 or total <= 100:
                print(f"정보 수집 {count}/{total}...")

            b_info = _get_basic_info(code)
            if b_info:
                basic_infos.append(b_info)

            d_info = _get_detail_info(code)
            if d_info:
                detail_infos.append(d_info)

            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단. 진행 상황 저장 중...")

    save_to_csv(basic_infos, f"apt_basic_info_{today_str}.csv")
    save_to_csv(detail_infos, f"apt_detail_info_{today_str}.csv")
    return basic_file, detail_file


# ==============================================================================
# 3. 수집: 매매/전월세 거래 데이터
# ==============================================================================

def _get_lawd_codes(filepath):
    """apt_code 파일에서 고유한 LAWD_CD (5자리 지역코드)를 추출합니다."""
    df = pd.read_csv(filepath)
    if 'bjdCode' not in df.columns:
        print("bjdCode 컬럼을 찾을 수 없습니다.")
        return []
    df['bjdCode'] = df['bjdCode'].astype(str)
    df['lawd_cd'] = df['bjdCode'].str[:5]
    return df['lawd_cd'].unique()


def _get_month_list(months_back=36):
    """현재로부터 months_back 개월 전까지의 월 목록을 반환합니다."""
    today = datetime.now()
    month_list = []
    for i in range(months_back + 1):
        d = today - relativedelta(months=i)
        month_list.append(d.strftime("%Y%m"))
    return sorted(month_list)


def _collect_trade_data(lawd_cd, deal_ym):
    """특정 지역/월의 매매 데이터를 수집합니다."""
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    params = {"LAWD_CD": lawd_cd, "DEAL_YMD": deal_ym, "numOfRows": 1000, "pageNo": 1}
    all_items = []

    while True:
        data = fetch_data(url, params)
        if not data:
            break
        try:
            response = data.get('response', {})
            header = response.get('header', {})
            r_code = str(header.get('resultCode'))
            if r_code not in ['00', '000']:
                print(f"API Error (Trade): {header.get('resultMsg')} (Code: {r_code})")
                break

            body = response.get('body', {})
            items = body.get('items')
            if not items:
                break

            if isinstance(items, list):
                item_list = items
            elif isinstance(items, dict):
                item_list = items.get('item', [])
            else:
                item_list = []
            if isinstance(item_list, dict):
                item_list = [item_list]

            all_items.extend(item_list)
            if len(item_list) < params['numOfRows']:
                break
            params['pageNo'] += 1

        except Exception as e:
            print(f"매매 데이터 파싱 오류: {e}")
            break

    return all_items


def _collect_rent_data(lawd_cd, deal_ym):
    """특정 지역/월의 전월세 데이터를 수집합니다."""
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
    params = {"LAWD_CD": lawd_cd, "DEAL_YMD": deal_ym, "numOfRows": 1000, "pageNo": 1}
    all_items = []

    while True:
        data = fetch_data(url, params)
        if not data:
            break
        try:
            response = data.get('response', {})
            header = response.get('header', {})
            r_code = str(header.get('resultCode'))
            if r_code not in ['00', '000']:
                print(f"API Error (Rent): {header.get('resultMsg')} (Code: {r_code})")
                break

            body = response.get('body', {})
            items = body.get('items')
            if not items:
                break

            if isinstance(items, list):
                item_list = items
            elif isinstance(items, dict):
                item_list = items.get('item', [])
            else:
                item_list = []
            if isinstance(item_list, dict):
                item_list = [item_list]

            all_items.extend(item_list)
            if len(item_list) < params['numOfRows']:
                break
            params['pageNo'] += 1

        except Exception as e:
            print(f"전월세 데이터 파싱 오류: {e}")
            break

    return all_items


def collect_all_trade_rent(apt_code_file, months_back=36):
    """모든 지역/월에 대해 매매 및 전월세 데이터를 수집합니다."""
    lawd_codes = _get_lawd_codes(apt_code_file)
    month_list = _get_month_list(months_back)

    print(f"수집 대상: {len(lawd_codes)}개 지역 × {len(month_list)}개월")

    all_trades = []
    all_rents = []

    count = 0
    total = len(lawd_codes) * len(month_list)

    for lawd in lawd_codes:
        for ym in month_list:
            count += 1
            if count % 10 == 0:
                print(f"진행 중 {count}/{total} (지역: {lawd}, 월: {ym})")

            trades = _collect_trade_data(lawd, ym)
            if trades:
                for t in trades:
                    t['LAWD_CD'] = lawd
                    t['DEAL_YMD'] = ym
                all_trades.extend(trades)

            rents = _collect_rent_data(lawd, ym)
            if rents:
                for r in rents:
                    r['LAWD_CD'] = lawd
                    r['DEAL_YMD'] = ym
                all_rents.extend(rents)

            time.sleep(0.1)

    today_str = get_today_str()
    save_to_csv(all_trades, f"apt_trade_{today_str}.csv")
    save_to_csv(all_rents, f"apt_rent_{today_str}.csv")

    return (
        os.path.join(DATA_DIR, f"apt_trade_{today_str}.csv"),
        os.path.join(DATA_DIR, f"apt_rent_{today_str}.csv")
    )


# ==============================================================================
# 4. 가공: Kakao 지오코딩
# ==============================================================================

def get_kakao_coords(address):
    """Kakao API를 사용하여 주소의 위경도 및 행정동을 반환합니다."""
    if not KAKAO_API_KEY:
        return None, None, None

    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    params = {"query": address}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=5, verify=False)
        if response.status_code == 200:
            data = response.json()
            if data['documents']:
                doc = data['documents'][0]
                y = doc['y']
                x = doc['x']

                admin_dong = None
                try:
                    cidx_url = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"
                    cidx_params = {"x": x, "y": y}
                    c_resp = requests.get(cidx_url, headers=headers, params=cidx_params, timeout=5, verify=False)
                    if c_resp.status_code == 200:
                        c_data = c_resp.json()
                        for region in c_data.get('documents', []):
                            if region['region_type'] == 'H':
                                admin_dong = region['region_3depth_name']
                                break
                except:
                    pass

                if not admin_dong:
                    admin_dong = doc['address']['region_3depth_name'] if doc['address'] else None

                return y, x, admin_dong

    except Exception:
        pass

    return None, None, None


def _normalize_string(s):
    """문자열 정규화 (아파트, 공백, 괄호 제거)"""
    if not isinstance(s, str):
        return ""
    s = re.sub(r'아파트', '', s)
    s = re.sub(r'\s+', '', s)
    s = re.sub(r'\(.*?\)', '', s)
    return s.strip()


# ==============================================================================
# 5. 가공: 기본 정보 (apt_basic_info_master)
# ==============================================================================

def process_basic_info(trade_file, rent_file=None):
    """
    매매/전월세 원본 데이터에서 고유 아파트 목록을 추출하고
    지오코딩을 수행하여 apt_basic_info_master 파일을 생성합니다.
    """
    print("--- 기본 정보 가공 (지오코딩 포함) ---")

    if not os.path.exists(trade_file):
        print(f"매매 파일 없음: {trade_file}")
        return None

    df_trade = pd.read_csv(trade_file, dtype={'aptSeq': str}, low_memory=False)
    cols = ['aptSeq', 'aptNm', 'buildYear', 'jibun', 'roadNm', 'roadNmBonbun', 'roadNmBubun', 'umdNm', 'sggNm']
    avail = [c for c in cols if c in df_trade.columns]
    df_combined = df_trade[avail].drop_duplicates(subset=['aptSeq'])

    if rent_file and os.path.exists(rent_file):
        df_rent = pd.read_csv(rent_file, dtype={'aptSeq': str}, low_memory=False)
        avail_r = [c for c in cols if c in df_rent.columns]
        df_rent_u = df_rent[avail_r].drop_duplicates(subset=['aptSeq'])
        df_combined = pd.concat([df_combined, df_rent_u]).drop_duplicates(subset=['aptSeq'])

    # 이어받기
    apt_basic_file = os.path.join(DATA_DIR, f"apt_basic_info_master_{get_today_str()}.csv")
    processed_ids = set()
    all_results = []

    if os.path.exists(apt_basic_file):
        df_exist = pd.read_csv(apt_basic_file, dtype={'apt_id': str})
        processed_ids = set(df_exist['apt_id'].unique())
        all_results = df_exist.to_dict('records')
        print(f"이어받기: {len(processed_ids)}개 처리 완료")

    df_combined = df_combined[~df_combined['aptSeq'].isin(processed_ids)]
    print(f"남은 가공 대상: {len(df_combined)}개 아파트")

    def build_address(row):
        sgg = str(row.get('sggNm', '')) if pd.notna(row.get('sggNm')) else ''
        road = str(row.get('roadNm', '')) if pd.notna(row.get('roadNm')) else ''

        def clean_num(n):
            if pd.isna(n) or str(n) == 'nan':
                return ''
            try:
                return str(int(float(str(n))))
            except:
                return str(n).strip()

        if road and road != 'nan':
            bon = clean_num(row.get('roadNmBonbun', ''))
            bu = clean_num(row.get('roadNmBubun', ''))
            addr = f"{sgg} {road}"
            if bon and bon != '0':
                addr += f" {bon}"
                if bu and bu != '0':
                    addr += f"-{bu}"
            return addr.strip()

        umd = str(row.get('umdNm', '')) if pd.notna(row.get('umdNm')) else ''
        real_jibun = str(row.get('jibun', '')) if pd.notna(row.get('jibun')) else ''
        return f"{sgg} {umd} {real_jibun}".strip()

    df_combined['search_addr'] = df_combined.apply(build_address, axis=1)

    if len(df_combined) > 0:
        print("지오코딩 시작 (20 threads)...")
        with ThreadPoolExecutor(max_workers=20) as executor:
            future_to_row = {
                executor.submit(get_kakao_coords, row['search_addr']): row
                for _, row in df_combined.iterrows()
            }

            completed = 0
            for future in as_completed(future_to_row):
                row = future_to_row[future]
                try:
                    lat, lon, admin = future.result()
                except Exception:
                    lat, lon, admin = None, None, None

                all_results.append({
                    'apt_id': row['aptSeq'],
                    'apt_name': row.get('aptNm'),
                    'build_year': row.get('buildYear'),
                    'road_address': row.get('search_addr'),
                    'jibun_address': f"{row.get('umdNm', '')} {row.get('jibun', '')}",
                    'latitude': lat,
                    'longitude': lon,
                    'admin_dong': admin
                })

                completed += 1
                if completed % 100 == 0:
                    print(f"지오코딩 {completed}/{len(df_combined)}...")
                    pd.DataFrame(all_results).to_csv(apt_basic_file, index=False, encoding='utf-8-sig')

    df_basic = pd.DataFrame(all_results)
    df_basic.to_csv(apt_basic_file, index=False, encoding='utf-8-sig')
    print(f"저장 완료: {apt_basic_file} ({len(df_basic)}건)")
    return df_basic


# ==============================================================================
# 6. 가공: 상세 정보 (apt_detail_info_master)
# ==============================================================================

def process_detail_info(df_basic, basic_info_file, detail_info_file):
    """
    K-Apt 기본/상세 정보를 병합하고 apt_basic과 매핑하여
    apt_detail_info_master 파일을 생성합니다.
    """
    print("--- 상세 정보 가공 ---")

    if not os.path.exists(basic_info_file) or not os.path.exists(detail_info_file):
        print("K-Apt 파일 없음. 상세 정보 가공 건너뜀.")
        return

    df_kb = pd.read_csv(basic_info_file)
    df_kd = pd.read_csv(detail_info_file)

    # 중복 컬럼 방지하여 병합
    cols_to_use = df_kd.columns.difference(df_kb.columns).tolist()
    if 'kaptCode' not in cols_to_use:
        cols_to_use.append('kaptCode')
    df_k = pd.merge(df_kb, df_kd[cols_to_use], on='kaptCode', how='inner')

    # 총 주차수 계산
    if 'kaptdPcnt' in df_k.columns and 'kaptdPcntu' in df_k.columns:
        df_k['total_parking'] = df_k['kaptdPcnt'].fillna(0) + df_k['kaptdPcntu'].fillna(0)
    else:
        df_k['total_parking'] = 0

    # apt_basic과 매핑
    print("aptSeq ↔ kaptCode 매핑 중...")
    df_basic['norm_name'] = df_basic['apt_name'].apply(_normalize_string)
    df_basic['norm_dong'] = df_basic['admin_dong'].fillna('').apply(_normalize_string)

    df_k['norm_name'] = df_k['kaptName'].apply(_normalize_string)

    def get_dong(addr):
        if pd.isna(addr):
            return ""
        for p in addr.split():
            if p.endswith('동'):
                return p
        return ""

    df_k['norm_dong'] = df_k['kaptAddr'].apply(get_dong).apply(_normalize_string)

    # 이름+동 매핑
    basic_map = {}
    for _, row in df_basic.iterrows():
        key = (row['norm_name'], row['norm_dong'])
        if key not in basic_map:
            basic_map[key] = row['apt_id']

    matched_ids = []
    need_geo = []
    for idx, row in df_k.iterrows():
        key = (row['norm_name'], row['norm_dong'])
        m_id = basic_map.get(key)
        matched_ids.append(m_id)
        if m_id is None:
            need_geo.append(idx)

    df_k['apt_id'] = matched_ids
    print(f"이름 매칭: {df_k['apt_id'].notna().sum()}/{len(df_k)}")

    # 공간 매칭
    to_geo = df_k.loc[need_geo]
    if len(to_geo) > 0:
        print(f"공간 매칭 {len(to_geo)}건...")
        with ThreadPoolExecutor(max_workers=20) as executor:
            future_to_idx = {
                executor.submit(get_kakao_coords, row['kaptAddr']): idx
                for idx, row in to_geo.iterrows()
            }
            geo_map = {}
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    lat, lon, _ = future.result()
                    if lat and lon:
                        geo_map[idx] = (float(lat), float(lon))
                except:
                    pass

        valid_basic = df_basic.dropna(subset=['latitude', 'longitude']).copy()
        if len(valid_basic) > 0:
            targets_lat = valid_basic['latitude'].astype(float).values
            targets_lon = valid_basic['longitude'].astype(float).values
            basic_ids = valid_basic['apt_id'].values

            q_indices, q_lats, q_lons = [], [], []
            for idx in need_geo:
                if idx in geo_map:
                    q_indices.append(idx)
                    lat, lon = geo_map[idx]
                    q_lats.append(lat)
                    q_lons.append(lon)

            q_lats = np.array(q_lats)
            q_lons = np.array(q_lons)
            count_spatial = 0
            chunk_size = 100

            for i in range(0, len(q_indices), chunk_size):
                c_lats = q_lats[i:i + chunk_size]
                c_lons = q_lons[i:i + chunk_size]
                c_indices = q_indices[i:i + chunk_size]

                d_lat = c_lats[:, np.newaxis] - targets_lat[np.newaxis, :]
                d_lon = c_lons[:, np.newaxis] - targets_lon[np.newaxis, :]
                dists_sq = d_lat ** 2 + d_lon ** 2
                threshold_sq = 0.0005 ** 2

                min_dists_sq = np.min(dists_sq, axis=1)
                min_indices = np.argmin(dists_sq, axis=1)

                for j, d_sq in enumerate(min_dists_sq):
                    if d_sq < threshold_sq:
                        df_k.at[c_indices[j], 'apt_id'] = basic_ids[min_indices[j]]
                        count_spatial += 1

            print(f"공간 매칭 추가: {count_spatial}건")

    print(f"최종 매칭: {df_k['apt_id'].notna().sum()}/{len(df_k)}")

    # 컬럼 리네임
    rename_map = {
        'kaptCode': 'complex_id', 'kaptName': 'complex_name', 'doroJuso': 'road_address',
        'codeSaleNm': 'sale_type', 'kaptdaCnt': 'household_count', 'codeAptNm': 'apartment_type',
        'kaptUsedate': 'approval_date', 'convenientFacility': 'convenient_facilities',
        'educationFacility': 'education_facilities', 'kaptdWtimebus': 'bus_station_walking_time',
        'kaptdWtimesub': 'subway_station_walking_time', 'subwayLine': 'subway_line',
        'subwayStation': 'subway_station', 'total_parking': 'total_parking_count'
    }
    df_k.rename(columns=rename_map, inplace=True)

    target_cols = list(rename_map.values()) + ['apt_id']
    final_cols = [c for c in target_cols if c in df_k.columns]
    df_final = df_k[final_cols].copy()

    out_path = os.path.join(DATA_DIR, f"apt_detail_info_master_{get_today_str()}.csv")
    df_final.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f"저장 완료: {out_path} ({len(df_final)}건)")


# ==============================================================================
# 7. 가공: 매매/전월세 데이터 (apt_trade/rent_master)
# ==============================================================================

def _parse_money(x):
    try:
        return float(str(x).replace(',', ''))
    except:
        return 0.0


def process_trade_rent(trade_file, rent_file=None):
    """매매/전월세 원본 데이터를 가공하여 master 파일을 생성합니다."""
    print("--- 매매/전월세 데이터 가공 ---")
    today_str = get_today_str()

    # — 매매 가공 —
    if os.path.exists(trade_file):
        df_trade = pd.read_csv(trade_file, low_memory=False)

        if 'excluUseAr' in df_trade.columns:
            df_trade['excluUseAr'] = pd.to_numeric(df_trade['excluUseAr'], errors='coerce').fillna(0).astype(int)
            df_trade = df_trade[df_trade['excluUseAr'] > 0]

        df_trade['deal_date'] = pd.to_datetime(
            df_trade['dealYear'].astype(str) + "-" +
            df_trade['dealMonth'].astype(str).str.zfill(2) + "-" +
            df_trade['dealDay'].astype(str).str.zfill(2), errors='coerce'
        )

        t_rename = {
            'aptSeq': 'apt_id', 'aptNm': 'apartment_name', 'dealAmount': 'deal_amount',
            'excluUseAr': 'exclusive_area', 'floor': 'floor', 'buyerGbn': 'buyer_type',
            'slerGbn': 'seller_type', 'dealingGbn': 'dealing_type', 'rgstDate': 'registration_date',
        }
        df_trade.rename(columns=t_rename, inplace=True)

        df_trade['deal_amount'] = df_trade['deal_amount'].astype(str).str.replace(',', '').astype(float)
        df_trade.sort_values(by=['apt_id', 'exclusive_area', 'deal_date'], inplace=True)
        df_trade['previous_deal_amount'] = df_trade.groupby(['apt_id', 'exclusive_area'])['deal_amount'].shift(1)
        df_trade['deal_diff'] = df_trade['deal_amount'] - df_trade['previous_deal_amount']
        df_trade['deal_diff_rate'] = ((df_trade['deal_diff'] / df_trade['previous_deal_amount']) * 100).round(2)

        t_cols = ['apt_id', 'apartment_name', 'deal_date', 'deal_amount', 'exclusive_area', 'floor',
                  'buyer_type', 'seller_type', 'dealing_type', 'registration_date',
                  'previous_deal_amount', 'deal_diff', 'deal_diff_rate']
        final_cols = [c for c in t_cols if c in df_trade.columns]

        out_t = os.path.join(DATA_DIR, f"apt_trade_master_{today_str}.csv")
        df_trade[final_cols].to_csv(out_t, index=False, encoding='utf-8-sig')
        print(f"저장 완료: {out_t}")

    # — 전월세 가공 —
    if rent_file and os.path.exists(rent_file):
        df_rent = pd.read_csv(rent_file, low_memory=False)

        if 'excluUseAr' in df_rent.columns:
            df_rent['excluUseAr'] = pd.to_numeric(df_rent['excluUseAr'], errors='coerce').fillna(0).astype(int)
            df_rent = df_rent[df_rent['excluUseAr'] > 0]

        df_rent['deal_date'] = pd.to_datetime(
            df_rent['dealYear'].astype(str) + "-" +
            df_rent['dealMonth'].astype(str).str.zfill(2) + "-" +
            df_rent['dealDay'].astype(str).str.zfill(2), errors='coerce'
        )

        r_rename = {
            'aptSeq': 'apt_id', 'aptNm': 'apartment_name', 'deposit': 'deposit',
            'monthlyRent': 'monthly_rent', 'excluUseAr': 'exclusive_area', 'floor': 'floor',
            'contractTerm': 'contract_term', 'contractType': 'contract_type',
        }
        df_rent.rename(columns=r_rename, inplace=True)

        df_rent['deposit'] = df_rent['deposit'].apply(_parse_money)
        df_rent['monthly_rent'] = df_rent['monthly_rent'].apply(_parse_money)

        df_rent['rental_adjusted_deposit'] = df_rent['deposit']
        mask = df_rent['monthly_rent'] > 0
        df_rent.loc[mask, 'rental_adjusted_deposit'] = (
            df_rent.loc[mask, 'deposit'] + (df_rent.loc[mask, 'monthly_rent'] * 12 / 0.045)
        )

        df_rent.sort_values(by=['apt_id', 'exclusive_area', 'deal_date'], inplace=True)
        df_rent['previous_rental_adjusted_deposit'] = (
            df_rent.groupby(['apt_id', 'exclusive_area'])['rental_adjusted_deposit'].shift(1)
        )
        df_rent['deposit_diff'] = df_rent['rental_adjusted_deposit'] - df_rent['previous_rental_adjusted_deposit']
        df_rent['deposit_diff_rate'] = (
            (df_rent['deposit_diff'] / df_rent['previous_rental_adjusted_deposit']) * 100
        ).round(2)

        r_cols = ['apt_id', 'apartment_name', 'deal_date', 'deposit', 'monthly_rent',
                  'rental_adjusted_deposit', 'exclusive_area', 'floor', 'contract_term', 'contract_type',
                  'deposit_diff', 'deposit_diff_rate']
        final_cols = [c for c in r_cols if c in df_rent.columns]

        out_r = os.path.join(DATA_DIR, f"apt_rent_master_{today_str}.csv")
        df_rent[final_cols].to_csv(out_r, index=False, encoding='utf-8-sig')
        print(f"저장 완료: {out_r}")

    print("매매/전월세 가공 완료.")


# ==============================================================================
# 메인 실행
# ==============================================================================

def main(regions, months_back, skip_code=False, skip_basic=False, skip_trade=False, max_basic=10000):
    """전체 수집 및 가공 파이프라인을 실행합니다."""
    print("=" * 60)
    print(f"아파트 데이터 수집 및 가공 시작")
    print(f"  대상 지역: {regions}")
    print(f"  수집 기간: 최근 {months_back}개월")
    print("=" * 60)

    today_str = get_today_str()
    # 기본 파일명 설정
    code_file = os.path.join(DATA_DIR, f"apt_code_{today_str}.csv")
    basic_info_file = os.path.join(DATA_DIR, f"apt_basic_info_{today_str}.csv")
    detail_info_file = os.path.join(DATA_DIR, f"apt_detail_info_{today_str}.csv")
    trade_file = os.path.join(DATA_DIR, f"apt_trade_{today_str}.csv")
    rent_file = os.path.join(DATA_DIR, f"apt_rent_{today_str}.csv")

    # Step 1: 단지 코드 수집
    if not skip_code:
        print("\n[Step 1/4] 단지 코드 수집")
        code_file_res = collect_apt_codes(regions)
        if code_file_res: code_file = code_file_res
    else:
        print("\n[Step 1/4] 단지 코드 수집 건너뜀")

    # Step 2: 기본/상세 정보 수집
    if not skip_basic:
        print("\n[Step 2/4] 기본/상세 정보 수집")
        res = collect_all_info(code_file, max_items=max_basic)
        if res and len(res) == 2:
            basic_info_file, detail_info_file = res
    else:
        print("\n[Step 2/4] 기본/상세 정보 수집 건너뜀")

    # Step 3: 매매/전월세 데이터 수집
    if not skip_trade:
        print("\n[Step 3/4] 매매/전월세 거래 데이터 수집")
        res = collect_all_trade_rent(code_file, months_back)
        if res and len(res) == 2:
            trade_file, rent_file = res
    else:
        print("\n[Step 3/4] 매매/전월세 거래 데이터 수집 건너뜀")

    # Step 4: 데이터 가공
    print("\n[Step 4/4] 데이터 가공")
    df_basic = process_basic_info(trade_file, rent_file)
    if df_basic is not None:
        process_detail_info(df_basic, basic_info_file, detail_info_file)
    process_trade_rent(trade_file, rent_file)

    print("\n" + "=" * 60)
    print("수집 및 가공 완료!")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="아파트 데이터 수집 및 가공")
    parser.add_argument('--regions', nargs='+', default=['11', '28', '41'],
                        help='시도 코드 목록 (기본값: 11 28 41 = 서울, 인천, 경기)')
    parser.add_argument('--months', type=int, default=36,
                        help='수집 기간(개월) (기본값: 36)')
    parser.add_argument('--skip-code', action='store_true', help='단지 코드 수집 건너뛰기')
    parser.add_argument('--skip-basic', action='store_true', help='기본/상세 정보(K-Apt) 수집 건너뛰기')
    parser.add_argument('--skip-trade', action='store_true', help='매매/전월세 수집 건너뛰기')
    parser.add_argument('--max-basic', type=int, default=10000, 
                        help='하루 최대 기본/상세 정보 수집 건수 제한 (기본값: 10000)')
    
    args = parser.parse_args()
    main(args.regions, args.months, args.skip_code, args.skip_basic, args.skip_trade, args.max_basic)
