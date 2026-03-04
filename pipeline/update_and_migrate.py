"""
데이터 업데이트 및 DB 마이그레이션 스크립트

일일 업데이트 → DB 마이그레이션 → 파일 정리 → 보고서 생성

사용법:
    python update_and_migrate.py                # 전체 실행
    python update_and_migrate.py --skip-db      # DB 마이그레이션 건너뛰기
    python update_and_migrate.py --skip-cleanup  # 파일 정리 건너뛰기
"""

import argparse
import os
import sys
import glob
import time
import subprocess
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text

from utils import (
    fetch_data, save_to_csv, get_today_str, get_latest_file,
    get_db_engine, DATA_DIR, KAKAO_API_KEY
)


# ==============================================================================
# 1. 증분 수집: 아파트 기본/상세 정보
# ==============================================================================

def _get_basic_info(kapt_code):
    """단일 아파트 기본 정보 수집 (K-Apt API)"""
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
    """단일 아파트 상세 정보 수집 (K-Apt API)"""
    url = "https://apis.data.go.kr/1613000/AptBasisInfoServiceV4/getAphusDtlInfoV4"
    params = {"kaptCode": kapt_code}
    data = fetch_data(url, params)
    if data and isinstance(data, dict):
        try:
            return data.get('response', {}).get('body', {}).get('item', {})
        except:
            return None
    return None


def collect_info_incremental(kapt_codes):
    """주어진 코드 목록에 대해 기본/상세 정보를 증분 수집합니다."""
    if not kapt_codes:
        print("수집할 아파트 코드가 없습니다.")
        return [], []

    print(f"상세 정보 수집 대상: {len(kapt_codes)}개 단지")
    basic_infos, detail_infos = [], []

    for i, code in enumerate(kapt_codes, 1):
        if i % 10 == 0:
            print(f"정보 수집 {i}/{len(kapt_codes)}...")
        b = _get_basic_info(code)
        if b:
            basic_infos.append(b)
        d = _get_detail_info(code)
        if d:
            detail_infos.append(d)
        time.sleep(0.1)

    return basic_infos, detail_infos


# ==============================================================================
# 2. 증분 수집: 매매/전월세 데이터
# ==============================================================================

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


def collect_data_incremental(target_months, lawd_codes=None):
    """지정 월/지역에 대해 매매/전월세 데이터를 증분 수집합니다."""
    if lawd_codes is None:
        latest_code = get_latest_file("apt_code_*.csv")
        if not latest_code:
            print("apt_code CSV 파일을 찾을 수 없습니다.")
            return [], []
        df = pd.read_csv(latest_code)
        df['bjdCode'] = df['bjdCode'].astype(str)
        lawd_codes = df['bjdCode'].str[:5].unique()

    print(f"수집 대상: {len(lawd_codes)}개 지역, {len(target_months)}개 월")
    all_trades, all_rents = [], []
    count = 0
    total = len(lawd_codes) * len(target_months)

    for lawd in lawd_codes:
        for ym in target_months:
            count += 1
            if count % 10 == 0:
                print(f"진행 중 {count}/{total}")
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

    return all_trades, all_rents


# ==============================================================================
# 3. 스키마 변환 (원본 → 가공 스키마)
# ==============================================================================

def convert_to_basic_schema(df_raw_infos):
    """K-Apt 기본 정보 → apt_basic 스키마로 변환합니다."""
    import re
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed

    print("--- 기본 정보 스키마 변환 ---")
    if isinstance(df_raw_infos, list):
        df = pd.DataFrame(df_raw_infos)
    else:
        df = df_raw_infos.copy()

    if df.empty:
        return pd.DataFrame()

    def get_kakao_coords(address):
        if not KAKAO_API_KEY:
            return None, None, None
        url = "https://dapi.kakao.com/v2/local/search/address.json"
        headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
        params = {"query": address}
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=5, verify=False)
            if resp.status_code == 200:
                data = resp.json()
                if data['documents']:
                    doc = data['documents'][0]
                    y, x = doc['y'], doc['x']
                    admin_dong = None
                    try:
                        c_resp = requests.get(
                            "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json",
                            headers=headers, params={"x": x, "y": y}, timeout=5, verify=False
                        )
                        if c_resp.status_code == 200:
                            for region in c_resp.json().get('documents', []):
                                if region['region_type'] == 'H':
                                    admin_dong = region['region_3depth_name']
                                    break
                    except:
                        pass
                    if not admin_dong:
                        admin_dong = doc['address']['region_3depth_name'] if doc.get('address') else None
                    return y, x, admin_dong
        except:
            pass
        return None, None, None

    results = []
    print(f"지오코딩 {len(df)}건...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_row = {
            executor.submit(get_kakao_coords, row.get('doroJuso', row.get('kaptAddr', ''))): row
            for _, row in df.iterrows()
        }
        for future in as_completed(future_to_row):
            row = future_to_row[future]
            try:
                lat, lon, admin = future.result()
            except:
                lat, lon, admin = None, None, None
            results.append({
                'apt_id': row.get('kaptCode'),
                'apt_name': row.get('kaptName'),
                'build_year': str(row.get('kaptUsedate', ''))[:4] if row.get('kaptUsedate') else '',
                'road_address': row.get('doroJuso'),
                'jibun_address': row.get('kaptAddr'),
                'latitude': lat,
                'longitude': lon,
                'admin_dong': admin
            })

    return pd.DataFrame(results)


def convert_to_detail_schema(df_basic_infos, df_detail_infos):
    """K-Apt 기본+상세 정보 → apt_detail 스키마로 변환합니다."""
    print("--- 상세 정보 스키마 변환 ---")
    if isinstance(df_basic_infos, list):
        df_kb = pd.DataFrame(df_basic_infos)
    else:
        df_kb = df_basic_infos.copy()

    if isinstance(df_detail_infos, list):
        df_kd = pd.DataFrame(df_detail_infos)
    else:
        df_kd = df_detail_infos.copy()

    if df_kb.empty or df_kd.empty:
        return pd.DataFrame()

    cols_to_use = df_kd.columns.difference(df_kb.columns).tolist()
    if 'kaptCode' not in cols_to_use:
        cols_to_use.append('kaptCode')
    df_k = pd.merge(df_kb, df_kd[cols_to_use], on='kaptCode', how='inner')

    if 'kaptdPcnt' in df_k.columns and 'kaptdPcntu' in df_k.columns:
        df_k['total_parking'] = pd.to_numeric(df_k['kaptdPcnt'], errors='coerce').fillna(0) + \
                                pd.to_numeric(df_k['kaptdPcntu'], errors='coerce').fillna(0)
    else:
        df_k['total_parking'] = 0

    rename_map = {
        'kaptCode': 'complex_id', 'kaptName': 'complex_name', 'doroJuso': 'road_address',
        'codeSaleNm': 'sale_type', 'kaptdaCnt': 'household_count', 'codeAptNm': 'apartment_type',
        'kaptUsedate': 'approval_date', 'convenientFacility': 'convenient_facilities',
        'educationFacility': 'education_facilities', 'kaptdWtimebus': 'bus_station_walking_time',
        'kaptdWtimesub': 'subway_station_walking_time', 'subwayLine': 'subway_line',
        'subwayStation': 'subway_station', 'total_parking': 'total_parking_count'
    }
    df_k.rename(columns=rename_map, inplace=True)

    target_cols = list(rename_map.values())
    if 'apt_id' not in df_k.columns:
        df_k['apt_id'] = None
    target_cols.append('apt_id')
    final_cols = [c for c in target_cols if c in df_k.columns]

    return df_k[final_cols]


def convert_to_trade_schema(df_raw_trade):
    """원본 매매 데이터 → 가공 스키마로 변환합니다."""
    print("--- 매매 스키마 변환 ---")
    if isinstance(df_raw_trade, list):
        df = pd.DataFrame(df_raw_trade)
    else:
        df = df_raw_trade.copy()

    if df.empty:
        return pd.DataFrame()

    if 'excluUseAr' in df.columns:
        df['excluUseAr'] = pd.to_numeric(df['excluUseAr'], errors='coerce').fillna(0).astype(int)
        df = df[df['excluUseAr'] > 0]

    df['deal_date'] = pd.to_datetime(
        df['dealYear'].astype(str) + "-" +
        df['dealMonth'].astype(str).str.zfill(2) + "-" +
        df['dealDay'].astype(str).str.zfill(2), errors='coerce'
    )

    t_rename = {
        'aptSeq': 'apt_id', 'aptNm': 'apartment_name', 'dealAmount': 'deal_amount',
        'excluUseAr': 'exclusive_area', 'floor': 'floor', 'buyerGbn': 'buyer_type',
        'slerGbn': 'seller_type', 'dealingGbn': 'dealing_type',
        'cdealType': 'cancellation_deal_type', 'cdealDay': 'cancellation_deal_day',
        'rgstDate': 'registration_date'
    }
    df.rename(columns=t_rename, inplace=True)

    def safe_float(x):
        try:
            return float(str(x).replace(',', ''))
        except:
            return 0.0

    df['deal_amount'] = df['deal_amount'].apply(safe_float)
    df.sort_values(by=['apt_id', 'exclusive_area', 'deal_date'], inplace=True)
    df['previous_deal_amount'] = df.groupby(['apt_id', 'exclusive_area'])['deal_amount'].shift(1)
    df['deal_diff'] = df['deal_amount'] - df['previous_deal_amount']
    df['deal_diff_rate'] = 0.0
    mask = df['previous_deal_amount'] != 0
    df.loc[mask, 'deal_diff_rate'] = ((df.loc[mask, 'deal_diff'] / df.loc[mask, 'previous_deal_amount']) * 100).round(2)

    t_cols = ['apt_id', 'apartment_name', 'deal_date', 'deal_amount', 'exclusive_area', 'floor',
              'buyer_type', 'seller_type', 'dealing_type', 'cancellation_deal_type',
              'cancellation_deal_day', 'registration_date',
              'previous_deal_amount', 'deal_diff', 'deal_diff_rate']
    for c in t_cols:
        if c not in df.columns:
            df[c] = None
    return df[t_cols]


def convert_to_rent_schema(df_raw_rent):
    """원본 전월세 데이터 → 가공 스키마로 변환합니다."""
    print("--- 전월세 스키마 변환 ---")
    if isinstance(df_raw_rent, list):
        df = pd.DataFrame(df_raw_rent)
    else:
        df = df_raw_rent.copy()

    if df.empty:
        return pd.DataFrame()

    if 'excluUseAr' in df.columns:
        df['excluUseAr'] = pd.to_numeric(df['excluUseAr'], errors='coerce').fillna(0).astype(int)
        df = df[df['excluUseAr'] > 0]

    df['deal_date'] = pd.to_datetime(
        df['dealYear'].astype(str) + "-" +
        df['dealMonth'].astype(str).str.zfill(2) + "-" +
        df['dealDay'].astype(str).str.zfill(2), errors='coerce'
    )

    r_rename = {
        'aptSeq': 'apt_id', 'aptNm': 'apartment_name', 'deposit': 'deposit',
        'monthlyRent': 'monthly_rent', 'excluUseAr': 'exclusive_area', 'floor': 'floor',
        'contractTerm': 'contract_term', 'contractType': 'contract_type'
    }
    df.rename(columns=r_rename, inplace=True)

    def parse_money(x):
        try:
            return float(str(x).replace(',', ''))
        except:
            return 0.0

    df['deposit'] = df['deposit'].apply(parse_money)
    df['monthly_rent'] = df['monthly_rent'].apply(parse_money)
    df['rental_adjusted_deposit'] = df['deposit']
    mask = df['monthly_rent'] > 0
    df.loc[mask, 'rental_adjusted_deposit'] = df.loc[mask, 'deposit'] + (df.loc[mask, 'monthly_rent'] * 12 / 0.045)

    df.sort_values(by=['apt_id', 'exclusive_area', 'deal_date'], inplace=True)
    df['previous_rental_adjusted_deposit'] = df.groupby(['apt_id', 'exclusive_area'])['rental_adjusted_deposit'].shift(1)
    df['deposit_diff'] = df['rental_adjusted_deposit'] - df['previous_rental_adjusted_deposit']
    df['deposit_diff_rate'] = 0.0
    mask2 = df['previous_rental_adjusted_deposit'] != 0
    df.loc[mask2, 'deposit_diff_rate'] = (
        (df.loc[mask2, 'deposit_diff'] / df.loc[mask2, 'previous_rental_adjusted_deposit']) * 100
    ).round(2)

    r_cols = ['apt_id', 'apartment_name', 'deal_date', 'deposit', 'monthly_rent',
              'rental_adjusted_deposit', 'exclusive_area', 'floor', 'contract_term', 'contract_type',
              'previous_rental_adjusted_deposit', 'deposit_diff', 'deposit_diff_rate']
    for c in r_cols:
        if c not in df.columns:
            df[c] = None
    return df[r_cols]


# ==============================================================================
# 4. 일일 업데이트 단계
# ==============================================================================

def step_1_update_codes():
    """Step 1: 아파트 단지 코드 업데이트 및 신규 코드 추출"""
    print(">>> Step 1: 아파트 단지 코드 업데이트")

    prev_code_file = get_latest_file("apt_code_*.csv")

    # collect_and_process의 수집 함수 임포트
    from collect_and_process import collect_apt_codes
    collect_apt_codes(['11', '28', '41'])

    curr_code_file = get_latest_file("apt_code_*.csv")
    print(f"이전: {prev_code_file}")
    print(f"현재: {curr_code_file}")

    new_codes = []
    if prev_code_file and curr_code_file and prev_code_file != curr_code_file:
        df_prev = pd.read_csv(prev_code_file)
        df_curr = pd.read_csv(curr_code_file)
        prev_set = set(df_prev['kaptCode'].astype(str))
        curr_set = set(df_curr['kaptCode'].astype(str))
        new_codes = list(curr_set - prev_set)
        print(f"신규 단지 코드: {len(new_codes)}개")
    else:
        print("변경 없음 (신규 코드 없음)")

    return new_codes


def step_2_update_info(new_codes):
    """Step 2: 아파트 기본/상세 정보 업데이트 (가공 스키마)"""
    print(">>> Step 2: 아파트 상세 정보 업데이트")

    kb_master = get_latest_file("apt_basic_info_master_*.csv")
    kd_master = get_latest_file("apt_detail_info_master_*.csv")

    df_kb = pd.DataFrame()
    df_kd = pd.DataFrame()

    # 기존 마스터 로드
    if kb_master:
        print(f"기본 마스터 로드: {kb_master}")
        df_kb = pd.read_csv(kb_master)
        if 'kaptCode' in df_kb.columns and 'latitude' not in df_kb.columns:
            print("RAW 기본 마스터 감지. 스키마 변환 중...")
            df_kb = convert_to_basic_schema(df_kb)

    if kd_master:
        print(f"상세 마스터 로드: {kd_master}")
        df_kd = pd.read_csv(kd_master)
        if 'kaptCode' in df_kd.columns and 'household_count' not in df_kd.columns:
            print("RAW 상세 마스터 감지. 초기화...")
            df_kd = pd.DataFrame()

    print(f"기존 데이터: 기본 {len(df_kb)}건, 상세 {len(df_kd)}건")

    if new_codes:
        raw_b, raw_d = collect_info_incremental(new_codes)

        if raw_b:
            df_new_b = convert_to_basic_schema(raw_b)
            if not df_new_b.empty:
                df_kb = pd.concat([df_kb, df_new_b], ignore_index=True)
                if 'apt_id' in df_kb.columns:
                    df_kb.drop_duplicates(subset=['apt_id'], keep='last', inplace=True)

        if raw_d:
            df_new_d = convert_to_detail_schema(raw_b, raw_d)
            if not df_new_d.empty:
                df_kd = pd.concat([df_kd, df_new_d], ignore_index=True)
                if 'complex_id' in df_kd.columns:
                    df_kd.drop_duplicates(subset=['complex_id'], keep='last', inplace=True)

    today_str = get_today_str()
    new_kb = os.path.join(DATA_DIR, f"apt_basic_info_master_{today_str}.csv")
    new_kd = os.path.join(DATA_DIR, f"apt_detail_info_master_{today_str}.csv")
    df_kb.to_csv(new_kb, index=False, encoding='utf-8-sig')
    df_kd.to_csv(new_kd, index=False, encoding='utf-8-sig')
    print(f"저장: {new_kb}, {new_kd}")

    return len(df_kb), len(df_kd)


def step_3_update_trade_rent():
    """Step 3: 매매/전월세 데이터 업데이트 (최근 3개월 + 36개월 보존)
    
    Returns:
        tuple: (매매 건수, 전월세 건수, 원본 매매 리스트, 원본 전월세 리스트)
            원본 리스트는 step_3b에서 신규 apt_id 감지에 사용됩니다.
    """
    print(">>> Step 3: 매매/전월세 데이터 업데이트")

    today = datetime.datetime.now()
    target_months = [(today - relativedelta(months=i)).strftime("%Y%m") for i in range(3)]
    print(f"수집 대상 월: {target_months}")

    new_trades, new_rents = collect_data_incremental(target_months)
    df_new_t = convert_to_trade_schema(new_trades)
    df_new_r = convert_to_rent_schema(new_rents)
    print(f"가공 완료: 매매 {len(df_new_t)}건, 전월세 {len(df_new_r)}건")

    # 기존 마스터 로드
    pd.set_option('future.no_silent_downcasting', True)

    t_master = get_latest_file("apt_trade_master_*.csv")
    df_master_t = pd.DataFrame()
    if t_master:
        print(f"매매 마스터 로드: {t_master}")
        df_master_t = pd.read_csv(t_master, low_memory=False)
        if 'deal_date' not in df_master_t.columns and 'aptSeq' in df_master_t.columns:
            print("RAW 매매 마스터 감지. 스키마 변환 중...")
            df_master_t = convert_to_trade_schema(df_master_t)

    r_master = get_latest_file("apt_rent_master_*.csv")
    df_master_r = pd.DataFrame()
    if r_master:
        print(f"전월세 마스터 로드: {r_master}")
        df_master_r = pd.read_csv(r_master, low_memory=False)
        if 'deal_date' not in df_master_r.columns and 'aptSeq' in df_master_r.columns:
            print("RAW 전월세 마스터 감지. 스키마 변환 중...")
            df_master_r = convert_to_rent_schema(df_master_r)

    # 보존 및 병합
    cutoff_date = pd.to_datetime((today - relativedelta(months=36)).strftime("%Y-%m-%d"))

    def merge_and_filter(df_master, df_new, target_months_list):
        if df_master.empty and df_new.empty:
            return pd.DataFrame()
        if not df_master.empty:
            df_master['deal_date'] = pd.to_datetime(df_master['deal_date'], errors='coerce')
            df_master = df_master[df_master['deal_date'] >= cutoff_date]
            df_master['ym'] = df_master['deal_date'].dt.strftime('%Y%m')
            df_master = df_master[~df_master['ym'].isin(target_months_list)]
            df_master = df_master.drop(columns=['ym'])
        if not df_new.empty:
            df_new['deal_date'] = pd.to_datetime(df_new['deal_date'], errors='coerce')
            return pd.concat([df_master, df_new], ignore_index=True)
        return df_master

    df_final_t = merge_and_filter(df_master_t, df_new_t, target_months)
    df_final_r = merge_and_filter(df_master_r, df_new_r, target_months)

    today_str = get_today_str()
    new_t = os.path.join(DATA_DIR, f"apt_trade_master_{today_str}.csv")
    new_r = os.path.join(DATA_DIR, f"apt_rent_master_{today_str}.csv")
    df_final_t.to_csv(new_t, index=False, encoding='utf-8-sig')
    df_final_r.to_csv(new_r, index=False, encoding='utf-8-sig')
    print(f"저장: {new_t}, {new_r}")

    return len(df_final_t), len(df_final_r), new_trades, new_rents


def step_3b_update_basic_from_trades(raw_trades, raw_rents):
    """Step 3b: 매매/전월세 데이터에서 신규 apt_id를 감지하여 기본 정보 마스터에 추가합니다.
    
    수집된 거래 데이터에 새로운 아파트(apt_id)가 포함된 경우,
    원본 데이터에서 주소를 추출하고 Kakao API로 지오코딩하여
    apt_basic_info_master에 자동 추가합니다.
    """
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed

    print(">>> Step 3b: 매매/전월세 신규 apt_id 감지 및 기본 정보 추가")

    # 원본 데이터를 DataFrame으로 변환
    df_raw_t = pd.DataFrame(raw_trades) if raw_trades else pd.DataFrame()
    df_raw_r = pd.DataFrame(raw_rents) if raw_rents else pd.DataFrame()

    # 원본 데이터에서 고유 apt_id(aptSeq) 추출
    all_raw = pd.concat([df_raw_t, df_raw_r], ignore_index=True)
    if all_raw.empty or 'aptSeq' not in all_raw.columns:
        print("원본 거래 데이터가 없습니다.")
        return 0

    raw_apt_ids = set(all_raw['aptSeq'].dropna().astype(str).unique())
    print(f"수집된 거래 데이터의 고유 apt_id: {len(raw_apt_ids)}개")

    # 기존 기본 정보 마스터 로드
    kb_master = get_latest_file("apt_basic_info_master_*.csv")
    if kb_master:
        df_kb = pd.read_csv(kb_master)
        existing_ids = set(df_kb['apt_id'].dropna().astype(str).unique())
    else:
        df_kb = pd.DataFrame()
        existing_ids = set()

    # 신규 apt_id 감지
    new_apt_ids = raw_apt_ids - existing_ids
    if not new_apt_ids:
        print("신규 apt_id 없음. 기본 정보 마스터 변경 없음.")
        return 0

    print(f"신규 apt_id 발견: {len(new_apt_ids)}개")

    # 신규 apt_id에 대한 대표 행 추출 (첫 번째 거래 기록에서 주소 정보 확보)
    df_new_apts = all_raw[all_raw['aptSeq'].astype(str).isin(new_apt_ids)].copy()
    df_new_apts = df_new_apts.drop_duplicates(subset=['aptSeq'], keep='first')

    # 주소 구성
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
        jibun = str(row.get('jibun', '')) if pd.notna(row.get('jibun')) else ''
        return f"{sgg} {umd} {jibun}".strip()

    def build_jibun_address(row):
        sgg = str(row.get('sggNm', '')) if pd.notna(row.get('sggNm')) else ''
        umd = str(row.get('umdNm', '')) if pd.notna(row.get('umdNm')) else ''
        jibun = str(row.get('jibun', '')) if pd.notna(row.get('jibun')) else ''
        return f"{sgg} {umd} {jibun}".strip()

    # 지오코딩
    def get_kakao_coords(address):
        if not KAKAO_API_KEY:
            return None, None, None
        url = "https://dapi.kakao.com/v2/local/search/address.json"
        headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
        params = {"query": address}
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=5, verify=False)
            if resp.status_code == 200:
                data = resp.json()
                if data['documents']:
                    doc = data['documents'][0]
                    y, x = doc['y'], doc['x']
                    admin_dong = None
                    try:
                        c_resp = requests.get(
                            "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json",
                            headers=headers, params={"x": x, "y": y}, timeout=5, verify=False
                        )
                        if c_resp.status_code == 200:
                            for region in c_resp.json().get('documents', []):
                                if region['region_type'] == 'H':
                                    admin_dong = region['region_3depth_name']
                                    break
                    except:
                        pass
                    if not admin_dong:
                        admin_dong = doc['address']['region_3depth_name'] if doc.get('address') else None
                    return y, x, admin_dong
        except:
            pass
        return None, None, None

    # 병렬 지오코딩 실행
    results = []
    print(f"신규 apt_id {len(df_new_apts)}건 지오코딩 중...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_row = {
            executor.submit(get_kakao_coords, build_address(row)): row
            for _, row in df_new_apts.iterrows()
        }
        for future in as_completed(future_to_row):
            row = future_to_row[future]
            try:
                lat, lon, admin = future.result()
            except:
                lat, lon, admin = None, None, None

            build_year = ''
            if pd.notna(row.get('buildYear')):
                try:
                    build_year = int(float(str(row['buildYear'])))
                except:
                    build_year = ''

            results.append({
                'apt_id': str(row.get('aptSeq')),
                'apt_name': row.get('aptNm', ''),
                'build_year': build_year,
                'road_address': build_address(row),
                'jibun_address': build_jibun_address(row),
                'latitude': lat,
                'longitude': lon,
                'admin_dong': admin
            })

    df_new_basic = pd.DataFrame(results)
    print(f"신규 기본 정보 {len(df_new_basic)}건 생성 완료")

    # 기존 마스터에 추가
    df_kb = pd.concat([df_kb, df_new_basic], ignore_index=True)
    df_kb.drop_duplicates(subset=['apt_id'], keep='last', inplace=True)

    today_str = get_today_str()
    new_kb = os.path.join(DATA_DIR, f"apt_basic_info_master_{today_str}.csv")
    df_kb.to_csv(new_kb, index=False, encoding='utf-8-sig')
    print(f"기본 정보 마스터 업데이트 저장: {new_kb} ({len(df_kb)}건, 신규 {len(new_apt_ids)}건 추가)")

    return len(new_apt_ids)


# ==============================================================================
# 5. DB 마이그레이션 (PostgreSQL)
# ==============================================================================

def create_schema(engine):
    """PostgreSQL 테이블 스키마 생성 (Drop & Create)"""
    print(">>> 스키마 생성 및 초기화")
    with engine.begin() as conn:
        conn.execute(text("""
            DROP TABLE IF EXISTS apt_basic CASCADE;
            CREATE TABLE apt_basic (
                apt_id VARCHAR(50) PRIMARY KEY,
                apt_name VARCHAR(100),
                build_year INTEGER,
                road_address VARCHAR(200),
                jibun_address VARCHAR(200),
                latitude FLOAT,
                longitude FLOAT,
                admin_dong VARCHAR(50)
            );
            COMMENT ON TABLE apt_basic IS '아파트 단지 기본 정보 (Basic Apartment Info)';
            COMMENT ON COLUMN apt_basic.apt_id IS '아파트 고유 코드 (Unique Apartment Identifier)';
            COMMENT ON COLUMN apt_basic.apt_name IS '아파트 단지명 (Apartment Complex Name)';
            COMMENT ON COLUMN apt_basic.build_year IS '건축 연도 (Year Built)';
            COMMENT ON COLUMN apt_basic.road_address IS '도로명 주소 (Road Address)';
            COMMENT ON COLUMN apt_basic.jibun_address IS '지번 주소 (Jibun Address)';
            COMMENT ON COLUMN apt_basic.latitude IS '위도 (Latitude)';
            COMMENT ON COLUMN apt_basic.longitude IS '경도 (Longitude)';
            COMMENT ON COLUMN apt_basic.admin_dong IS '행정동 (Administrative Dong)';
        """))

        conn.execute(text("""
            DROP TABLE IF EXISTS apt_detail CASCADE;
            CREATE TABLE apt_detail (
                complex_id VARCHAR(50) PRIMARY KEY,
                apt_id VARCHAR(50),
                complex_name VARCHAR(100),
                household_count INTEGER,
                approval_date DATE,
                total_parking_count INTEGER,
                sale_type VARCHAR(50),
                apartment_type VARCHAR(50),
                phone_number VARCHAR(20),
                fax_number VARCHAR(20),
                management_office_address VARCHAR(200),
                convenient_facilities TEXT,
                education_facilities TEXT,
                bus_station_walking_time VARCHAR(50),
                subway_station_walking_time VARCHAR(50),
                subway_line TEXT,
                subway_station TEXT
            );
            COMMENT ON TABLE apt_detail IS '아파트 상세 시설 정보 (Detailed Facility Info from K-Apt)';
            COMMENT ON COLUMN apt_detail.complex_id IS '단지 고유 코드 (kaptCode)';
            COMMENT ON COLUMN apt_detail.apt_id IS '매매/전월세 데이터와 연결되는 아파트 ID';
            COMMENT ON COLUMN apt_detail.household_count IS '총 세대수';
            COMMENT ON COLUMN apt_detail.total_parking_count IS '총 주차대수';
            COMMENT ON COLUMN apt_detail.subway_line IS '인근 지하철 노선';
            COMMENT ON COLUMN apt_detail.subway_station IS '인근 지하철역';
        """))

        conn.execute(text("""
            DROP TABLE IF EXISTS apt_trade CASCADE;
            CREATE TABLE apt_trade (
                id SERIAL PRIMARY KEY,
                apt_id VARCHAR(50),
                apartment_name VARCHAR(100),
                deal_date DATE,
                deal_amount FLOAT,
                exclusive_area FLOAT,
                floor INTEGER,
                buyer_type VARCHAR(50),
                seller_type VARCHAR(50),
                dealing_type VARCHAR(50),
                cancellation_deal_type VARCHAR(50),
                cancellation_deal_day VARCHAR(20),
                registration_date VARCHAR(20),
                previous_deal_amount FLOAT,
                deal_diff FLOAT,
                deal_diff_rate FLOAT
            );
            COMMENT ON TABLE apt_trade IS '아파트 매매 실거래가 (Trade Transaction History)';
            COMMENT ON COLUMN apt_trade.apt_id IS '아파트 고유 코드';
            COMMENT ON COLUMN apt_trade.deal_date IS '거래 일자 (YYYY-MM-DD)';
            COMMENT ON COLUMN apt_trade.deal_amount IS '거래 금액 (만원)';
            COMMENT ON COLUMN apt_trade.exclusive_area IS '전용 면적 (㎡)';
            COMMENT ON COLUMN apt_trade.deal_diff_rate IS '직전 거래 대비 변동률 (%)';
        """))

        conn.execute(text("""
            DROP TABLE IF EXISTS apt_rent CASCADE;
            CREATE TABLE apt_rent (
                id SERIAL PRIMARY KEY,
                apt_id VARCHAR(50),
                apartment_name VARCHAR(100),
                deal_date DATE,
                deposit FLOAT,
                monthly_rent FLOAT,
                rental_adjusted_deposit FLOAT,
                exclusive_area FLOAT,
                floor INTEGER,
                contract_term VARCHAR(50),
                contract_type VARCHAR(50),
                previous_rental_adjusted_deposit FLOAT,
                deposit_diff FLOAT,
                deposit_diff_rate FLOAT
            );
            COMMENT ON TABLE apt_rent IS '아파트 전월세 실거래가 (Rent Transaction History)';
            COMMENT ON COLUMN apt_rent.apt_id IS '아파트 고유 코드';
            COMMENT ON COLUMN apt_rent.deal_date IS '거래 일자';
            COMMENT ON COLUMN apt_rent.deposit IS '보증금 (만원)';
            COMMENT ON COLUMN apt_rent.monthly_rent IS '월세 (만원)';
            COMMENT ON COLUMN apt_rent.rental_adjusted_deposit IS '환산 보증금 (만원, 월세→전세 환산)';
        """))

    print("스키마 생성 완료.")


def load_data(engine):
    """최신 master CSV 파일을 DB에 적재합니다."""
    print(">>> 데이터 적재 (CSV → DB)")
    results = {}

    # Basic
    file_basic = get_latest_file("apt_basic_info_master_*.csv")
    if file_basic:
        print(f"Loading Basic: {file_basic}")
        with engine.begin() as conn:
            df = pd.read_csv(file_basic)
            cols = ['apt_id', 'apt_name', 'build_year', 'road_address', 'jibun_address',
                    'latitude', 'longitude', 'admin_dong']
            avail = [c for c in cols if c in df.columns]
            df[avail].to_sql('apt_basic', conn, if_exists='append', index=False, chunksize=1000)
        results['apt_basic'] = len(df)

    # Detail
    file_detail = get_latest_file("apt_detail_info_master_*.csv")
    if file_detail:
        print(f"Loading Detail: {file_detail}")
        with engine.begin() as conn:
            df = pd.read_csv(file_detail)
            # 날짜 형식 변환 (YYYYMMDD -> Date)
            if 'approval_date' in df.columns:
                df['approval_date'] = pd.to_datetime(df['approval_date'], format='%Y%m%d', errors='coerce')
            
            cols = ['complex_id', 'apt_id', 'complex_name', 'household_count', 'approval_date',
                    'total_parking_count', 'sale_type', 'apartment_type',
                    'convenient_facilities', 'education_facilities',
                    'bus_station_walking_time', 'subway_station_walking_time',
                    'subway_line', 'subway_station']
            avail = [c for c in cols if c in df.columns]
            df[avail].to_sql('apt_detail', conn, if_exists='append', index=False, chunksize=1000)
        results['apt_detail'] = len(df)

    # Trade
    file_trade = get_latest_file("apt_trade_master_*.csv")
    if file_trade:
        print(f"Loading Trade: {file_trade}")
        count = 0
        with engine.begin() as conn:
            cols = ['apt_id', 'apartment_name', 'deal_date', 'deal_amount', 'exclusive_area',
                    'floor', 'buyer_type', 'seller_type', 'dealing_type',
                    'previous_deal_amount', 'deal_diff', 'deal_diff_rate']
            for chunk in pd.read_csv(file_trade, chunksize=50000, low_memory=False):
                chunk['deal_date'] = pd.to_datetime(chunk.get('deal_date'), errors='coerce')
                avail = [c for c in cols if c in chunk.columns]
                chunk[avail].to_sql('apt_trade', conn, if_exists='append', index=False)
                count += len(chunk)
                print(".", end="", flush=True)
            print(f" ({count}건)")
        results['apt_trade'] = count

    # Rent
    file_rent = get_latest_file("apt_rent_master_*.csv")
    if file_rent:
        print(f"Loading Rent: {file_rent}")
        count = 0
        with engine.begin() as conn:
            cols = ['apt_id', 'apartment_name', 'deal_date', 'deposit', 'monthly_rent',
                    'rental_adjusted_deposit', 'exclusive_area', 'floor',
                    'contract_term', 'contract_type',
                    'deposit_diff', 'deposit_diff_rate']
            for chunk in pd.read_csv(file_rent, chunksize=50000, low_memory=False):
                chunk['deal_date'] = pd.to_datetime(chunk.get('deal_date'), errors='coerce')
                avail = [c for c in cols if c in chunk.columns]
                chunk[avail].to_sql('apt_rent', conn, if_exists='append', index=False)
                count += len(chunk)
                print(".", end="", flush=True)
            print(f" ({count}건)")
        results['apt_rent'] = count

    print("데이터 적재 완료.")
    return results


def create_indices(engine):
    """성능 최적화용 인덱스를 생성합니다."""
    print(">>> 인덱스 생성")
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_apt_basic_address ON apt_basic (road_address);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_apt_basic_dong ON apt_basic (admin_dong);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_trade_apt_date ON apt_trade (apt_id, deal_date);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_trade_area_amount ON apt_trade (exclusive_area, deal_amount);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_trade_date ON apt_trade (deal_date);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rent_apt_date ON apt_rent (apt_id, deal_date);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rent_deposit ON apt_rent (rental_adjusted_deposit);"))
    print("인덱스 생성 완료.")


def run_migration():
    """전체 DB 마이그레이션 실행"""
    print("\n=== PostgreSQL 마이그레이션 시작 ===")
    try:
        engine = get_db_engine()
        create_schema(engine)
        db_results = load_data(engine)
        create_indices(engine)
        print("=== 마이그레이션 성공 ===")
        return db_results
    except Exception as e:
        print(f"[ERROR] 마이그레이션 실패: {e}")
        return {"error": str(e)}


# ==============================================================================
# 6. 파일 정리
# ==============================================================================

def cleanup_old_files():
    """이전 버전 master, 원본 raw, 중복 파일을 삭제합니다."""
    print("\n>>> 파일 정리")
    deleted_files = []
    today_str = get_today_str()

    # 이전 버전 master 파일 삭제 (오늘 날짜 제외)
    patterns = [
        "apt_basic_info_master_*.csv",
        "apt_detail_info_master_*.csv",
        "apt_trade_master_*.csv",
        "apt_rent_master_*.csv",
        "apt_code_*.csv"
    ]
    for pattern in patterns:
        files = glob.glob(os.path.join(DATA_DIR, pattern))
        for f in files:
            if today_str not in f:
                print(f"삭제: {f}")
                os.remove(f)
                deleted_files.append(f)

    # 중복 파일 삭제
    duplicates = [
        "apt_basic.csv", "apt_details.csv",
        "apt_trade_processed.csv", "apt_rent_processed.csv",
        "test_apt_basic.csv"
    ]
    for name in duplicates:
        path = os.path.join(DATA_DIR, name)
        if os.path.exists(path):
            print(f"삭제: {path}")
            os.remove(path)
            deleted_files.append(path)

    # 원본 raw 파일 삭제
    raw_patterns = [
        "apt_basic_info_2*.csv",  # apt_basic_info_20260128.csv 등
        "apt_detail_info_2*.csv",
        "apt_trade_2*.csv",
        "apt_rent_2*.csv"
    ]
    for pattern in raw_patterns:
        files = glob.glob(os.path.join(DATA_DIR, pattern))
        for f in files:
            # master가 아닌 raw 파일만 삭제
            if "master" not in f:
                print(f"삭제: {f}")
                os.remove(f)
                deleted_files.append(f)

    print(f"총 {len(deleted_files)}개 파일 삭제 완료.")
    return deleted_files


# ==============================================================================
# 7. 보고서 생성
# ==============================================================================

def generate_report(update_results, db_results, deleted_files):
    """업데이트 결과 보고서를 생성합니다."""
    print("\n" + "=" * 60)
    print("          업데이트 및 마이그레이션 보고서")
    print("=" * 60)

    today_str = get_today_str()
    print(f"\n실행 일자: {today_str}")

    # 업데이트 결과
    print("\n--- 데이터 업데이트 ---")
    print(f"  기본 정보: {update_results.get('basic_count', 'N/A')}건")
    print(f"  상세 정보: {update_results.get('detail_count', 'N/A')}건")
    print(f"  매매 데이터: {update_results.get('trade_count', 'N/A')}건")
    print(f"  전월세 데이터: {update_results.get('rent_count', 'N/A')}건")
    new_basic = update_results.get('new_basic_from_trades', 0)
    if new_basic > 0:
        print(f"  거래 데이터 기반 신규 기본 정보 추가: {new_basic}건")

    # DB 결과
    if db_results:
        print("\n--- DB 마이그레이션 ---")
        if 'error' in db_results:
            print(f"  오류: {db_results['error']}")
        else:
            for table, count in db_results.items():
                print(f"  {table}: {count}건 적재")

    # 삭제 파일
    if deleted_files:
        print(f"\n--- 파일 정리 ---")
        print(f"  삭제된 파일: {len(deleted_files)}개")
        for f in deleted_files[:10]:
            print(f"    - {os.path.basename(f)}")
        if len(deleted_files) > 10:
            print(f"    ... 외 {len(deleted_files) - 10}개")

    # 현재 master 파일
    print("\n--- 현재 보존 파일 ---")
    for pattern in ["apt_code_*.csv", "apt_*_master_*.csv"]:
        files = glob.glob(os.path.join(DATA_DIR, pattern))
        for f in files:
            size_mb = os.path.getsize(f) / (1024 * 1024)
            print(f"  {os.path.basename(f)} ({size_mb:.1f} MB)")

    print("\n" + "=" * 60)


# ==============================================================================
# 메인 실행
# ==============================================================================

def main(skip_db=False, skip_cleanup=False, skip_update=False):
    """전체 파이프라인: 일일 업데이트 → DB 마이그레이션 → 정리 → 보고서"""
    print("=" * 60)
    print("  일일 데이터 업데이트 및 DB 마이그레이션")
    print("=" * 60)

    update_results = {}

    if not skip_update:
        # Step 1: 코드 업데이트
        new_codes = step_1_update_codes()

        # Step 2: 정보 업데이트
        b_count, d_count = step_2_update_info(new_codes)
        update_results['basic_count'] = b_count
        update_results['detail_count'] = d_count

        # Step 3: 매매/전월세 업데이트
        t_count, r_count, raw_trades, raw_rents = step_3_update_trade_rent()
        update_results['trade_count'] = t_count
        update_results['rent_count'] = r_count

        # Step 3b: 거래 데이터에서 신규 apt_id 감지 → 기본 정보 마스터에 추가
        new_basic_count = step_3b_update_basic_from_trades(raw_trades, raw_rents)
        update_results['new_basic_from_trades'] = new_basic_count
        update_results['basic_count'] = update_results.get('basic_count', 0) + new_basic_count
    else:
        print(">>> 신규 데이터 수집 및 업데이트 건너뛰기")

    # Step 4: DB 마이그레이션
    db_results = {}
    if not skip_db:
        db_results = run_migration()
    else:
        print("\n(DB 마이그레이션 건너뛰기)")

    # Step 5: 파일 정리
    deleted_files = []
    if not skip_cleanup:
        deleted_files = cleanup_old_files()
    else:
        print("\n(파일 정리 건너뛰기)")

    # 보고서
    generate_report(update_results, db_results, deleted_files)

    print("\n완료!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="데이터 업데이트 및 DB 마이그레이션")
    parser.add_argument('--skip-update', action='store_true', help='데이터 업데이트(수집/병합) 건너뛰기')
    parser.add_argument('--skip-db', action='store_true', help='DB 마이그레이션 건너뛰기')
    parser.add_argument('--skip-cleanup', action='store_true', help='파일 정리 건너뛰기')
    args = parser.parse_args()
    main(skip_db=args.skip_db, skip_cleanup=args.skip_cleanup, skip_update=args.skip_update)
