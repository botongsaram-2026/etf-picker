"""
ETF 데이터 수집 모듈
- 네이버 금융 API (주 데이터 소스 - 전체 ETF 1회 호출)
- 네이버 금융 개별 페이지 병렬 스크래핑 (운용사, 기초지수 보완)
"""

import requests
import time
import logging
import concurrent.futures
from bs4 import BeautifulSoup
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NAVER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.naver.com/",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

# 배당/분배형 ETF 키워드 (is_dividend=True 분류 기준)
DIVIDEND_KEYWORDS = [
    "배당", "고배당", "배당성장", "인컴", "분배",
    "dividend", "DIVD", "SCHD", "income", "INCOME",
    "다우존스배당", "다우배당", "배당귀족", "배당킹",
    "커버드콜", "coveredcall", "COVERED", "프리미엄",
    "월배당", "분기배당", "연배당",
    "위클리", "타겟커버드콜", "컨벡스", "콜매도",
]

# 월배당 ETF 키워드 (월간 분배금 지급)
MONTHLY_DIV_KEYWORDS = [
    "월배당", "Monthly", "매월", "월분배", "월지급",
    "위클리",           # 위클리 커버드콜 → 월분배
]
# 분기배당 ETF 키워드
QUARTERLY_DIV_KEYWORDS = ["분기배당", "Quarterly", "매분기", "분기분배"]
# 커버드콜/파생 계열 - 보통 월배당
COVERED_CALL_KEYWORDS = [
    "커버드콜", "CoveredCall", "컨벡스", "타겟커버드콜",
    "콜매도", "프리미엄",
    # 해외 커버드콜 (QYLD 계열 등)
    "QYLD", "XYLD", "RYLD",
]

# ETF 브랜드 → 운용사 매핑
BRAND_TO_ISSUER = {
    "KODEX": "삼성자산운용",
    "TIGER": "미래에셋자산운용",
    "ACE": "한국투자신탁운용",
    "KINDEX": "한국투자신탁운용",
    "HANARO": "NH아문디자산운용",
    "SOL": "신한자산운용",
    "KOSEF": "키움투자자산운용",
    "ARIRANG": "한화자산운용",
    "KBSTAR": "KB자산운용",
    "PLUS": "한화자산운용",
    "WON": "우리자산운용",
    "TIMEFOLIO": "타임폴리오자산운용",
    "BNK": "BNK자산운용",
    "TREX": "트러스톤자산운용",
    "FOCUS": "키움투자자산운용",
    "GIANT": "한국투자신탁운용",
    "ONE": "원자산운용",
    "KTOP": "한국투자신탁운용",
    "MAAX": "마이다스에셋자산운용",
    "ITOCHU": "이토추자산운용",
    "iShares": "블랙록자산운용",
    "파워": "교보악사자산운용",
}

# etfTabCode → 분류 매핑 (네이버 금융 기준)
# 1: 국내주식(시장형), 2: 국내섹터/테마, 3: 레버리지/인버스
# 4: 해외주식, 5: 채권, 6: 원자재/부동산/통화/기타, 7: 혼합
TAB_CODE_REGION = {
    1: "국내", 2: "국내", 3: "",
    4: "해외", 5: "", 6: "", 7: "국내",
}
TAB_CODE_ASSET = {
    1: "주식", 2: "주식", 3: "주식",
    4: "주식", 5: "채권", 6: "원자재", 7: "혼합",
}


def get_naver_etf_list() -> list[dict]:
    """네이버 금융 ETF 전체 목록 API 호출 (1회 호출로 1000개 이상)"""
    url = "https://finance.naver.com/api/sise/etfItemList.nhn?etfType=0"
    try:
        resp = requests.get(url, headers=NAVER_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("result", {}).get("etfItemList", [])
        logger.info(f"네이버 ETF 목록 수신: {len(items)}개")
        return items
    except Exception as e:
        logger.error(f"네이버 ETF 목록 조회 실패: {e}")
        return []


def get_naver_etf_detail(code: str) -> dict:
    """네이버 금융 + WiseReport 스크래핑 (기초지수, 운용사, 수익률, 보수율, ETF 타입)"""
    import re, json as _json
    url = f"https://finance.naver.com/item/coinfo.naver?code={code}&target=etf"
    result = {
        "code": code,
        "index_name": "",
        "issuer": "",
        "listed_date": "",
        "return_1m": None,
        "return_3m_detail": None,
        "return_6m": None,
        "return_1y": None,
        "expense_ratio": None,   # 연간 총보수율(%)
        "etf_type_svc": "",      # WiseReport ETF 유형 (예: 국내주식형, 파생상품)
        "dist_freq_detail": "",  # WiseReport 기반 보완 분배 주기
    }
    try:
        resp = requests.get(url, headers=NAVER_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        tables = soup.find_all("table")

        # Table 2: 기초지수, 운용사, 상장일
        if len(tables) > 2:
            for row in tables[2].find_all("tr"):
                cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
                if len(cells) >= 2:
                    label, value = cells[0], cells[1]
                    if "기초지수" in label or "추종지수" in label:
                        result["index_name"] = value
                    elif "운용사" in label:
                        result["issuer"] = value.split(",")[0].replace("(주)", "").strip()
                    elif "상장일" in label:
                        result["listed_date"] = value

        # Table 3: 자산운용사 보완
        if not result["issuer"] and len(tables) > 3:
            for row in tables[3].find_all("tr"):
                cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
                if len(cells) >= 2 and "자산운용사" in cells[0]:
                    result["issuer"] = cells[1].replace("(주)", "").strip()

        # Table 5: 기간별 수익률
        if len(tables) > 5:
            return_labels = {
                "1개월": "return_1m",
                "3개월": "return_3m_detail",
                "6개월": "return_6m",
                "1년": "return_1y",
            }
            for row in tables[5].find_all("tr"):
                cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
                if len(cells) >= 2:
                    for label_text, field in return_labels.items():
                        if label_text in cells[0]:
                            try:
                                val = cells[1].replace("+", "").replace("%", "").strip()
                                result[field] = float(val)
                            except ValueError:
                                pass

        # WiseReport 스크래핑: 보수율 + ETF 유형
        _enrich_from_wisereport(code, result, re, _json)

    except Exception as e:
        logger.debug(f"ETF 상세 조회 실패 ({code}): {e}")
    return result


def _enrich_from_wisereport(code: str, result: dict, re_mod, json_mod) -> None:
    """WiseReport iframe 페이지에서 보수율·ETF유형·기간별수익률 보완"""
    wr_url = f"https://navercomp.wisereport.co.kr/v2/ETF/index.aspx?cmp_cd={code}&target=etf"
    wr_headers = {**NAVER_HEADERS, "Referer": "https://finance.naver.com/"}
    try:
        wr = requests.get(wr_url, headers=wr_headers, timeout=8)
        wr.raise_for_status()
        wr_soup = BeautifulSoup(wr.text, "lxml")
        for script in wr_soup.find_all("script"):
            content = script.string or ""
            # summary_data → 보수율 + ETF 유형
            if "summary_data" in content:
                m_pay = re_mod.search(r'"TOT_PAY"\s*:\s*"?([0-9.]+)"?', content)
                if m_pay:
                    try:
                        result["expense_ratio"] = float(m_pay.group(1))
                    except ValueError:
                        pass
                m_typ = re_mod.search(r'"ETF_TYP_SVC_NM"\s*:\s*"([^"]*)"', content)
                if m_typ:
                    result["etf_type_svc"] = m_typ.group(1)
            # status_data → 기간별 수익률 (ERN1·ERN3·ERN6·ERN12)
            if "status_data" in content:
                for ern_key, result_key in (
                    ("ERN1", "return_1m"),
                    ("ERN3", "return_3m_detail"),
                    ("ERN6", "return_6m"),
                    ("ERN12", "return_1y"),
                ):
                    m = re_mod.search(rf'"{ern_key}"\s*:\s*"?(-?[0-9.]+)"?', content)
                    if m:
                        try:
                            result[result_key] = float(m.group(1))
                        except ValueError:
                            pass
    except Exception:
        pass


def get_all_etf_data(enrich_details: bool = True, max_workers: int = 20) -> list[dict]:
    """
    전체 ETF 데이터 수집 및 통합
    enrich_details=True 시 개별 페이지 병렬 스크래핑으로 상세 정보 보완
    """
    logger.info("ETF 데이터 수집 시작 (네이버 금융)...")
    raw_items = get_naver_etf_list()
    if not raw_items:
        return []

    # 기본 데이터 처리
    etfs = [_process_basic(item) for item in raw_items if item.get("itemcode")]
    etfs = [e for e in etfs if e]
    logger.info(f"기본 ETF 데이터 처리 완료: {len(etfs)}개")

    if enrich_details:
        logger.info(f"상세 정보 병렬 스크래핑 시작 ({max_workers}개 워커)...")
        codes = [e["code"] for e in etfs]
        detail_map: dict[str, dict] = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(get_naver_etf_detail, code): code for code in codes}
            done_count = 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    detail = future.result()
                    detail_map[detail["code"]] = detail
                except Exception as e:
                    logger.debug(f"상세 정보 처리 오류: {e}")
                done_count += 1
                if done_count % 100 == 0:
                    logger.info(f"  상세 스크래핑 진행: {done_count}/{len(codes)}")

        # 상세 정보 병합
        for etf in etfs:
            detail = detail_map.get(etf["code"], {})
            if detail.get("index_name"):
                etf["index_name"] = detail["index_name"]
            if detail.get("issuer"):
                etf["issuer"] = detail["issuer"]
            if detail.get("listed_date"):
                etf["listed_date"] = detail["listed_date"]
            if detail.get("return_1m") is not None:
                etf["return_1m"] = detail["return_1m"]
            if detail.get("return_6m") is not None:
                etf["return_6m"] = detail["return_6m"]
            if detail.get("return_1y") is not None:
                etf["return_1y"] = detail["return_1y"]

        logger.info("상세 정보 병합 완료")

    logger.info(f"ETF 데이터 수집 완료: {len(etfs)}개")
    return etfs


def _process_basic(item: dict) -> Optional[dict]:
    """네이버 API 응답 → 내부 ETF 딕셔너리 변환"""
    try:
        code = str(item.get("itemcode", "")).zfill(6)
        name = item.get("itemname", "")
        if not name:
            return None

        tab_code = int(item.get("etfTabCode", 1))
        current_price = int(item.get("nowVal") or 0)
        change_rate = float(item.get("changeRate") or 0)
        nav = float(item.get("nav") or 0)
        return_3m = float(item.get("threeMonthEarnRate") or 0)
        volume = int(item.get("quant") or 0)
        trade_amount = int(item.get("amonut") or 0)  # 백만원
        market_sum = int(item.get("marketSum") or 0)  # 억원

        # 분류
        region = _classify_region(name, tab_code)
        asset_type = _classify_asset_type(name, tab_code)
        product_type = _classify_product_type(name)
        issuer = _extract_issuer(name)

        # 레버리지/인버스는 탭코드 3이므로 region/asset 재보정
        if tab_code == 3:
            asset_type = _classify_asset_type(name, tab_code)

        is_dividend = _is_dividend_etf(name)
        dist_freq = _classify_dist_freq(name, is_dividend)

        return {
            "code": code,
            "name": name,
            "current_price": current_price,
            "change_rate": change_rate,
            "nav": nav,
            "return_3m": return_3m,
            "return_1m": None,
            "return_6m": None,
            "return_1y": None,
            "volume": volume,
            "trade_amount_mil": trade_amount,
            "net_asset_billion": market_sum,
            "index_name": "",
            "issuer": issuer,
            "listed_date": "",
            "region": region,
            "asset_type": asset_type,
            "product_type": product_type,
            "is_dividend": is_dividend,
            "dist_freq": dist_freq,
            "tab_code": tab_code,
            "expense_ratio": None,   # 연간 총보수율 (2단계 스크래핑 후 채워짐)
            "etf_type_svc": "",
        }
    except Exception as e:
        logger.debug(f"기본 데이터 처리 오류: {e}")
        return None


def _classify_region(name: str, tab_code: int) -> str:
    """국내/해외 분류 - 탭코드 우선, 이름으로 보완"""
    # 탭코드 기반 1차 분류
    tab_region = TAB_CODE_REGION.get(tab_code, "")
    if tab_region:
        return tab_region

    # 탭코드가 명확하지 않을 때 이름 키워드로 분류 (탭 3, 5, 6 등)
    overseas_keywords = [
        "미국", "S&P", "나스닥", "NASDAQ", "다우", "DOW", "미국채",
        "중국", "일본", "유럽", "신흥국", "글로벌", "선진국", "인도",
        "베트남", "브라질", "홍콩", "유로", "달러", "해외", "월드",
        "MSCI", "FTSE", "Russell", "독일", "영국", "대만", "동남아",
        "EM", "UK", "EU", "아시아퍼시픽",
    ]
    upper = name.upper()
    for kw in overseas_keywords:
        if kw.upper() in upper:
            return "해외"
    return "국내"


def _classify_asset_type(name: str, tab_code: int) -> str:
    """자산 유형 분류 - 이름 키워드 우선, 탭코드 보완"""
    upper = name.upper()

    if any(k in upper for k in ["머니마켓", "MMF", "단기자금", "CD금리", "KOFR", "SOFR"]):
        return "머니마켓"
    if any(k in upper for k in ["채권", "국채", "회사채", "단기채", "BOND", "TREASURY", "금리", "크레딧"]):
        return "채권"
    if any(k in upper for k in ["리츠", "부동산", "REIT"]):
        return "부동산"
    if any(k in upper for k in ["금", "은", "원자재", "원유", "오일", "OIL", "GOLD", "COMMODITY", "농산물", "구리", "천연가스"]):
        return "원자재"
    if any(k in upper for k in ["혼합", "자산배분", "멀티에셋", "밸런스", "올웨더", "TDF"]):
        return "혼합"

    # 탭코드 기반 보완
    return TAB_CODE_ASSET.get(tab_code, "주식")


def _classify_product_type(name: str) -> str:
    """레버리지/인버스 여부"""
    upper = name.upper()
    if "2X" in upper or "레버리지" in upper or "LEVERAGE" in upper:
        return "레버리지"
    if "인버스" in upper or "INVERSE" in upper or "곱버스" in upper:
        return "인버스"
    return "일반"


def _extract_issuer(name: str) -> str:
    """ETF 이름에서 운용사 추출"""
    upper = name.upper()
    for brand, issuer in BRAND_TO_ISSUER.items():
        if upper.startswith(brand.upper()):
            return issuer
    return ""


def _is_dividend_etf(name: str) -> bool:
    """배당/분배형 ETF 여부 판별 (이름 기반)"""
    upper = name.upper()
    return any(kw.upper() in upper for kw in DIVIDEND_KEYWORDS)


def _classify_dist_freq(name: str, is_dividend: bool) -> str:
    """분배 주기 분류 (이름 기반)
    반환값: "월배당" | "분기배당" | "배당형" | "비배당"
    """
    if not is_dividend:
        return "비배당"
    upper = name.upper()
    # 월배당: 명시적 키워드 또는 커버드콜 계열
    if any(kw.upper() in upper for kw in MONTHLY_DIV_KEYWORDS):
        return "월배당"
    if any(kw.upper() in upper for kw in COVERED_CALL_KEYWORDS):
        return "월배당"
    # 분기배당
    if any(kw.upper() in upper for kw in QUARTERLY_DIV_KEYWORDS):
        return "분기배당"
    # 일반 배당형 (주기 미지정)
    return "배당형"


def filter_etfs(etfs: list[dict], filters: dict) -> list[dict]:
    """
    필터 조건에 따라 ETF 목록 필터링

    filters 예시:
    {
        "region": "해외",
        "asset_type": "주식",
        "product_type": "일반",
        "dividend_only": False,      # 배당/분배형 ETF만 보기
        "issuer": "삼성",
        "index_keyword": "S&P",
        "name_keyword": "나스닥",
        "min_net_asset": 100,
        "return_period": "3m",       # "1m"|"3m"|"6m"|"1y" - 수익률 기간
        "min_return": 5.0,
        "max_return": 50.0,
        "min_change_rate": -3.0,
        "max_change_rate": 5.0,
        "sort_by": "net_asset",
        "sort_order": "desc",
        "limit": 100,
    }
    """
    result = etfs.copy()

    region = filters.get("region", "")
    if region:
        result = [e for e in result if e["region"] == region]

    asset_type = filters.get("asset_type", "")
    if asset_type:
        result = [e for e in result if e["asset_type"] == asset_type]

    product_type = filters.get("product_type", "")
    if product_type:
        result = [e for e in result if e["product_type"] == product_type]

    dividend_only = filters.get("dividend_only", False)
    if dividend_only:
        result = [e for e in result if e.get("is_dividend", False)]

    dist_freq = filters.get("dist_freq", "")
    if dist_freq:
        if dist_freq == "배당형전체":
            result = [e for e in result if e.get("is_dividend", False)]
        else:
            result = [e for e in result if e.get("dist_freq", "") == dist_freq]

    issuer = filters.get("issuer", "")
    if issuer:
        result = [e for e in result if issuer in e.get("issuer", "")]

    index_kw = filters.get("index_keyword", "")
    if index_kw:
        result = [
            e for e in result
            if index_kw.lower() in e.get("index_name", "").lower()
            or index_kw.lower() in e["name"].lower()
        ]

    name_kw = filters.get("name_keyword", "")
    if name_kw:
        result = [e for e in result if name_kw.lower() in e["name"].lower()]

    min_asset = filters.get("min_net_asset")
    if min_asset is not None:
        result = [e for e in result if e["net_asset_billion"] >= float(min_asset)]

    # 수익률 기간별 필터 (1m/3m/6m/1y)
    period = filters.get("return_period", "3m")
    period_field_map = {
        "1m": "return_1m",
        "3m": "return_3m",
        "6m": "return_6m",
        "1y": "return_1y",
    }
    return_field = period_field_map.get(period, "return_3m")

    min_ret = filters.get("min_return")
    if min_ret is not None:
        result = [e for e in result if (e.get(return_field) or 0) >= float(min_ret)]

    max_ret = filters.get("max_return")
    if max_ret is not None:
        result = [e for e in result if (e.get(return_field) or 0) <= float(max_ret)]

    min_change = filters.get("min_change_rate")
    if min_change is not None:
        result = [e for e in result if e["change_rate"] >= float(min_change)]

    max_change = filters.get("max_change_rate")
    if max_change is not None:
        result = [e for e in result if e["change_rate"] <= float(max_change)]

    # 정렬
    sort_by = filters.get("sort_by", "net_asset")
    sort_order = filters.get("sort_order", "desc")
    sort_map = {
        "net_asset": "net_asset_billion",
        "change_rate": "change_rate",
        "return": return_field,
        "volume": "volume",
        "price": "current_price",
    }
    field = sort_map.get(sort_by, "net_asset_billion")
    result.sort(key=lambda x: x.get(field) or 0, reverse=(sort_order == "desc"))

    limit = int(filters.get("limit", 100))
    return result[:limit]


def get_issuer_list(etfs: list[dict]) -> list[str]:
    return sorted({e["issuer"] for e in etfs if e.get("issuer")})
