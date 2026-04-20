"""
ETF Picker - Flask 웹 애플리케이션
"""

import json
import time
import threading
import logging
from flask import Flask, render_template, request, jsonify
from etf_fetcher import get_all_etf_data, filter_etfs, get_issuer_list

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 인메모리 캐시
_cache = {
    "etfs": [],
    "issuers": [],
    "last_updated": None,
    "loading": False,
    "enriching": False,  # 상세 정보 수집 중 여부
    "error": None,
}
_CACHE_TTL = 1800  # 30분


def _load_etf_data():
    """백그라운드에서 ETF 데이터 로딩 (2단계: 기본 → 상세)"""
    from etf_fetcher import get_naver_etf_list, get_naver_etf_detail, get_issuer_list, _process_basic
    import concurrent.futures

    _cache["loading"] = True
    _cache["enriching"] = False
    _cache["error"] = None

    try:
        # 1단계: 기본 데이터 (빠름)
        logger.info("1단계: 기본 ETF 데이터 수집...")
        raw_items = get_naver_etf_list()
        if not raw_items:
            _cache["error"] = "네이버 금융에서 데이터를 불러오지 못했습니다."
            return

        etfs = [_process_basic(item) for item in raw_items if item.get("itemcode")]
        etfs = [e for e in etfs if e]
        _cache["etfs"] = etfs
        _cache["issuers"] = get_issuer_list(etfs)
        _cache["loading"] = False
        logger.info(f"1단계 완료: {len(etfs)}개")

        # 2단계: 상세 정보 수집 (느림, 백그라운드)
        _cache["enriching"] = True
        logger.info("2단계: 상세 정보 병렬 스크래핑...")
        codes = [e["code"] for e in etfs]
        detail_map = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
            futures = {executor.submit(get_naver_etf_detail, code): code for code in codes}
            done = 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    d = future.result()
                    detail_map[d["code"]] = d
                except Exception:
                    pass
                done += 1
                if done % 100 == 0:
                    logger.info(f"  상세 스크래핑: {done}/{len(codes)}")

        # 병합
        for etf in etfs:
            d = detail_map.get(etf["code"], {})
            if d.get("index_name"):
                etf["index_name"] = d["index_name"]
            if d.get("issuer"):
                etf["issuer"] = d["issuer"]
            if d.get("listed_date"):
                etf["listed_date"] = d["listed_date"]
            if d.get("return_1m") is not None:
                etf["return_1m"] = d["return_1m"]
            # 3m은 bulk API 값 우선, 없으면 상세 페이지 값 사용
            if etf.get("return_3m") is None and d.get("return_3m_detail") is not None:
                etf["return_3m"] = d["return_3m_detail"]
            if d.get("return_6m") is not None:
                etf["return_6m"] = d["return_6m"]
            if d.get("return_1y") is not None:
                etf["return_1y"] = d["return_1y"]
            if d.get("expense_ratio") is not None:
                etf["expense_ratio"] = d["expense_ratio"]
            if d.get("etf_type_svc"):
                etf["etf_type_svc"] = d["etf_type_svc"]
                # WiseReport 유형에서 분배 주기 보완
                typ = d["etf_type_svc"]
                if etf.get("dist_freq") == "배당형" and "파생" in typ:
                    etf["dist_freq"] = "월배당"  # 파생상품형 배당ETF → 커버드콜 월배당
                if "배당" in typ and not etf.get("is_dividend"):
                    etf["is_dividend"] = True

        _cache["etfs"] = etfs
        _cache["issuers"] = get_issuer_list(etfs)
        _cache["last_updated"] = time.time()
        _cache["enriching"] = False
        logger.info("2단계 완료 - 전체 ETF 데이터 로딩 완료")

    except Exception as e:
        logger.error(f"ETF 데이터 로딩 실패: {e}")
        _cache["error"] = str(e)
    finally:
        _cache["loading"] = False
        _cache["enriching"] = False


def _ensure_data():
    """캐시 만료 시 재로딩"""
    now = time.time()
    if (
        not _cache["etfs"]
        and not _cache["loading"]
        and not _cache["error"]
    ):
        thread = threading.Thread(target=_load_etf_data, daemon=True)
        thread.start()
    elif (
        _cache["last_updated"]
        and now - _cache["last_updated"] > _CACHE_TTL
        and not _cache["loading"]
    ):
        thread = threading.Thread(target=_load_etf_data, daemon=True)
        thread.start()


@app.route("/")
def index():
    _ensure_data()
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    """데이터 로딩 상태 확인"""
    return jsonify({
        "loading": _cache["loading"],
        "enriching": _cache["enriching"],
        "ready": len(_cache["etfs"]) > 0,
        "count": len(_cache["etfs"]),
        "error": _cache["error"],
        "last_updated": _cache["last_updated"],
    })


@app.route("/api/issuers")
def api_issuers():
    """운용사 목록 반환"""
    return jsonify({"issuers": _cache["issuers"]})


@app.route("/api/search", methods=["POST"])
def api_search():
    """ETF 검색 API"""
    if _cache["loading"] and not _cache["etfs"]:
        return jsonify({"error": "데이터를 불러오는 중입니다. 잠시 후 다시 시도해주세요.", "loading": True}), 202

    if _cache["error"] and not _cache["etfs"]:
        return jsonify({"error": _cache["error"]}), 500

    if not _cache["etfs"]:
        _ensure_data()
        return jsonify({"error": "데이터를 불러오는 중입니다. 잠시 후 다시 시도해주세요.", "loading": True}), 202

    body = request.get_json(force=True) or {}

    filters = {
        "region": body.get("region", ""),
        "asset_type": body.get("asset_type", ""),
        "product_type": body.get("product_type", ""),
        "dividend_only": bool(body.get("dividend_only", False)),
        "dist_freq": body.get("dist_freq", ""),
        "issuer": body.get("issuer", ""),
        "index_keyword": body.get("index_keyword", ""),
        "name_keyword": body.get("name_keyword", ""),
        "return_period": body.get("return_period", "3m"),
        "sort_by": body.get("sort_by", "net_asset"),
        "sort_order": body.get("sort_order", "desc"),
        "limit": int(body.get("limit", 100)),
    }

    min_net_asset = body.get("min_net_asset")
    if min_net_asset not in (None, "", 0):
        filters["min_net_asset"] = float(min_net_asset)

    min_ret = body.get("min_return")
    if min_ret not in (None, ""):
        filters["min_return"] = float(min_ret)

    max_ret = body.get("max_return")
    if max_ret not in (None, ""):
        filters["max_return"] = float(max_ret)

    min_change = body.get("min_change_rate")
    if min_change not in (None, ""):
        filters["min_change_rate"] = float(min_change)

    max_change = body.get("max_change_rate")
    if max_change not in (None, ""):
        filters["max_change_rate"] = float(max_change)

    results = filter_etfs(_cache["etfs"], filters)

    # 통계 정보
    return_period = filters.get("return_period", "3m")
    stats = _compute_stats(results, return_period)

    return jsonify({
        "total": len(results),
        "etfs": results,
        "stats": stats,
    })


@app.route("/api/reload", methods=["POST"])
def api_reload():
    """데이터 강제 새로고침"""
    if not _cache["loading"]:
        _cache["etfs"] = []
        _cache["last_updated"] = None
        _cache["error"] = None
        thread = threading.Thread(target=_load_etf_data, daemon=True)
        thread.start()
        return jsonify({"message": "데이터 새로고침 시작"})
    return jsonify({"message": "이미 로딩 중입니다."})


def _compute_stats(etfs: list, return_period: str = "3m") -> dict:
    """검색 결과 통계 계산"""
    if not etfs:
        return {}

    period_field_map = {
        "1m": "return_1m", "3m": "return_3m",
        "6m": "return_6m", "1y": "return_1y",
    }
    ret_field = period_field_map.get(return_period, "return_3m")

    changes = [e["change_rate"] for e in etfs if e["change_rate"] != 0]
    returns = [e[ret_field] for e in etfs if e.get(ret_field) is not None]
    dividend_count = sum(1 for e in etfs if e.get("is_dividend", False))

    return {
        "avg_change_rate": round(sum(changes) / len(changes), 2) if changes else 0,
        "avg_return": round(sum(returns) / len(returns), 2) if returns else 0,
        "total_net_asset": round(sum(e["net_asset_billion"] for e in etfs), 0),
        "rising": sum(1 for e in etfs if e["change_rate"] > 0),
        "falling": sum(1 for e in etfs if e["change_rate"] < 0),
        "flat": sum(1 for e in etfs if e["change_rate"] == 0),
        "dividend_count": dividend_count,
        "return_period": return_period,
    }


if __name__ == "__main__":
    # 앱 시작 시 데이터 미리 로딩
    logger.info("ETF Picker 시작 - 데이터 로딩 중...")
    thread = threading.Thread(target=_load_etf_data, daemon=True)
    thread.start()
    app.run(debug=False, host="0.0.0.0", port=5000, use_reloader=False)
