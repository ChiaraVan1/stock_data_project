import requests
import pandas as pd
from datetime import datetime
import re
import json
import os
import yfinance as yf
import time 
import pandas_datareader.data as web
from datetime import date, timedelta
from typing import List, Dict, Optional, Any


# --- 获取东方财富估值数据的原有函数---

def get_all_boards_mapping():
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_VALUEINDUSTRY_STA",
        "columns": "BOARD_CODE,BOARD_NAME",
        "source": "WEB",
        "client": "WEB",
        "pageNumber": 1,
        "pageSize": 1000
    }
    r = requests.get(url, params=params)
    data = r.json()
    boards = data.get("result", {}).get("data", [])
    board_map = {b["BOARD_NAME"]: b["BOARD_CODE"] for b in boards}
    return board_map

def get_stock_basic_info(stock_code):
    secid = f"0.{stock_code}" if stock_code.startswith(("0", "3")) else f"1.{stock_code}"
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "secid": secid,
        "fields": "f57,f58,f60,f117,f161,f162,f71,f72"
    }
    r = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
    js = r.json()
    if js.get("data"):
        d = js["data"]
        return {
            "type": "stock",
            "code": d.get("f57"),
            "name": d.get("f58"),
            "date": datetime.today().strftime("%Y-%m-%d"),
            "latest_price": d.get("f60") / 100 if d.get("f60") else None,
            "market_cap": d.get("f117"),
            "pb": d.get("f161") / 10000 if d.get("f161") else None,
            "pe": d.get("f162") / 100 if d.get("f162") else None,
        }
    return None

def get_stock_boards(stock_code):
    secid = f"1.{stock_code}" if stock_code.startswith("6") else f"0.{stock_code}"
    url = "https://push2.eastmoney.com/api/qt/slist/get"
    params = {
        "fltt": 1,
        "invt": 2,
        "cb": "callback",
        "fields": "f14,f12,f13,f3,f152,f4,f128,f140,f141",
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "pi": 0,
        "po": 1,
        "np": 1,
        "pz": 10,
        "spt": 3,
        "wbp2u": "6222375839819990|0|1|0|web",
        "_": 0
    }
    r = requests.get(url, params=params)
    text = r.text
    m = re.search(r'\((\{.*\})\)', text)
    try:
        data = json.loads(m.group(1))
    except Exception:
        return None
    if not data or "data" not in data or data["data"] is None:
        return None
    boards = data["data"].get("diff", [])
    if boards:
        return boards[0].get("f14")
    return None

def get_board_valuation(board_code):
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_VALUEINDUSTRY_STA",
        "columns": "ALL",
        "quoteColumns": "",
        "source": "WEB",
        "client": "WEB",
        "pageNumber": 1,
        "filter": f'(BOARD_CODE="{board_code}")',
    }
    r = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
    js = r.json()
    if js.get("result") and js["result"].get("data"):
        d = js["result"]["data"][0]
        return {
            "board_code": d.get("BOARD_CODE"),
            "board_name": d.get("BOARD_NAME"),
            "board_latest_price": (
                float(d["TOTAL_MARKET_CAP"]) / float(d["TOTAL_SHARES"])
                if d.get("TOTAL_MARKET_CAP") and d.get("TOTAL_SHARES") and float(d["TOTAL_SHARES"]) != 0
                else None
            ),
            "board_market_cap": float(d.get("TOTAL_MARKET_CAP")) if d.get("TOTAL_MARKET_CAP") else None,
            "board_pb": float(d.get("PB_MRQ")) if d.get("PB_MRQ") else None,
            "board_pe": float(d.get("PE_TTM")) if d.get("PE_TTM") else None
        }
    return None

# 全局加载一次映射表，避免重复请求
BOARD_MAP = get_all_boards_mapping()

def get_stock_board_valuation(stock_code):
    board_name = get_stock_boards(stock_code)
    if not board_name:
        print(f"⚠️ 无法获取股票 {stock_code} 的板块名称")
        return None

    board_code = BOARD_MAP.get(board_name)
    if not board_code:
        # 模糊匹配尝试
        for name, code in BOARD_MAP.items():
            if board_name in name:
                board_code = code
                print(f"✅ 模糊匹配: '{board_name}' → '{name}'")
                break

    if not board_code:
        print(f"⚠️ 找不到板块 '{board_name}' 对应的板块代码")
        return None

    return get_board_valuation(board_code)

def analyze_relative_valuation(stock_codes):
    results = []
    for code in stock_codes:
        stock = get_stock_basic_info(code)
        if not stock:
            continue

        board_info = get_stock_board_valuation(code)
        if not board_info:
            board_info = {
                "board_code": None, "board_name": None,
                "board_latest_price": None, "board_market_cap": None,
                "board_pb": None, "board_pe": None,
            }

        pb_ratio = stock["pb"] / board_info["board_pb"] if stock["pb"] and board_info["board_pb"] else None
        pe_ratio = stock["pe"] / board_info["board_pe"] if stock["pe"] and board_info["board_pe"] else None

        results.append({
            "code": stock.get("code"),
            "name": stock.get("name"),
            "pb": stock.get("pb"),
            "board_name": board_info.get("board_name"),
            "board_pb": board_info.get("board_pb"),
            "pb_ratio": pb_ratio,
            "pe": stock.get("pe"),
            "board_pe": board_info.get("board_pe"),
            "pe_ratio": pe_ratio,
            "latest_price": stock.get("latest_price"),
            "market_cap": stock.get("market_cap"),
            "date": stock.get("date"),
            "board_latest_price": board_info.get("board_latest_price"),
            "board_market_cap": board_info.get("board_market_cap"),
        })
    return pd.DataFrame(results)

# --- 新增的获取 Yahoo Finance 数据的函数---

def get_yahoo_finance_data(stock_codes):
    """
    从 Yahoo Finance 获取分析师目标价数据，并集成重试机制。
    """
    yahoo_data = []
    
    # 将A股代码转换为Yahoo Finance格式
    suffix_map = {
        '00': '.SZ',
        '30': '.SZ',
        '60': '.SS',
        '68': '.SS'
    }

    for code in stock_codes:
        ticker_symbol = f"{code}{suffix_map.get(str(code)[:2], '')}"
        max_retries = 3
        retries = 0
        
        print(f"正在从Yahoo Finance获取 {code} 的数据...")
        
        while retries < max_retries:
            try:
                stock = yf.Ticker(ticker_symbol)
                info = stock.info
                
                target_mean = info.get('targetMeanPrice')
                target_high = info.get('targetHighPrice')
                target_low = info.get('targetLowPrice')
                current_price = info.get('currentPrice')

                if target_mean and target_high and target_low and current_price:
                    upside_potential = ((target_mean - current_price) / current_price) * 100
                    yahoo_data.append({
                        '股票代码': code,
                        '目标均价': target_mean,
                        '目标最高价': target_high,
                        '目标最低价': target_low,
                        '当前价格': current_price,
                        '预测上涨/下跌空间(%)': upside_potential
                    })
                    print(f"✅ {code} 数据获取成功。")
                    break  # 成功获取数据，跳出循环
                else:
                    print(f"⚠️ {code} 数据不完整，正在进行第 {retries + 1} 次重试...")
                    retries += 1
                    time.sleep(5) # 等待5秒
            except Exception as e:
                print(f"❌ 获取 {code} 数据时出错: {e}，正在进行第 {retries + 1} 次重试...")
                retries += 1
                time.sleep(5) # 等待5秒
        else:
            # 当所有重试都失败后执行
            print(f"❌ {code} 无法获取数据，已达到最大重试次数。")
            yahoo_data.append({
                '股票代码': code,
                '目标均价': None,
                '目标最高价': None,
                '目标最低价': None,
                '当前价格': None,
                '预测上涨/下跌空间(%)': None
            })
            
    return pd.DataFrame(yahoo_data)

def get_vix() -> Optional[float]:
    """
    获取VIX指数。
    """
    start_date = date.today() - timedelta(days=7)
    try:
        vix_data = web.DataReader('VIXCLS', 'fred', start=start_date)
        if not vix_data.empty:
            return float(vix_data['VIXCLS'].iloc[-1])
        return None
    except Exception as e:
        print(f"获取VIX数据失败: {e}")
        return None

def get_fear_greed_index() -> Optional[float]:
    """
    获取CNN恐慌与贪婪指数。
    """
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        return float(data['fear_and_greed']['score'])
    except Exception as e:
        print(f"获取恐慌与贪婪指数失败: {e}")
        return None

def get_market_sentiment() -> Dict[str, Any]:
    """
    汇总大盘情绪指标。
    """
    return {
        "Date": date.today().strftime("%Y-%m-%d"),
        "VIX": get_vix(),
        "CNN Fear & Greed": get_fear_greed_index()
    }


def get_margin_balance(stock_codes: List[str]) -> pd.DataFrame:
    """东方财富接口 - 融资融券余额，返回DataFrame。"""
    results = []
    for code in stock_codes:
        try:
            url = f"https://push2.eastmoney.com/api/qt/stock/get?secid=0.{code}&fields=f57,f58,f161,f162"
            r = requests.get(url, timeout=10).json()
            if not r.get("data"):
                print(f"警告：股票代码 {code} 的数据不可用或不存在。")
                results.append({"Name": None, "Code": code, "Margin Buy": None, "Margin Sell": None})
                continue
            data = r["data"]
            results.append({
                "Name": data.get("f58"),
                "Code": data.get("f57"),
                "Margin Buy": data.get("f161"),
                "Margin Sell": data.get("f162")
            })
        except Exception as e:
            print(f"获取股票 {code} 的数据失败：{e}")
            results.append({"Name": None, "Code": code, "Margin Buy": None, "Margin Sell": None})
    return pd.DataFrame(results)



if __name__ == "__main__":
    # 股票代码列表
    stock_codes = ["002690", "002223", "516620", "300119", "600887"]

    # 1. 获取东方财富的估值数据
    print("正在获取东方财富的估值数据...")
    df_valuation = analyze_relative_valuation(stock_codes)
    print("东方财富数据获取完成。")

    # 2. 获取Yahoo Finance的分析师估值数据
    print("正在获取Yahoo Finance的分析师估值数据...")
    df_yahoo = get_yahoo_finance_data(stock_codes)
    print("Yahoo Finance数据获取完成。")

    # 3. 合并数据：使用 left_on 和 right_on 参数指定不同的列名进行合并
    df_merged = pd.merge(df_valuation, df_yahoo, left_on='code', right_on='股票代码', how='left')
    print("数据合并完成。")

    # 4. 获取你的融资融券数据
    print("正在获取融资融券数据...")
    df_margin = get_margin_balance(stock_codes)
    print("融资融券数据获取完成。")

    # 5. 大盘情绪
    market_data = get_market_sentiment()  
    print("大盘情绪数据获取完成。")

    # 6. 将融资融券数据合并到主数据框
    df_merged = pd.merge(df_merged, df_margin, left_on='code', right_on='Code', how='left')
    print("融资融券数据合并完成。")

    # 7. 添加大盘情绪数据到每一行
    for key, value in market_data.items():
        df_merged[key] = value
    print("大盘情绪数据添加完成。")  

    # 4. 重命名列并处理输出
    df_merged = df_merged.rename(columns={
        "code": "股票代码",
        "name": "股票名称",
        "pb": "市净率",
        "board_name": "行业名称",
        "board_pb": "行业市净率",
        "pb_ratio": "个股行业市净率比",
        "pe": "市盈率",
        "board_pe": "行业市盈率",
        "pe_ratio": "个股行业市盈率比",
        "latest_price": "最新价格",
        "market_cap": "市值",
        "date": "日期",
        "board_latest_price": "行业最新价格",
        "board_market_cap": "行业市值",
        "目标均价": "yahoo目标均价",
        "目标最高价": "yahoo目标最高价",
        "目标最低价": "yahoo目标最低价",
        "预测上涨/下跌空间(%)": "yahoo预测上涨/下跌空间(%)", 
        "Margin Buy": "融资买入额",
        "Margin Sell": "融券卖出额",
        "VIX": "VIX指数",
        "CNN Fear & Greed": "CNN恐慌与贪婪指数"
    })
    
    # 删除多余的 '股票代码' 列
    df_merged = df_merged.drop(columns=['股票代码', 'Name', 'Code', 'Date'])

    # 打印最终合并后的数据框
    print("\n最终合并后的数据（前5行）：")
    print(df_merged.head())
    
    # 5. 保存到新的CSV文件
    output_file_name = r'D:\python\monitoringmodeling\估值数据.csv'
    df_merged.to_csv(output_file_name, index=False, encoding='utf-8-sig')
    print(f"\n数据已成功保存至 '{output_file_name}'。")