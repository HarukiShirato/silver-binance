#!/usr/bin/env python3
"""
白银价差率监控脚本
监控上期所白银主力合约(AG)与Binance XAGUSDT永续合约的价差率

数据源:
- 上期所AG主力合约: 通过新浪财经或东方财富API获取
- Binance XAGUSDT: 通过Binance API获取
"""

import requests
import time
import json
from datetime import datetime
from typing import Optional, Tuple, Dict
import sys

# ============== 配置 ==============
# 监控间隔(秒)
MONITOR_INTERVAL = 30

# 价差率告警阈值(%)
ALERT_THRESHOLD_HIGH = 5.0   # 国内溢价超过5%告警
ALERT_THRESHOLD_LOW = -5.0   # 国内折价超过5%告警

# 汇率(可手动设置或自动获取)
USD_CNY_RATE = 7.25  # 美元兑人民币汇率

# 单位换算
# 上期所AG: 元/千克
# Binance XAG: 美元/盎司
# 1盎司 = 31.1035克 = 0.0311035千克
OUNCE_TO_KG = 0.0311035

# ============== API接口 ==============

def get_shfe_silver_price() -> Optional[Dict]:
    """
    获取上期所白银主力合约价格
    使用新浪财经期货接口
    返回: {'price': 价格(元/千克), 'name': 合约名称, 'time': 时间}
    """
    try:
        # 新浪期货行情接口 - 白银主力合约
        # ag0: 白银主力连续
        url = "https://hq.sinajs.cn/list=nf_AG0"
        headers = {
            'Referer': 'https://finance.sina.com.cn',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }

        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'gbk'

        # 解析返回数据
        # 格式: var hq_str_nf_AG0="白银连续,开盘,最高,最低,昨收,买价,卖价,最新价,...";
        text = response.text
        if 'hq_str_nf_AG0=' in text:
            data_str = text.split('="')[1].rstrip('";')
            fields = data_str.split(',')

            if len(fields) >= 8:
                name = fields[0]  # 合约名称
                # 新浪期货数据格式:
                # 0:名称, 1:开盘, 2:最高, 3:最低, 4:昨收, 5:买价, 6:卖价, 7:最新价
                # 最新价在第7个字段
                price = float(fields[7]) if fields[7] else float(fields[6])

                return {
                    'price': price,
                    'name': name,
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'unit': '元/千克'
                }
    except Exception as e:
        print(f"[错误] 获取上期所白银价格失败: {e}")

    return None


def get_shfe_silver_price_eastmoney() -> Optional[Dict]:
    """
    备用方案: 通过东方财富获取上期所白银主力合约价格
    """
    try:
        # 东方财富期货行情接口
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            'secid': '114.ag2406',  # 白银主力合约代码(需根据当前主力合约调整)
            'fields': 'f43,f44,f45,f46,f47,f48,f57,f58,f60',
            'ut': 'fa5fd1943c7b386f172d6893dbfba10b'
        }

        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        if data.get('data'):
            price = data['data'].get('f43', 0) / 100  # 东方财富价格需要除以100
            return {
                'price': price,
                'name': '白银主力',
                'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'unit': '元/千克'
            }
    except Exception as e:
        print(f"[错误] 东方财富接口获取失败: {e}")

    return None


def get_binance_xag_price() -> Optional[Dict]:
    """
    获取Binance XAGUSDT永续合约价格
    返回: {'price': 价格(美元/盎司), 'time': 时间}
    """
    try:
        # Binance期货API
        url = "https://fapi.binance.com/fapi/v1/ticker/price"
        params = {'symbol': 'XAGUSDT'}

        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        if 'price' in data:
            return {
                'price': float(data['price']),
                'symbol': 'XAGUSDT',
                'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'unit': 'USD/盎司'
            }
    except Exception as e:
        print(f"[错误] 获取Binance XAG价格失败: {e}")

    return None


def get_binance_xag_price_spot() -> Optional[Dict]:
    """
    备用方案: 获取Binance现货XAG价格(如果永续合约不可用)
    """
    try:
        url = "https://api.binance.com/api/v3/ticker/price"
        params = {'symbol': 'XAGUSDT'}

        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        if 'price' in data:
            return {
                'price': float(data['price']),
                'symbol': 'XAGUSDT',
                'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'unit': 'USD/盎司'
            }
    except Exception as e:
        print(f"[错误] Binance现货接口获取失败: {e}")

    return None


def get_usd_cny_rate() -> float:
    """
    获取实时美元兑人民币汇率
    """
    try:
        # 使用汇率API
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        response = requests.get(url, timeout=10)
        data = response.json()
        return data['rates'].get('CNY', USD_CNY_RATE)
    except:
        return USD_CNY_RATE


def convert_binance_to_cny_per_kg(binance_price_usd: float, usd_cny_rate: float) -> float:
    """
    将Binance价格(美元/盎司)转换为(人民币/千克)

    计算过程:
    1. 美元/盎司 * 汇率 = 人民币/盎司
    2. 人民币/盎司 / 0.0311035千克/盎司 = 人民币/千克
    """
    cny_per_ounce = binance_price_usd * usd_cny_rate
    cny_per_kg = cny_per_ounce / OUNCE_TO_KG
    return cny_per_kg


def calculate_spread(shfe_price: float, binance_price_cny: float) -> Tuple[float, float]:
    """
    计算价差和价差率

    返回: (价差, 价差率%)
    价差 = 国内价格 - 国际价格(转换后)
    价差率 = (国内价格 - 国际价格) / 国际价格 * 100%

    正值表示国内溢价，负值表示国内折价
    """
    spread = shfe_price - binance_price_cny
    spread_rate = (spread / binance_price_cny) * 100
    return spread, spread_rate


def check_alert(spread_rate: float) -> Optional[str]:
    """
    检查是否需要告警
    """
    if spread_rate >= ALERT_THRESHOLD_HIGH:
        return f"⚠️ 告警: 国内白银溢价过高! 价差率: {spread_rate:.2f}%"
    elif spread_rate <= ALERT_THRESHOLD_LOW:
        return f"⚠️ 告警: 国内白银折价过大! 价差率: {spread_rate:.2f}%"
    return None


def print_header():
    """打印表头"""
    print("\n" + "=" * 80)
    print(" 白银价差率监控系统 - 上期所AG vs Binance XAGUSDT")
    print("=" * 80)
    print(f"{'时间':<20} {'上期所(元/kg)':<15} {'Binance($/oz)':<15} {'Binance换算':<15} {'价差率':<10}")
    print("-" * 80)


def monitor_once() -> Optional[Dict]:
    """
    执行一次价差监控
    返回监控结果
    """
    # 获取上期所白银价格
    shfe_data = get_shfe_silver_price()
    if not shfe_data:
        shfe_data = get_shfe_silver_price_eastmoney()

    if not shfe_data:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 无法获取上期所白银价格")
        return None

    # 获取Binance XAG价格
    binance_data = get_binance_xag_price()
    if not binance_data:
        binance_data = get_binance_xag_price_spot()

    if not binance_data:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 无法获取Binance XAG价格")
        return None

    # 获取汇率
    usd_cny = get_usd_cny_rate()

    # 转换Binance价格为人民币/千克
    binance_cny_per_kg = convert_binance_to_cny_per_kg(binance_data['price'], usd_cny)

    # 计算价差
    spread, spread_rate = calculate_spread(shfe_data['price'], binance_cny_per_kg)

    # 格式化输出
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 根据价差率设置颜色标识
    if spread_rate > 0:
        rate_str = f"+{spread_rate:.2f}%"
        indicator = "📈"
    else:
        rate_str = f"{spread_rate:.2f}%"
        indicator = "📉"

    print(f"{current_time:<20} {shfe_data['price']:<15.2f} {binance_data['price']:<15.4f} {binance_cny_per_kg:<15.2f} {indicator}{rate_str:<10}")

    # 检查告警
    alert = check_alert(spread_rate)
    if alert:
        print(f"\n{alert}\n")

    return {
        'time': current_time,
        'shfe_price': shfe_data['price'],
        'binance_price_usd': binance_data['price'],
        'binance_price_cny': binance_cny_per_kg,
        'usd_cny_rate': usd_cny,
        'spread': spread,
        'spread_rate': spread_rate
    }


def monitor_continuous():
    """
    持续监控模式
    """
    print_header()
    print(f"\n监控间隔: {MONITOR_INTERVAL}秒 | 告警阈值: 溢价>{ALERT_THRESHOLD_HIGH}% 或 折价<{ALERT_THRESHOLD_LOW}%")
    print("按 Ctrl+C 停止监控\n")

    try:
        while True:
            monitor_once()
            time.sleep(MONITOR_INTERVAL)
    except KeyboardInterrupt:
        print("\n\n监控已停止")


def monitor_single():
    """
    单次查询模式
    """
    print_header()
    result = monitor_once()

    if result:
        print("\n" + "-" * 80)
        print("详细信息:")
        print(f"  上期所白银主力: {result['shfe_price']:.2f} 元/千克")
        print(f"  Binance XAGUSDT: {result['binance_price_usd']:.4f} 美元/盎司")
        print(f"  汇率 USD/CNY: {result['usd_cny_rate']:.4f}")
        print(f"  Binance换算价格: {result['binance_price_cny']:.2f} 元/千克")
        print(f"  价差: {result['spread']:.2f} 元/千克")
        print(f"  价差率: {result['spread_rate']:.2f}%")

        if result['spread_rate'] > 0:
            print(f"\n  📈 国内白银相对国际市场溢价 {result['spread_rate']:.2f}%")
        else:
            print(f"\n  📉 国内白银相对国际市场折价 {abs(result['spread_rate']):.2f}%")

    print("=" * 80)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '-c':
        # 持续监控模式
        monitor_continuous()
    else:
        # 单次查询模式
        monitor_single()
