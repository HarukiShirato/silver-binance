#!/usr/bin/env python3
"""
黄金价差率回测分析脚本 (15分钟级别)
回测上期所黄金主力合约(AU)与Binance XAUUSDT的历史价差率
计算偏离值的方差、标准差、均值等统计指标
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict
import warnings
warnings.filterwarnings('ignore')

# ============== 配置 ==============
# 单位换算:
# 上期所黄金: 元/克
# Binance XAU: 美元/盎司
# 1盎司 = 31.1035克
OUNCE_TO_GRAM = 31.1035

# 默认K线周期
DEFAULT_INTERVAL = '15m'

# Binance最大获取条数
BINANCE_LIMIT = 1000

# ============== 数据获取函数 ==============

def get_binance_klines(symbol: str, interval: str = '15m', limit: int = 1000) -> Optional[pd.DataFrame]:
    """
    获取Binance K线历史数据
    """
    try:
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': min(limit, 1500)
        }

        response = requests.get(url, params=params, timeout=15)
        data = response.json()

        if isinstance(data, list) and len(data) > 0:
            df = pd.DataFrame(data, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                'taker_buy_quote', 'ignore'
            ])

            df['datetime'] = pd.to_datetime(df['open_time'], unit='ms')
            df['close'] = df['close'].astype(float)
            df['open'] = df['open'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)

            return df[['datetime', 'open', 'high', 'low', 'close']]

    except Exception as e:
        print(f"[错误] 获取Binance数据失败: {e}")

    return None


def get_shfe_gold_minute_akshare(interval: str = '15') -> Optional[pd.DataFrame]:
    """
    通过akshare获取上期所黄金期货分钟级数据
    interval: '5', '15', '30', '60'
    """
    try:
        import akshare as ak

        # 获取黄金主力连续合约分钟数据
        df = ak.futures_zh_minute_sina(symbol='AU0', period=interval)

        if df is not None and len(df) > 0:
            df = df.rename(columns={'datetime': 'datetime'})
            df['datetime'] = pd.to_datetime(df['datetime'])
            return df[['datetime', 'open', 'high', 'low', 'close']]

    except Exception as e:
        print(f"[错误] 获取akshare分钟数据失败: {e}")

    return None


def convert_binance_to_cny_per_gram(price_usd: float, usd_cny: float) -> float:
    """
    将Binance价格(美元/盎司)转换为(人民币/克)

    计算: 美元/盎司 * 汇率 / 31.1035克/盎司 = 人民币/克
    """
    return (price_usd * usd_cny) / OUNCE_TO_GRAM


# ============== 回测分析 ==============

def backtest_spread(interval: str = '15m') -> Optional[pd.DataFrame]:
    """
    执行价差率回测 (分钟级别)
    """
    # 解析周期
    if interval.endswith('m'):
        sina_period = interval[:-1]
    elif interval == '1h':
        sina_period = '60'
    else:
        sina_period = '15'

    print(f"\n正在获取 {interval} K线历史数据...\n")

    # 获取上期所黄金数据
    print(f"1. 获取上期所黄金主力合约 {interval} 数据 (akshare)...")
    shfe_data = get_shfe_gold_minute_akshare(sina_period)

    if shfe_data is None or len(shfe_data) == 0:
        print("   [错误] 无法获取上期所黄金历史数据")
        return None
    print(f"   成功获取 {len(shfe_data)} 条记录")
    print(f"   数据范围: {shfe_data['datetime'].min()} ~ {shfe_data['datetime'].max()}")

    # 获取Binance XAUUSDT数据
    print(f"\n2. 获取Binance XAUUSDT {interval} 数据...")
    binance_data = get_binance_klines('XAUUSDT', interval, BINANCE_LIMIT)

    if binance_data is None or len(binance_data) == 0:
        print("   [错误] 无法获取Binance XAUUSDT历史数据")
        return None
    print(f"   成功获取 {len(binance_data)} 条记录")
    print(f"   数据范围: {binance_data['datetime'].min()} ~ {binance_data['datetime'].max()}")

    # 获取汇率
    print("\n3. 获取汇率数据...")
    try:
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        response = requests.get(url, timeout=10)
        usd_cny_rate = response.json()['rates'].get('CNY', 7.25)
    except:
        usd_cny_rate = 7.25
    print(f"   使用汇率: {usd_cny_rate}")

    # 数据对齐
    print("\n4. 对齐数据...")
    shfe_data['datetime'] = pd.to_datetime(shfe_data['datetime']).dt.floor('min')
    binance_data['datetime'] = pd.to_datetime(binance_data['datetime']).dt.floor('min')

    # 合并数据
    merged = pd.merge(
        shfe_data[['datetime', 'close']].rename(columns={'close': 'shfe_close'}),
        binance_data[['datetime', 'close']].rename(columns={'close': 'binance_close'}),
        on='datetime',
        how='inner'
    )

    if len(merged) == 0:
        print("   [错误] 没有重叠的时间数据")
        return None

    merged = merged.sort_values('datetime').reset_index(drop=True)
    print(f"   共有 {len(merged)} 个重叠时间点")

    # 计算价差率
    print("\n5. 计算价差率...")
    merged['binance_cny'] = merged['binance_close'].apply(
        lambda x: convert_binance_to_cny_per_gram(x, usd_cny_rate)
    )
    merged['spread'] = merged['shfe_close'] - merged['binance_cny']
    merged['spread_rate'] = (merged['spread'] / merged['binance_cny']) * 100

    return merged


def analyze_spread(df: pd.DataFrame) -> Dict:
    """
    分析价差率统计数据
    """
    spread_rates = df['spread_rate'].values

    stats = {
        '样本数量': len(spread_rates),
        '均值(%)': np.mean(spread_rates),
        '中位数(%)': np.median(spread_rates),
        '方差': np.var(spread_rates),
        '标准差(%)': np.std(spread_rates),
        '最大值(%)': np.max(spread_rates),
        '最小值(%)': np.min(spread_rates),
        '极差(%)': np.max(spread_rates) - np.min(spread_rates),
        '偏度': pd.Series(spread_rates).skew(),
        '峰度': pd.Series(spread_rates).kurtosis(),
        '25%分位数(%)': np.percentile(spread_rates, 25),
        '75%分位数(%)': np.percentile(spread_rates, 75),
        'IQR(%)': np.percentile(spread_rates, 75) - np.percentile(spread_rates, 25)
    }

    return stats


def print_results(df: pd.DataFrame, stats: Dict, interval: str):
    """
    打印回测结果
    """
    print("\n" + "=" * 90)
    print(f" 黄金价差率回测分析报告 ({interval} K线)")
    print(" 上期所AU主力 vs Binance XAUUSDT")
    print("=" * 90)

    print("\n【统计摘要】")
    print("-" * 45)
    print(f"  样本数量:     {stats['样本数量']} 个数据点")
    print(f"  数据区间:     {df['datetime'].min()} ~ {df['datetime'].max()}")
    print("-" * 45)

    print("\n【偏离值(价差率)统计】")
    print("-" * 45)
    print(f"  均值:         {stats['均值(%)']:.4f} %")
    print(f"  中位数:       {stats['中位数(%)']:.4f} %")
    print(f"  方差:         {stats['方差']:.6f}")
    print(f"  标准差:       {stats['标准差(%)']:.4f} %")
    print("-" * 45)

    print("\n【极值分析】")
    print("-" * 45)
    print(f"  最大值:       {stats['最大值(%)']:.4f} %")
    print(f"  最小值:       {stats['最小值(%)']:.4f} %")
    print(f"  极差:         {stats['极差(%)']:.4f} %")
    print("-" * 45)

    print("\n【分布特征】")
    print("-" * 45)
    print(f"  偏度:         {stats['偏度']:.4f}  {'(右偏)' if stats['偏度'] > 0 else '(左偏)' if stats['偏度'] < 0 else '(对称)'}")
    print(f"  峰度:         {stats['峰度']:.4f}  {'(尖峰)' if stats['峰度'] > 0 else '(平坦)' if stats['峰度'] < 0 else '(正态)'}")
    print(f"  25%分位数:    {stats['25%分位数(%)']:.4f} %")
    print(f"  75%分位数:    {stats['75%分位数(%)']:.4f} %")
    print(f"  四分位距IQR:  {stats['IQR(%)']:.4f} %")
    print("-" * 45)

    mean = stats['均值(%)']
    std = stats['标准差(%)']
    print("\n【波动区间】")
    print("-" * 45)
    print(f"  1σ区间 (68%): [{mean - std:.4f}%, {mean + std:.4f}%]")
    print(f"  2σ区间 (95%): [{mean - 2*std:.4f}%, {mean + 2*std:.4f}%]")
    print(f"  3σ区间 (99%): [{mean - 3*std:.4f}%, {mean + 3*std:.4f}%]")
    print("-" * 45)

    print("\n【最近20条价差率明细】")
    print("-" * 95)
    print(f"{'时间':<20} {'上期所(元/克)':<15} {'Binance($/oz)':<15} {'Binance换算':<15} {'价差率(%)':<12}")
    print("-" * 95)

    for _, row in df.tail(20).iterrows():
        dt_str = row['datetime'].strftime('%Y-%m-%d %H:%M')
        indicator = "📈" if row['spread_rate'] > 0 else "📉"
        print(f"{dt_str:<20} {row['shfe_close']:<15.2f} {row['binance_close']:<15.2f} {row['binance_cny']:<15.2f} {indicator}{row['spread_rate']:<+11.4f}")

    print("=" * 95)

    print("\n【分析结论】")
    print("-" * 45)
    if stats['均值(%)'] > 0:
        print(f"  • 国内黄金平均溢价 {stats['均值(%)']:.2f}%")
    else:
        print(f"  • 国内黄金平均折价 {abs(stats['均值(%)']):.2f}%")

    print(f"  • 价差率波动标准差为 {stats['标准差(%)']:.2f}%")
    print(f"  • 68%的时间价差率在 [{mean - std:.2f}%, {mean + std:.2f}%] 范围内")

    outliers = df[(df['spread_rate'] > mean + 2*std) | (df['spread_rate'] < mean - 2*std)]
    if len(outliers) > 0:
        print(f"  • 存在 {len(outliers)} 个异常数据点(超出2σ范围)")

    print("=" * 95)


def analyze_arbitrage(df: pd.DataFrame, stats: Dict):
    """
    统计套利分析
    """
    spread_rates = df['spread_rate'].values
    mean = stats['均值(%)']
    std = stats['标准差(%)']

    # 计算动态Z-score
    window = 20
    rolling_mean = pd.Series(spread_rates).rolling(window=window).mean()
    rolling_std = pd.Series(spread_rates).rolling(window=window).std()
    z_scores = (pd.Series(spread_rates) - rolling_mean) / rolling_std
    z_scores = z_scores.dropna()

    print("\n" + "=" * 70)
    print(" 统计套利开仓点位分析")
    print("=" * 70)

    print("\n【动态Z-score统计】")
    print("-" * 50)
    print(f"  滚动窗口: {window}个周期（5小时）")
    print(f"  Z-score范围: [{z_scores.min():.2f}, {z_scores.max():.2f}]")

    print("\n【开仓点位建议】")
    print("-" * 50)
    print('  ┌──────────────┬─────────────────────────────────┐')
    print('  │   Z-score    │            操作建议              │')
    print('  ├──────────────┼─────────────────────────────────┤')
    print('  │   > +2.0     │ 强烈做空 (溢价过高)              │')
    print('  │   > +1.5     │ 轻仓做空                        │')
    print('  │  -1.5~+1.5   │ 观望                            │')
    print('  │   < -1.5     │ 轻仓做多                        │')
    print('  │   < -2.0     │ 强烈做多 (溢价过低)              │')
    print('  └──────────────┴─────────────────────────────────┘')

    # 回测
    print("\n【策略回测】")
    print("-" * 50)
    for z_threshold in [1.5, 2.0]:
        win_short, total_short = 0, 0
        for i in range(len(z_scores) - 10):
            if z_scores.iloc[i] > z_threshold:
                total_short += 1
                if any(z_scores.iloc[i+1:i+11] <= 0.5):
                    win_short += 1

        win_long, total_long = 0, 0
        for i in range(len(z_scores) - 10):
            if z_scores.iloc[i] < -z_threshold:
                total_long += 1
                if any(z_scores.iloc[i+1:i+11] >= -0.5):
                    win_long += 1

        short_wr = win_short/total_short*100 if total_short > 0 else 0
        long_wr = win_long/total_long*100 if total_long > 0 else 0

        print(f"  Z-score阈值 ±{z_threshold}:")
        print(f"    做空: {total_short}次信号, 胜率{short_wr:.0f}%")
        print(f"    做多: {total_long}次信号, 胜率{long_wr:.0f}%")

    # 当前状态
    current_z = z_scores.iloc[-1]
    current_spread = spread_rates[-1]
    print(f"\n【当前状态】")
    print("-" * 50)
    print(f"  当前价差率: {current_spread:.2f}%")
    print(f"  当前Z-score: {current_z:.2f}")
    if current_z > 2:
        print(f"  📉 建议: 做空信号")
    elif current_z > 1.5:
        print(f"  📉 建议: 轻仓做空")
    elif current_z < -2:
        print(f"  📈 建议: 做多信号")
    elif current_z < -1.5:
        print(f"  📈 建议: 轻仓做多")
    else:
        print(f"  ⏸️ 建议: 观望")

    print("=" * 70)


def export_to_csv(df: pd.DataFrame, filename: str = 'gold_spread_backtest_15m.csv'):
    """
    导出回测结果到CSV
    """
    export_df = df.copy()
    export_df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"\n数据已导出到: {filename}")


def plot_spread_chart(df: pd.DataFrame, stats: Dict, interval: str, save_path: str = 'gold_spread_chart_15m.png'):
    """
    绘制价差率时间折线图
    """
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False

    fig, axes = plt.subplots(3, 1, figsize=(16, 12))
    fig.suptitle(f'黄金价差率回测分析 ({interval} K线)\n上期所AU主力 vs Binance XAUUSDT', fontsize=14, fontweight='bold')

    dates = pd.to_datetime(df['datetime'])
    mean = stats['均值(%)']
    std = stats['标准差(%)']

    # 图1: 价差率时间序列
    ax1 = axes[0]
    ax1.plot(dates, df['spread_rate'], 'b-', linewidth=1, alpha=0.8, label='价差率')
    ax1.axhline(y=mean, color='red', linestyle='--', linewidth=1.5, label=f'均值 ({mean:.2f}%)')
    ax1.axhline(y=mean + std, color='orange', linestyle=':', linewidth=1, label=f'+1σ ({mean+std:.2f}%)')
    ax1.axhline(y=mean - std, color='orange', linestyle=':', linewidth=1, label=f'-1σ ({mean-std:.2f}%)')
    ax1.axhline(y=mean + 2*std, color='green', linestyle=':', linewidth=1, alpha=0.7)
    ax1.axhline(y=mean - 2*std, color='green', linestyle=':', linewidth=1, alpha=0.7)
    ax1.fill_between(dates, mean - std, mean + std, alpha=0.2, color='blue', label='1σ区间')
    ax1.set_ylabel('价差率 (%)', fontsize=11)
    ax1.set_title('价差率时间序列 (国内溢价为正)', fontsize=12)
    ax1.legend(loc='upper left', fontsize=8, ncol=2)
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 图2: 价格对比
    ax2 = axes[1]
    ax2_twin = ax2.twinx()
    line1, = ax2.plot(dates, df['shfe_close'], 'r-', linewidth=1, alpha=0.8, label='上期所AU (元/克)')
    line2, = ax2_twin.plot(dates, df['binance_close'], 'b-', linewidth=1, alpha=0.8, label='Binance XAU ($/oz)')
    ax2.set_ylabel('上期所价格 (元/克)', fontsize=11, color='red')
    ax2_twin.set_ylabel('Binance价格 (美元/盎司)', fontsize=11, color='blue')
    ax2.set_title('价格走势对比', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='red')
    ax2_twin.tick_params(axis='y', labelcolor='blue')
    ax2.legend([line1, line2], [l.get_label() for l in [line1, line2]], loc='upper left', fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax2.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 图3: 分布直方图
    ax3 = axes[2]
    ax3.hist(df['spread_rate'], bins=30, edgecolor='black', alpha=0.7, color='steelblue')
    ax3.axvline(x=mean, color='red', linestyle='--', linewidth=2, label=f'均值: {mean:.2f}%')
    ax3.axvline(x=mean + std, color='orange', linestyle=':', linewidth=1.5, label=f'±1σ: {std:.2f}%')
    ax3.axvline(x=mean - std, color='orange', linestyle=':', linewidth=1.5)
    ax3.set_xlabel('价差率 (%)', fontsize=11)
    ax3.set_ylabel('频次', fontsize=11)
    ax3.set_title(f'价差率分布 (样本: {stats["样本数量"]}, 方差: {stats["方差"]:.4f}, 标准差: {std:.2f}%)', fontsize=12)
    ax3.legend(loc='upper right', fontsize=9)
    ax3.grid(True, alpha=0.3, axis='y')

    textstr = f'样本数: {stats["样本数量"]}\n均值: {mean:.2f}%\n方差: {stats["方差"]:.4f}\n标准差: {std:.2f}%'
    ax3.text(0.02, 0.98, textstr, transform=ax3.transAxes, fontsize=9,
             verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"图表已保存到: {save_path}")


# ============== 主程序 ==============

if __name__ == "__main__":
    import sys

    interval = DEFAULT_INTERVAL
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg in ['5m', '15m', '30m', '1h']:
            interval = arg

    print(f"使用 {interval} K线周期进行黄金回测")

    result = backtest_spread(interval)

    if result is not None and len(result) > 0:
        stats = analyze_spread(result)
        print_results(result, stats, interval)
        analyze_arbitrage(result, stats)
        export_to_csv(result, f'gold_spread_backtest_{interval}.csv')
        plot_spread_chart(result, stats, interval, f'gold_spread_chart_{interval}.png')
    else:
        print("\n回测失败，请检查网络连接或数据源可用性")
