#!/usr/bin/env python3
"""
白银主力合约(上期所AG)与xyz:SILVER的z-score分析
剔除休市时间后进行拟合分析
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ============== 配置 ==============
# 单位换算: 1盎司 = 0.0311035千克
OUNCE_TO_KG = 0.0311035

# 上期所白银交易时间 (北京时间)
# 日盘: 09:00-10:15, 10:30-11:30, 13:30-15:00
# 夜盘: 21:00-次日02:30
SHFE_TRADING_HOURS = {
    'day_session_1': (9, 10),    # 9:00-10:15 (整点小时)
    'day_session_2': (10, 11),   # 10:30-11:30
    'day_session_3': (13, 15),   # 13:30-15:00
    'night_session': (21, 2),    # 21:00-02:30
}

# 文件路径
BASE_PATH = '/Users/wanghao/Library/Mobile Documents/com~apple~CloudDocs/查询白银主力合约和binance'
XYZ_SILVER_PATH = f'{BASE_PATH}/data/silver/xyz_silver.json'

# ============== 数据获取函数 ==============

def get_shfe_silver_hourly() -> pd.DataFrame:
    """
    通过akshare获取上期所白银期货小时级数据
    """
    try:
        import akshare as ak
        # 获取白银主力连续合约60分钟数据
        df = ak.futures_zh_minute_sina(symbol='AG0', period='60')

        if df is not None and len(df) > 0:
            df = df.rename(columns={'datetime': 'datetime'})
            df['datetime'] = pd.to_datetime(df['datetime'])
            return df[['datetime', 'open', 'high', 'low', 'close', 'volume', 'hold']]
    except Exception as e:
        print(f"[错误] 获取akshare分钟数据失败: {e}")

    return pd.DataFrame()


def load_xyz_silver() -> pd.DataFrame:
    """
    加载xyz:SILVER数据
    """
    try:
        with open(XYZ_SILVER_PATH, 'r') as f:
            data = json.load(f)

        df = pd.DataFrame(data)
        df['datetime'] = pd.to_datetime(df['t'], unit='ms')
        df['close'] = df['c'].astype(float)
        df['open'] = df['o'].astype(float)
        df['high'] = df['h'].astype(float)
        df['low'] = df['l'].astype(float)

        return df[['datetime', 't', 'open', 'high', 'low', 'close']]
    except Exception as e:
        print(f"[错误] 加载xyz_silver数据失败: {e}")
        return pd.DataFrame()


def is_shfe_trading_hour(dt: datetime) -> bool:
    """
    判断给定时间是否在上期所交易时间内
    注意: xyz:SILVER使用UTC时间，需要转换为北京时间判断
    """
    # 转换为北京时间 (UTC+8)
    beijing_dt = dt + timedelta(hours=8)
    hour = beijing_dt.hour
    weekday = beijing_dt.weekday()  # 0=周一, 6=周日

    # 周末不交易
    if weekday >= 5:  # 周六、周日
        return False

    # 周一没有夜盘(前一天是周日)
    # 周五夜盘只到次日凌晨

    # 日盘时间: 9:00-15:00 (小时级别简化)
    if 9 <= hour <= 14:  # 9:00-14:59
        return True
    if hour == 15:  # 15:00这一小时的开始
        return True

    # 夜盘时间: 21:00-02:30
    if 21 <= hour <= 23:  # 21:00-23:59
        return True
    if 0 <= hour <= 2:  # 00:00-02:30
        # 周六凌晨没有夜盘(周五夜盘结束)
        if weekday == 5:  # 周六
            return False
        return True

    return False


def filter_trading_hours(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤出交易时间的数据
    """
    df = df.copy()
    df['is_trading'] = df['datetime'].apply(is_shfe_trading_hour)
    filtered_df = df[df['is_trading']].copy()
    filtered_df = filtered_df.drop(columns=['is_trading'])
    return filtered_df


def get_historical_usd_cny_rates(start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取历史美元兑人民币汇率
    使用akshare获取每日汇率数据
    """
    try:
        import akshare as ak
        # 获取美元兑人民币汇率历史数据
        df = ak.currency_boc_sina(symbol="美元", start_date=start_date, end_date=end_date)

        if df is not None and len(df) > 0:
            df = df.rename(columns={'日期': 'date', '中行汇买价': 'rate'})
            df['date'] = pd.to_datetime(df['date'])
            df['rate'] = df['rate'].astype(float) / 100  # 转换为实际汇率
            return df[['date', 'rate']]
    except Exception as e:
        print(f"[警告] 获取akshare汇率数据失败: {e}")

    return pd.DataFrame()


def get_usd_cny_rate_for_date(rates_df: pd.DataFrame, target_date: datetime) -> float:
    """
    获取指定日期的汇率，如果没有则使用最近的汇率
    """
    if rates_df is None or len(rates_df) == 0:
        return 7.25  # 默认汇率

    target = pd.to_datetime(target_date).normalize()

    # 查找该日期或之前最近的汇率
    available = rates_df[rates_df['date'] <= target]
    if len(available) > 0:
        return available.iloc[-1]['rate']

    # 如果没有更早的数据，使用最早的汇率
    return rates_df.iloc[0]['rate']


def convert_usd_oz_to_cny_kg(price_usd: float, usd_cny: float) -> float:
    """将美元/盎司转换为人民币/千克"""
    return (price_usd * usd_cny) / OUNCE_TO_KG


# ============== 分析函数 ==============

def calculate_zscore(df: pd.DataFrame, window: int = 24) -> pd.DataFrame:
    """
    计算滚动z-score
    """
    df = df.copy()

    # 计算滚动均值和标准差
    df['spread_mean'] = df['spread'].rolling(window=window, min_periods=10).mean()
    df['spread_std'] = df['spread'].rolling(window=window, min_periods=10).std()

    # 计算z-score
    df['zscore'] = (df['spread'] - df['spread_mean']) / df['spread_std']

    return df


def analyze_trading_zones(df: pd.DataFrame) -> dict:
    """
    分析交易区间
    """
    valid_df = df.dropna(subset=['zscore'])

    if len(valid_df) == 0:
        return {}

    zscore = valid_df['zscore'].values
    spread = valid_df['spread'].values
    spread_rate = valid_df['spread_rate'].values

    stats = {
        '样本数量': len(zscore),
        'z-score均值': np.mean(zscore),
        'z-score标准差': np.std(zscore),
        'z-score最大值': np.max(zscore),
        'z-score最小值': np.min(zscore),
        '价差均值': np.mean(spread),
        '价差标准差': np.std(spread),
        '价差率均值(%)': np.mean(spread_rate),
        '价差率标准差(%)': np.std(spread_rate),
    }

    # 计算分位数
    for pct in [5, 10, 25, 50, 75, 90, 95]:
        stats[f'z-score {pct}%分位'] = np.percentile(zscore, pct)

    return stats


def print_results(df: pd.DataFrame, stats: dict):
    """打印分析结果"""
    print("\n" + "=" * 100)
    print(" 白银主力合约(AG) vs xyz:SILVER z-score分析")
    print(" (已剔除上期所休市时间)")
    print("=" * 100)

    print(f"\n数据范围: {df['datetime'].min()} ~ {df['datetime'].max()}")
    print(f"有效样本: {stats['样本数量']} 个数据点")

    print("\n【Z-Score统计】")
    print("-" * 50)
    print(f"  均值:     {stats['z-score均值']:.4f}")
    print(f"  标准差:   {stats['z-score标准差']:.4f}")
    print(f"  最大值:   {stats['z-score最大值']:.4f}")
    print(f"  最小值:   {stats['z-score最小值']:.4f}")

    print("\n【Z-Score分位数】")
    print("-" * 50)
    for pct in [5, 10, 25, 50, 75, 90, 95]:
        print(f"  {pct:>3}%分位: {stats[f'z-score {pct}%分位']:.4f}")

    print("\n【价差统计】")
    print("-" * 50)
    print(f"  价差均值:     {stats['价差均值']:.2f} 元/千克")
    print(f"  价差标准差:   {stats['价差标准差']:.2f} 元/千克")
    print(f"  价差率均值:   {stats['价差率均值(%)']:.4f} %")
    print(f"  价差率标准差: {stats['价差率标准差(%)']:.4f} %")

    # 交易区间建议
    mean_spread = stats['价差均值']
    std_spread = stats['价差标准差']

    print("\n【交易区间建议】")
    print("-" * 50)
    print(f"  中性区间 (±0.5σ): [{mean_spread - 0.5*std_spread:.2f}, {mean_spread + 0.5*std_spread:.2f}] 元/kg")
    print(f"  1σ 区间:          [{mean_spread - std_spread:.2f}, {mean_spread + std_spread:.2f}] 元/kg")
    print(f"  2σ 区间:          [{mean_spread - 2*std_spread:.2f}, {mean_spread + 2*std_spread:.2f}] 元/kg")

    # Z-score交易信号
    print("\n【基于Z-Score的交易信号】")
    print("-" * 50)
    print("  做多价差 (买AG卖xyz):")
    print(f"    入场: z-score < -2.0 (极度折价)")
    print(f"    止盈: z-score > 0 (回归均值)")
    print("  做空价差 (卖AG买xyz):")
    print(f"    入场: z-score > +2.0 (极度溢价)")
    print(f"    止盈: z-score < 0 (回归均值)")

    # 打印最近数据
    print("\n【最近20条数据】")
    print("-" * 120)
    print(f"{'时间':<20} {'AG(元/kg)':<12} {'xyz($/oz)':<12} {'xyz换算':<12} {'价差':<12} {'价差率(%)':<12} {'z-score':<10}")
    print("-" * 120)

    for _, row in df.dropna(subset=['zscore']).tail(20).iterrows():
        dt_str = row['datetime'].strftime('%Y-%m-%d %H:%M')
        zscore_val = row['zscore']
        signal = ""
        if zscore_val > 2:
            signal = "⬆️ 做空"
        elif zscore_val < -2:
            signal = "⬇️ 做多"
        print(f"{dt_str:<20} {row['shfe_close']:<12.2f} {row['xyz_close']:<12.4f} {row['xyz_cny']:<12.2f} {row['spread']:<+12.2f} {row['spread_rate']:<+12.4f} {zscore_val:<+10.4f} {signal}")

    print("=" * 100)


def plot_analysis(df: pd.DataFrame, stats: dict, save_path: str):
    """绘制分析图表"""
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False

    valid_df = df.dropna(subset=['zscore'])

    fig, axes = plt.subplots(4, 1, figsize=(16, 16))
    fig.suptitle('白银主力合约(AG) vs xyz:SILVER Z-Score分析\n(已剔除上期所休市时间)', fontsize=14, fontweight='bold')

    dates = pd.to_datetime(valid_df['datetime'])

    # 图1: Z-Score时间序列
    ax1 = axes[0]
    ax1.plot(dates, valid_df['zscore'], 'b-', linewidth=1, alpha=0.8, label='Z-Score')
    ax1.axhline(y=0, color='black', linestyle='-', linewidth=1)
    ax1.axhline(y=2, color='red', linestyle='--', linewidth=1.5, label='±2σ (开仓信号)')
    ax1.axhline(y=-2, color='red', linestyle='--', linewidth=1.5)
    ax1.axhline(y=1, color='orange', linestyle=':', linewidth=1, alpha=0.7, label='±1σ')
    ax1.axhline(y=-1, color='orange', linestyle=':', linewidth=1, alpha=0.7)
    ax1.fill_between(dates, -1, 1, alpha=0.1, color='green', label='中性区间')
    ax1.fill_between(dates, 2, valid_df['zscore'].max() + 0.5, where=valid_df['zscore'] > 2, alpha=0.3, color='red')
    ax1.fill_between(dates, valid_df['zscore'].min() - 0.5, -2, where=valid_df['zscore'] < -2, alpha=0.3, color='green')

    ax1.set_ylabel('Z-Score', fontsize=11)
    ax1.set_title('Z-Score时间序列 (做多区域=绿色, 做空区域=红色)', fontsize=12)
    ax1.legend(loc='upper left', fontsize=8, ncol=2)
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 图2: 价差率时间序列 (百分比)
    ax2 = axes[1]
    mean_spread_rate = stats['价差率均值(%)']
    std_spread_rate = stats['价差率标准差(%)']

    ax2.plot(dates, valid_df['spread_rate'], 'b-', linewidth=1, alpha=0.8, label='价差率 ((AG-xyz)/xyz)')
    ax2.axhline(y=mean_spread_rate, color='red', linestyle='--', linewidth=1.5, label=f'均值 ({mean_spread_rate:.2f}%)')
    ax2.axhline(y=mean_spread_rate + std_spread_rate, color='orange', linestyle=':', linewidth=1, label=f'±1σ')
    ax2.axhline(y=mean_spread_rate - std_spread_rate, color='orange', linestyle=':', linewidth=1)
    ax2.axhline(y=mean_spread_rate + 2*std_spread_rate, color='green', linestyle=':', linewidth=1, alpha=0.7)
    ax2.axhline(y=mean_spread_rate - 2*std_spread_rate, color='green', linestyle=':', linewidth=1, alpha=0.7)
    ax2.fill_between(dates, mean_spread_rate - std_spread_rate, mean_spread_rate + std_spread_rate, alpha=0.2, color='blue')

    ax2.set_ylabel('价差率 (%)', fontsize=11)
    ax2.set_title('价差率时间序列 (国内溢价百分比)', fontsize=12)
    ax2.legend(loc='upper left', fontsize=8)
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 图3: 价格对比
    ax3 = axes[2]
    ax3_twin = ax3.twinx()

    line1, = ax3.plot(dates, valid_df['shfe_close'], 'r-', linewidth=1, alpha=0.8, label='AG主力 (元/kg)')
    line2, = ax3_twin.plot(dates, valid_df['xyz_close'], 'b-', linewidth=1, alpha=0.8, label='xyz:SILVER ($/oz)')

    ax3.set_ylabel('AG价格 (元/千克)', fontsize=11, color='red')
    ax3_twin.set_ylabel('xyz价格 (美元/盎司)', fontsize=11, color='blue')
    ax3.set_title('价格走势对比', fontsize=12)
    ax3.tick_params(axis='y', labelcolor='red')
    ax3_twin.tick_params(axis='y', labelcolor='blue')

    lines = [line1, line2]
    labels = [l.get_label() for l in lines]
    ax3.legend(lines, labels, loc='upper left', fontsize=9)
    ax3.grid(True, alpha=0.3)
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 图4: Z-Score分布直方图
    ax4 = axes[3]
    n, bins, patches = ax4.hist(valid_df['zscore'], bins=40, edgecolor='black', alpha=0.7, color='steelblue')

    ax4.axvline(x=0, color='black', linestyle='-', linewidth=2, label='均值=0')
    ax4.axvline(x=2, color='red', linestyle='--', linewidth=1.5, label='±2σ (开仓)')
    ax4.axvline(x=-2, color='red', linestyle='--', linewidth=1.5)
    ax4.axvline(x=1, color='orange', linestyle=':', linewidth=1)
    ax4.axvline(x=-1, color='orange', linestyle=':', linewidth=1)

    # 标注交易区域
    ax4.axvspan(2, valid_df['zscore'].max() + 0.5, alpha=0.3, color='red', label='做空区域')
    ax4.axvspan(valid_df['zscore'].min() - 0.5, -2, alpha=0.3, color='green', label='做多区域')

    ax4.set_xlabel('Z-Score', fontsize=11)
    ax4.set_ylabel('频次', fontsize=11)
    ax4.set_title(f'Z-Score分布 (样本: {stats["样本数量"]}, 均值: {stats["z-score均值"]:.4f}, 标准差: {stats["z-score标准差"]:.4f})', fontsize=12)
    ax4.legend(loc='upper right', fontsize=8)
    ax4.grid(True, alpha=0.3, axis='y')

    # 统计信息
    short_count = len(valid_df[valid_df['zscore'] > 2])
    long_count = len(valid_df[valid_df['zscore'] < -2])
    textstr = f'做空信号 (z>2): {short_count} 次 ({short_count/len(valid_df)*100:.1f}%)\n做多信号 (z<-2): {long_count} 次 ({long_count/len(valid_df)*100:.1f}%)'
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    ax4.text(0.02, 0.98, textstr, transform=ax4.transAxes, fontsize=10,
             verticalalignment='top', bbox=props)

    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()

    print(f"\n图表已保存到: {save_path}")


# ============== 主程序 ==============

def main():
    print("=" * 60)
    print(" 白银主力合约 vs xyz:SILVER Z-Score分析")
    print(" (剔除上期所休市时间)")
    print("=" * 60)

    # 1. 获取上期所白银数据
    print("\n1. 获取上期所白银主力合约数据...")
    shfe_df = get_shfe_silver_hourly()

    if len(shfe_df) == 0:
        print("   [错误] 无法获取上期所数据")
        return

    print(f"   获取到 {len(shfe_df)} 条记录")
    print(f"   时间范围: {shfe_df['datetime'].min()} ~ {shfe_df['datetime'].max()}")

    # 2. 加载xyz:SILVER数据
    print("\n2. 加载xyz:SILVER数据...")
    xyz_df = load_xyz_silver()

    if len(xyz_df) == 0:
        print("   [错误] 无法加载xyz:SILVER数据")
        return

    print(f"   获取到 {len(xyz_df)} 条记录")
    print(f"   时间范围: {xyz_df['datetime'].min()} ~ {xyz_df['datetime'].max()}")

    # 3. 过滤交易时间
    print("\n3. 剔除休市时间...")
    xyz_filtered = filter_trading_hours(xyz_df)
    print(f"   过滤后剩余 {len(xyz_filtered)} 条记录 (剔除了 {len(xyz_df) - len(xyz_filtered)} 条)")

    # 4. 数据对齐
    print("\n4. 对齐数据...")

    # 将时间截断到小时
    shfe_df['datetime'] = pd.to_datetime(shfe_df['datetime']).dt.floor('H')
    xyz_filtered['datetime'] = pd.to_datetime(xyz_filtered['datetime']).dt.floor('H')

    # 合并数据
    merged = pd.merge(
        shfe_df[['datetime', 'close']].rename(columns={'close': 'shfe_close'}),
        xyz_filtered[['datetime', 'close']].rename(columns={'close': 'xyz_close'}),
        on='datetime',
        how='inner'
    )

    if len(merged) == 0:
        print("   [错误] 没有重叠的时间数据")
        return

    merged = merged.sort_values('datetime').reset_index(drop=True)
    print(f"   共有 {len(merged)} 个重叠时间点")

    # 5. 获取历史汇率
    print("\n5. 获取历史汇率...")
    start_date = merged['datetime'].min().strftime('%Y%m%d')
    end_date = merged['datetime'].max().strftime('%Y%m%d')
    rates_df = get_historical_usd_cny_rates(start_date, end_date)

    if len(rates_df) > 0:
        print(f"   获取到 {len(rates_df)} 条汇率记录")
        print(f"   汇率范围: {rates_df['rate'].min():.4f} ~ {rates_df['rate'].max():.4f}")
    else:
        print("   [警告] 无法获取历史汇率，使用默认汇率 7.25")

    # 6. 计算价差 (使用每日对应汇率)
    print("\n6. 计算价差 (使用历史汇率)...")

    def calc_xyz_cny(row):
        rate = get_usd_cny_rate_for_date(rates_df, row['datetime'])
        return convert_usd_oz_to_cny_kg(row['xyz_close'], rate)

    def get_rate_for_row(row):
        return get_usd_cny_rate_for_date(rates_df, row['datetime'])

    merged['usd_cny_rate'] = merged.apply(get_rate_for_row, axis=1)
    merged['xyz_cny'] = merged.apply(calc_xyz_cny, axis=1)
    merged['spread'] = merged['shfe_close'] - merged['xyz_cny']
    merged['spread_rate'] = (merged['spread'] / merged['xyz_cny']) * 100

    # 7. 计算z-score
    print("\n7. 计算Z-Score (24小时滚动窗口)...")
    merged = calculate_zscore(merged, window=24)

    # 8. 统计分析
    print("\n8. 统计分析...")
    stats = analyze_trading_zones(merged)

    # 9. 打印结果
    print_results(merged, stats)

    # 10. 保存结果
    output_csv = f'{BASE_PATH}/backtest/silver/shfe_xyz_zscore.csv'
    merged.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"\n数据已导出到: {output_csv}")

    # 11. 绘制图表
    output_png = f'{BASE_PATH}/backtest/silver/shfe_xyz_zscore.png'
    plot_analysis(merged, stats, output_png)


if __name__ == "__main__":
    main()
