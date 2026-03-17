# unit_converter.py - 白银合约单位换算和费率计算
#
# SHFE AG: RMB/kg, 15 kg/手
# HL SILVER: USD/oz
# 换算: 1 troy oz = 0.0311035 kg

# ==================== 常量 ====================

OUNCE_TO_KG = 0.0311035          # 1 金衡盎司 = 0.0311035 kg
AG_LOT_KG = 15                   # SHFE 白银: 1手 = 15 kg
HL_LOT_OZ = AG_LOT_KG / OUNCE_TO_KG  # ~482.25 oz (1手AG对应的HL盎司数)
AG_TICK_SIZE = 1                  # AG 最小变动价位: 1 RMB/kg
AG_FEE_RATE = 0.00005            # 万分之0.5 per side
HL_FEE_RATE = 0.00035            # 0.035% taker per side
SLIPPAGE_RATE = 0.0001           # 0.01% per side per leg
DEFAULT_USDCNY = 7.25            # 默认汇率 (fallback)


# ==================== 价格换算 ====================

def hl_usd_oz_to_cny_kg(price_usd_oz: float, usdcny: float) -> float:
    """HL价格 (USD/oz) → RMB/kg"""
    return price_usd_oz * usdcny / OUNCE_TO_KG


def cny_kg_to_usd_oz(price_cny_kg: float, usdcny: float) -> float:
    """RMB/kg → USD/oz"""
    if usdcny <= 0:
        return 0
    return price_cny_kg * OUNCE_TO_KG / usdcny


# ==================== 数量换算 ====================

def ag_lots_to_hl_oz(lots: int) -> float:
    """AG 手数 → HL 盎司数: lots * 15 / 0.0311035"""
    return lots * AG_LOT_KG / OUNCE_TO_KG


def hl_oz_to_ag_lots(oz: float) -> int:
    """HL 盎司数 → AG 手数 (向下取整)"""
    return int(oz * OUNCE_TO_KG / AG_LOT_KG)


def ag_lots_to_kg(lots: int) -> float:
    """AG 手数 → 公斤数"""
    return lots * AG_LOT_KG


# ==================== 费用计算 ====================

def calculate_ag_fee(price_cny_kg: float, lots: int) -> float:
    """AG 单边手续费 (RMB) = price * 15kg * lots * 0.00005"""
    return price_cny_kg * AG_LOT_KG * lots * AG_FEE_RATE


def calculate_hl_fee(price_usd_oz: float, size_oz: float) -> float:
    """HL 单边手续费 (USD) = price * size * 0.00035"""
    return price_usd_oz * size_oz * HL_FEE_RATE


def calculate_round_trip_fee(
    ag_entry_price: float,
    ag_exit_price: float,
    hl_entry_price: float,
    hl_exit_price: float,
    lots: int,
    usdcny: float,
) -> float:
    """计算往返总费用 (RMB), 包含 AG + HL + 滑点"""
    hl_size_oz = ag_lots_to_hl_oz(lots)

    # AG 往返手续费 (RMB)
    ag_fee = (ag_entry_price + ag_exit_price) * AG_LOT_KG * lots * AG_FEE_RATE

    # HL 往返手续费 (USD → RMB)
    hl_fee_usd = (hl_entry_price + hl_exit_price) * hl_size_oz * HL_FEE_RATE
    hl_fee_rmb = hl_fee_usd * usdcny

    # 滑点 (双边双腿)
    ag_notional = (ag_entry_price + ag_exit_price) / 2 * AG_LOT_KG * lots
    hl_notional_rmb = (hl_entry_price + hl_exit_price) / 2 * hl_size_oz * usdcny
    slippage = (ag_notional + hl_notional_rmb) * SLIPPAGE_RATE * 2

    return ag_fee + hl_fee_rmb + slippage
