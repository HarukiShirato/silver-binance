# unit_converter.py - unit conversion and fee helpers for AG/HL arbitrage

from config import STRATEGY

# Constants
OUNCE_TO_KG = 0.0311035
AG_LOT_KG = 15
HL_LOT_OZ = AG_LOT_KG / OUNCE_TO_KG
AG_TICK_SIZE = 1
SLIPPAGE_RATE = 0.0001
DEFAULT_USDCNY = 7.25


# Price conversion

def hl_usd_oz_to_cny_kg(price_usd_oz: float, usdcny: float) -> float:
    """Convert HL price from USD/oz to RMB/kg."""
    return price_usd_oz * usdcny / OUNCE_TO_KG


def cny_kg_to_usd_oz(price_cny_kg: float, usdcny: float) -> float:
    """Convert price from RMB/kg to USD/oz."""
    if usdcny <= 0:
        return 0
    return price_cny_kg * OUNCE_TO_KG / usdcny


# Size conversion

def ag_lots_to_hl_oz(lots: int) -> float:
    """AG lots -> HL ounces."""
    return lots * AG_LOT_KG / OUNCE_TO_KG


def hl_oz_to_ag_lots(oz: float) -> int:
    """HL ounces -> AG lots (floor)."""
    return int(oz * OUNCE_TO_KG / AG_LOT_KG)


def ag_lots_to_kg(lots: int) -> float:
    """AG lots -> kilograms."""
    return lots * AG_LOT_KG


# Fee calculation

def calculate_ag_fee(price_cny_kg: float, lots: int) -> float:
    """AG single-side fee in RMB = price * 15kg * lots * fee_rate."""
    lots = max(lots, 0)
    notional_rmb = price_cny_kg * AG_LOT_KG * lots
    return notional_rmb * STRATEGY.ag_fee_rate


def calculate_hl_fee(price_usd_oz: float, size_oz: float) -> float:
    """HL single-side fee in USD = price * size * fee_rate."""
    return price_usd_oz * size_oz * STRATEGY.hl_fee_rate


def calculate_round_trip_fee(
    ag_entry_price: float,
    ag_exit_price: float,
    hl_entry_price: float,
    hl_exit_price: float,
    lots: int,
    usdcny: float,
) -> float:
    """Estimate round-trip total cost in RMB: AG + HL + slippage."""
    lots = max(lots, 0)
    hl_size_oz = ag_lots_to_hl_oz(lots)

    # AG round-trip fees (RMB)
    ag_fee_entry = ag_entry_price * AG_LOT_KG * lots * STRATEGY.ag_fee_rate
    ag_fee_exit = ag_exit_price * AG_LOT_KG * lots * STRATEGY.ag_fee_rate
    ag_fee = ag_fee_entry + ag_fee_exit

    # HL round-trip fees (USD -> RMB)
    hl_fee_usd = (hl_entry_price + hl_exit_price) * hl_size_oz * STRATEGY.hl_fee_rate
    hl_fee_rmb = hl_fee_usd * usdcny

    # Slippage (both legs, both sides)
    ag_notional = (ag_entry_price + ag_exit_price) / 2 * AG_LOT_KG * lots
    hl_notional_rmb = (hl_entry_price + hl_exit_price) / 2 * hl_size_oz * usdcny
    slippage = (ag_notional + hl_notional_rmb) * SLIPPAGE_RATE * 2

    return ag_fee + hl_fee_rmb + slippage
