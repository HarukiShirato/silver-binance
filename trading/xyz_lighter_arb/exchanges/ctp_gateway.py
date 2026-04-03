# exchanges/ctp_gateway.py - 国贸期货 CTP 交易接口
#
# 通过 openctp-ctp 连接国贸期货仿真/实盘环境
# 支持行情订阅、下单、撤单、查持仓/资金
#
# 国贸期货仿真环境:
#   BrokerID: 0187
#   前置地址: 220.160.125.12
#   交易端口: 61209
#   行情端口: 61219

import asyncio
import time
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Callable, Any
from decimal import Decimal

from openctp_ctp import mdapi, tdapi

import logging
logger = logging.getLogger(__name__)


# ==================== 数据结构 ====================

@dataclass
class TickData:
    """行情 tick 数据"""
    instrument_id: str          # 合约代码 (如 ag2506)
    last_price: float           # 最新价
    bid_price1: float           # 买一价
    bid_volume1: int            # 买一量
    ask_price1: float           # 卖一价
    ask_volume1: int            # 卖一量
    volume: int                 # 成交量
    open_interest: float        # 持仓量
    upper_limit: float          # 涨停价
    lower_limit: float          # 跌停价
    timestamp: float            # 时间戳
    trading_day: str = ""       # 交易日
    update_time: str = ""       # 更新时间


@dataclass
class OrderResult:
    """下单结果"""
    order_ref: str              # 报单引用
    instrument_id: str
    direction: str              # "BUY" / "SELL"
    price: float
    volume: int
    status: str = "pending"     # pending, filled, partial, cancelled, error
    filled_volume: int = 0
    filled_price: float = 0.0
    error_msg: str = ""
    order_sys_id: str = ""      # 交易所报单编号


@dataclass
class PositionData:
    """持仓数据"""
    instrument_id: str
    direction: str              # "LONG" / "SHORT"
    volume: int                 # 持仓量
    available: int              # 可用量
    avg_price: float            # 持仓均价
    profit: float               # 持仓盈亏
    margin: float               # 保证金


@dataclass
class AccountData:
    """账户资金数据"""
    balance: float              # 总资金
    available: float            # 可用资金
    frozen: float               # 冻结资金
    margin: float               # 保证金占用
    profit: float               # 持仓盈亏
    commission: float           # 手续费


# ==================== 行情 SPI (回调) ====================

class CTPMdSpi(mdapi.CThostFtdcMdSpi):
    """CTP 行情回调处理"""

    def __init__(self, gateway: 'CTPGateway'):
        super().__init__()
        self.gateway = gateway
        self._login_event = threading.Event()
        self._connected = False

    def OnFrontConnected(self):
        logger.info("CTP MD: 前置连接成功")
        self._connected = True
        # 自动登录
        req = mdapi.CThostFtdcReqUserLoginField()
        req.BrokerID = self.gateway.broker_id
        req.UserID = self.gateway.user_id
        req.Password = self.gateway.password
        self.gateway._md_api.ReqUserLogin(req, 0)

    def OnFrontDisconnected(self, nReason: int):
        logger.warning(f"CTP MD: 前置断开, 原因={nReason}")
        self._connected = False

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.ErrorID != 0:
            logger.error(f"CTP MD: 登录失败, 错误={pRspInfo.ErrorID}: {pRspInfo.ErrorMsg}")
            return
        logger.info(f"CTP MD: 登录成功, 交易日={pRspUserLogin.TradingDay}")
        self.gateway._md_trading_day = pRspUserLogin.TradingDay
        self._login_event.set()

    def OnRspSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.ErrorID != 0:
            logger.error(f"CTP MD: 订阅失败 {pSpecificInstrument.InstrumentID}, "
                        f"错误={pRspInfo.ErrorID}: {pRspInfo.ErrorMsg}")
        else:
            logger.info(f"CTP MD: 订阅成功 {pSpecificInstrument.InstrumentID}")

    def OnRtnDepthMarketData(self, pDepthMarketData):
        """行情推送回调"""
        if not pDepthMarketData:
            return

        tick = TickData(
            instrument_id=pDepthMarketData.InstrumentID,
            last_price=pDepthMarketData.LastPrice if pDepthMarketData.LastPrice < 1e30 else 0,
            bid_price1=pDepthMarketData.BidPrice1 if pDepthMarketData.BidPrice1 < 1e30 else 0,
            bid_volume1=pDepthMarketData.BidVolume1,
            ask_price1=pDepthMarketData.AskPrice1 if pDepthMarketData.AskPrice1 < 1e30 else 0,
            ask_volume1=pDepthMarketData.AskVolume1,
            volume=pDepthMarketData.Volume,
            open_interest=pDepthMarketData.OpenInterest,
            upper_limit=pDepthMarketData.UpperLimitPrice if pDepthMarketData.UpperLimitPrice < 1e30 else 0,
            lower_limit=pDepthMarketData.LowerLimitPrice if pDepthMarketData.LowerLimitPrice < 1e30 else 0,
            timestamp=time.time(),
            trading_day=pDepthMarketData.TradingDay,
            update_time=pDepthMarketData.UpdateTime,
        )

        # 更新缓存
        self.gateway._ticks[tick.instrument_id] = tick

        # 保存历史
        if tick.instrument_id in self.gateway._tick_history:
            self.gateway._tick_history[tick.instrument_id].append(tick)

        # 触发回调
        if tick.instrument_id in self.gateway._tick_callbacks:
            callback = self.gateway._tick_callbacks[tick.instrument_id]
            # 在事件循环中调度回调
            if self.gateway._loop:
                self.gateway._loop.call_soon_threadsafe(
                    lambda t=tick, cb=callback: asyncio.ensure_future(cb(t))
                )


# ==================== 交易 SPI (回调) ====================

class CTPTdSpi(tdapi.CThostFtdcTraderSpi):
    """CTP 交易回调处理"""

    def __init__(self, gateway: 'CTPGateway'):
        super().__init__()
        self.gateway = gateway
        self._login_event = threading.Event()
        self._connected = False
        self._front_id: int = 0
        self._session_id: int = 0

    def OnFrontConnected(self):
        logger.info("CTP TD: 前置连接成功")
        self._connected = True
        # 认证
        req = tdapi.CThostFtdcReqAuthenticateField()
        req.BrokerID = self.gateway.broker_id
        req.UserID = self.gateway.user_id
        req.AppID = self.gateway.app_id
        req.AuthCode = self.gateway.auth_code
        self.gateway._td_api.ReqAuthenticate(req, 0)

    def OnFrontDisconnected(self, nReason: int):
        logger.warning(f"CTP TD: 前置断开, 原因={nReason}")
        self._connected = False

    def OnRspAuthenticate(self, pRspAuthenticateField, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.ErrorID != 0:
            logger.error(f"CTP TD: 认证失败, 错误={pRspInfo.ErrorID}: {pRspInfo.ErrorMsg}")
            return
        logger.info("CTP TD: 认证成功, 开始登录...")
        # 登录
        req = tdapi.CThostFtdcReqUserLoginField()
        req.BrokerID = self.gateway.broker_id
        req.UserID = self.gateway.user_id
        req.Password = self.gateway.password
        self.gateway._td_api.ReqUserLogin(req, 0)

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.ErrorID != 0:
            logger.error(f"CTP TD: 登录失败, 错误={pRspInfo.ErrorID}: {pRspInfo.ErrorMsg}")
            return
        self._front_id = pRspUserLogin.FrontID
        self._session_id = pRspUserLogin.SessionID
        logger.info(f"CTP TD: 登录成功, FrontID={self._front_id}, SessionID={self._session_id}")

        # 确认结算
        req = tdapi.CThostFtdcSettlementInfoConfirmField()
        req.BrokerID = self.gateway.broker_id
        req.InvestorID = self.gateway.user_id
        self.gateway._td_api.ReqSettlementInfoConfirm(req, 0)

    def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
        if pRspInfo and pRspInfo.ErrorID != 0:
            logger.error(f"CTP TD: 确认结算失败, 错误={pRspInfo.ErrorID}: {pRspInfo.ErrorMsg}")
            return
        logger.info("CTP TD: 结算确认完成, 交易就绪")
        self._login_event.set()

    def OnRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast):
        """报单错误回报"""
        if pRspInfo and pRspInfo.ErrorID != 0:
            order_ref = pInputOrder.OrderRef if pInputOrder else "unknown"
            logger.error(f"CTP TD: 报单失败 ref={order_ref}, "
                        f"错误={pRspInfo.ErrorID}: {pRspInfo.ErrorMsg}")
            if order_ref in self.gateway._pending_orders:
                order = self.gateway._pending_orders[order_ref]
                order.status = "error"
                order.error_msg = pRspInfo.ErrorMsg
                # 通知等待的协程
                if order_ref in self.gateway._order_events:
                    self.gateway._loop.call_soon_threadsafe(
                        self.gateway._order_events[order_ref].set
                    )

    def OnRtnOrder(self, pOrder):
        """报单回报"""
        if not pOrder:
            return
        order_ref = pOrder.OrderRef
        status_map = {
            '0': 'pending',      # 全部成交 -> filled 但这里是AllTraded
            '1': 'partial',      # 部分成交还在队列
            '2': 'partial',      # 部分成交不在队列
            '3': 'pending',      # 未成交还在队列
            '4': 'pending',      # 未成交不在队列
            '5': 'cancelled',    # 撤单
            'a': 'error',        # 未知
        }
        # CTP OrderStatus
        ctp_status = pOrder.OrderStatus
        status = status_map.get(ctp_status, 'pending')
        if ctp_status == '0':
            status = 'filled'

        logger.info(f"CTP TD: 报单回报 ref={order_ref}, status={status}, "
                    f"inst={pOrder.InstrumentID}, vol={pOrder.VolumeTotalOriginal}")

        if order_ref in self.gateway._pending_orders:
            order = self.gateway._pending_orders[order_ref]
            order.status = status
            order.order_sys_id = pOrder.OrderSysID
            order.filled_volume = pOrder.VolumeTraded

        if status in ('filled', 'cancelled', 'error'):
            if order_ref in self.gateway._order_events:
                self.gateway._loop.call_soon_threadsafe(
                    self.gateway._order_events[order_ref].set
                )

    def OnRtnTrade(self, pTrade):
        """成交回报"""
        if not pTrade:
            return
        order_ref = pTrade.OrderRef
        logger.info(f"CTP TD: 成交回报 ref={order_ref}, "
                    f"inst={pTrade.InstrumentID}, "
                    f"price={pTrade.Price}, vol={pTrade.Volume}, "
                    f"dir={'买' if pTrade.Direction == '0' else '卖'}")

        if order_ref in self.gateway._pending_orders:
            order = self.gateway._pending_orders[order_ref]
            order.filled_price = pTrade.Price
            order.filled_volume = pTrade.Volume

    def OnRspOrderAction(self, pInputOrderAction, pRspInfo, nRequestID, bIsLast):
        """撤单错误回报"""
        if pRspInfo and pRspInfo.ErrorID != 0:
            logger.error(f"CTP TD: 撤单失败, 错误={pRspInfo.ErrorID}: {pRspInfo.ErrorMsg}")

    def OnRspQryInvestorPosition(self, pInvestorPosition, pRspInfo, nRequestID, bIsLast):
        """查询持仓回报"""
        if pRspInfo and pRspInfo.ErrorID != 0:
            logger.error(f"CTP TD: 查询持仓失败, 错误={pRspInfo.ErrorID}: {pRspInfo.ErrorMsg}")
            self.gateway._query_errors['position'] = f"{pRspInfo.ErrorID}:{pRspInfo.ErrorMsg}"
            if 'position' in self.gateway._query_events:
                self.gateway._loop.call_soon_threadsafe(
                    self.gateway._query_events['position'].set
                )
            return

        if pInvestorPosition and pInvestorPosition.InstrumentID:
            direction = "LONG" if pInvestorPosition.PosiDirection == '2' else "SHORT"
            pos = PositionData(
                instrument_id=pInvestorPosition.InstrumentID,
                direction=direction,
                volume=pInvestorPosition.Position,
                available=pInvestorPosition.Position - pInvestorPosition.ShortFrozen - pInvestorPosition.LongFrozen,
                avg_price=pInvestorPosition.OpenCost / pInvestorPosition.Position if pInvestorPosition.Position > 0 else 0,
                profit=pInvestorPosition.PositionProfit,
                margin=pInvestorPosition.UseMargin,
            )
            key = f"{pos.instrument_id}_{pos.direction}"
            self.gateway._positions[key] = pos

        if bIsLast:
            if 'position' in self.gateway._query_events:
                self.gateway._loop.call_soon_threadsafe(
                    self.gateway._query_events['position'].set
                )

    def OnRspQryTradingAccount(self, pTradingAccount, pRspInfo, nRequestID, bIsLast):
        """查询资金回报"""
        if pRspInfo and pRspInfo.ErrorID != 0:
            logger.error(f"CTP TD: 查询资金失败, 错误={pRspInfo.ErrorID}: {pRspInfo.ErrorMsg}")
            self.gateway._query_errors['account'] = f"{pRspInfo.ErrorID}:{pRspInfo.ErrorMsg}"
            if 'account' in self.gateway._query_events:
                self.gateway._loop.call_soon_threadsafe(
                    self.gateway._query_events['account'].set
                )
            return

        if pTradingAccount:
            self.gateway._account = AccountData(
                balance=pTradingAccount.Balance,
                available=pTradingAccount.Available,
                frozen=pTradingAccount.FrozenCash,
                margin=pTradingAccount.CurrMargin,
                profit=pTradingAccount.PositionProfit,
                commission=pTradingAccount.Commission,
            )

        if bIsLast:
            if 'account' in self.gateway._query_events:
                self.gateway._loop.call_soon_threadsafe(
                    self.gateway._query_events['account'].set
                )


# ==================== CTP 网关主类 ====================

class CTPGateway:
    """
    CTP 网关 - 封装行情和交易接口

    使用方式:
        gateway = CTPGateway(
            broker_id="0187",
            user_id="your_user_id",
            password="your_password",
            md_front="tcp://220.160.125.12:61219",
            td_front="tcp://220.160.125.12:61209",
        )
        await gateway.connect()
        gateway.subscribe("ag2506")
        tick = await gateway.get_tick("ag2506")
    """

    def __init__(
        self,
        broker_id: str,
        user_id: str,
        password: str,
        md_front: str,
        td_front: str,
        app_id: str = "simnow_client_test",
        auth_code: str = "0000000000000000",
    ):
        self.broker_id = broker_id
        self.user_id = user_id
        self.password = password
        self.md_front = md_front
        self.td_front = td_front
        self.app_id = app_id
        self.auth_code = auth_code

        # API 实例
        self._md_api: Optional[mdapi.CThostFtdcMdApi] = None
        self._td_api: Optional[tdapi.CThostFtdcTraderApi] = None
        self._md_spi: Optional[CTPMdSpi] = None
        self._td_spi: Optional[CTPTdSpi] = None

        # 事件循环引用
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # 数据缓存
        self._ticks: Dict[str, TickData] = {}
        self._tick_history: Dict[str, deque] = {}
        self._positions: Dict[str, PositionData] = {}
        self._account: Optional[AccountData] = None
        self._md_trading_day: str = ""

        # 回调
        self._tick_callbacks: Dict[str, Callable] = {}

        # 订单管理
        self._order_ref_counter: int = 0
        self._pending_orders: Dict[str, OrderResult] = {}
        self._order_events: Dict[str, asyncio.Event] = {}
        self._query_events: Dict[str, asyncio.Event] = {}
        self._query_errors: Dict[str, str] = {}

        # 线程引用 (非 daemon, 用于优雅关闭)
        self._md_thread: Optional[threading.Thread] = None
        self._td_thread: Optional[threading.Thread] = None

        # 状态
        self._connected = False
        self._request_id: int = 0

    def _next_request_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def _next_order_ref(self) -> str:
        self._order_ref_counter += 1
        return str(self._order_ref_counter).zfill(12)

    async def connect(self, timeout: float = 30.0):
        """连接行情和交易前置"""
        self._loop = asyncio.get_event_loop()

        # 先在主线程创建 API 和 SPI 对象
        self._md_api = mdapi.CThostFtdcMdApi.CreateFtdcMdApi("md_data")
        self._md_spi = CTPMdSpi(self)
        self._md_api.RegisterSpi(self._md_spi)
        self._md_api.RegisterFront(self.md_front)

        self._td_api = tdapi.CThostFtdcTraderApi.CreateFtdcTraderApi("td_data")
        self._td_spi = CTPTdSpi(self)
        self._td_api.RegisterSpi(self._td_spi)
        self._td_api.SubscribePublicTopic(tdapi.THOST_TERT_QUICK)
        self._td_api.SubscribePrivateTopic(tdapi.THOST_TERT_QUICK)
        self._td_api.RegisterFront(self.td_front)

        # Init + Join 放到后台线程 (非 daemon, 在 close() 中显式 join)
        self._md_thread = threading.Thread(target=self._run_md, name="CTP-MD")
        self._md_thread.start()

        self._td_thread = threading.Thread(target=self._run_td, name="CTP-TD")
        self._td_thread.start()

        # 等待连接和登录完成
        logger.info("等待 CTP 连接...")

        md_ready = await asyncio.get_event_loop().run_in_executor(
            None, self._md_spi._login_event.wait, timeout
        )
        if not md_ready:
            raise ConnectionError("CTP 行情连接超时")

        td_ready = await asyncio.get_event_loop().run_in_executor(
            None, self._td_spi._login_event.wait, timeout
        )
        if not td_ready:
            raise ConnectionError("CTP 交易连接超时")

        self._connected = True
        logger.info("CTP 网关连接成功")

    def _run_md(self):
        """运行行情 API (在后台线程中)"""
        self._md_api.Init()
        self._md_api.Join()

    def _run_td(self):
        """运行交易 API (在后台线程中)"""
        self._td_api.Init()
        self._td_api.Join()

    # ==================== 行情接口 ====================

    def subscribe(self, instrument_id: str, history_size: int = 1000):
        """订阅行情"""
        if not self._md_api:
            raise RuntimeError("行情 API 未连接")
        # Defensive normalization: CTP Python binding requires List[str]
        if isinstance(instrument_id, (list, tuple)):
            if len(instrument_id) != 1:
                raise TypeError(f"instrument_id list/tuple length must be 1, got {len(instrument_id)}")
            instrument_id = instrument_id[0]
        if not isinstance(instrument_id, str):
            instrument_id = str(instrument_id)
        instrument_id = instrument_id.strip()
        if not instrument_id:
            raise ValueError("instrument_id is empty after normalization")

        self._tick_history[instrument_id] = deque(maxlen=history_size)
        logger.info(f"CTP subscribe instrument={instrument_id!r} type={type(instrument_id).__name__}")
        try:
            ret = self._md_api.SubscribeMarketData([instrument_id], 1)
        except TypeError as e:
            # Some CTP Python bindings on Py3 require bytes-like strings.
            logger.warning(f"SubscribeMarketData str call failed, fallback to bytes: {e}")
            ret = self._md_api.SubscribeMarketData([instrument_id.encode("utf-8")], 1)
        if ret != 0:
            logger.error(f"订阅 {instrument_id} 失败, ret={ret}")

    def unsubscribe(self, instrument_id: str):
        """取消订阅"""
        if self._md_api:
            try:
                self._md_api.UnSubscribeMarketData([instrument_id], 1)
            except TypeError:
                self._md_api.UnSubscribeMarketData([instrument_id.encode("utf-8")], 1)
        self._tick_history.pop(instrument_id, None)
        self._tick_callbacks.pop(instrument_id, None)

    def on_tick(self, instrument_id: str, callback: Callable):
        """注册 tick 回调"""
        self._tick_callbacks[instrument_id] = callback

    def get_tick(self, instrument_id: str) -> Optional[TickData]:
        """获取最新 tick"""
        return self._ticks.get(instrument_id)

    def get_tick_history(self, instrument_id: str) -> List[TickData]:
        """获取历史 tick"""
        return list(self._tick_history.get(instrument_id, []))

    @property
    def trading_day(self) -> str:
        return self._md_trading_day

    # ==================== 交易接口 ====================

    async def place_order(
        self,
        instrument_id: str,
        direction: str,          # "BUY" / "SELL"
        offset: str,             # "OPEN" / "CLOSE" / "CLOSE_TODAY"
        price: float,
        volume: int,
        order_type: str = "LIMIT",  # "LIMIT" / "MARKET"
        timeout: float = 10.0,
    ) -> OrderResult:
        """
        下单

        Args:
            instrument_id: 合约代码 (如 ag2506)
            direction: 方向 "BUY"=买, "SELL"=卖
            offset: 开平 "OPEN"=开仓, "CLOSE"=平仓, "CLOSE_TODAY"=平今
            price: 价格 (市价单时传0)
            volume: 手数
            order_type: 订单类型 "LIMIT"=限价, "MARKET"=市价
            timeout: 等待成交超时时间

        Returns:
            OrderResult 报单结果
        """
        if not self._td_api:
            raise RuntimeError("交易 API 未连接")

        order_ref = self._next_order_ref()

        req = tdapi.CThostFtdcInputOrderField()
        req.BrokerID = self.broker_id
        req.InvestorID = self.user_id
        req.InstrumentID = instrument_id
        req.OrderRef = order_ref

        # 方向
        req.Direction = tdapi.THOST_FTDC_D_Buy if direction == "BUY" else tdapi.THOST_FTDC_D_Sell

        # 开平标志
        offset_map = {
            "OPEN": tdapi.THOST_FTDC_OF_Open,
            "CLOSE": tdapi.THOST_FTDC_OF_Close,
            "CLOSE_TODAY": tdapi.THOST_FTDC_OF_CloseToday,
        }
        req.CombOffsetFlag = offset_map.get(offset, tdapi.THOST_FTDC_OF_Open)

        # 投机套保标志
        req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation

        # 价格和数量
        req.LimitPrice = price
        req.VolumeTotalOriginal = volume

        # 订单类型
        if order_type == "MARKET":
            req.OrderPriceType = tdapi.THOST_FTDC_OPT_AnyPrice
            req.LimitPrice = 0
            req.TimeCondition = tdapi.THOST_FTDC_TC_IOC
        else:
            req.OrderPriceType = tdapi.THOST_FTDC_OPT_LimitPrice
            req.TimeCondition = tdapi.THOST_FTDC_TC_GFD  # 当日有效

        req.VolumeCondition = tdapi.THOST_FTDC_VC_AV  # 任何数量
        req.MinVolume = 1
        req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately
        req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose
        req.IsAutoSuspend = 0

        # 创建结果和事件
        result = OrderResult(
            order_ref=order_ref,
            instrument_id=instrument_id,
            direction=direction,
            price=price,
            volume=volume,
        )
        self._pending_orders[order_ref] = result
        self._order_events[order_ref] = asyncio.Event()

        # 发送报单
        ret = self._td_api.ReqOrderInsert(req, self._next_request_id())
        if ret != 0:
            result.status = "error"
            result.error_msg = f"ReqOrderInsert 返回 {ret}"
            logger.error(f"报单发送失败: {result.error_msg}")
            return result

        logger.info(f"报单已发送: ref={order_ref}, {instrument_id} "
                    f"{'买' if direction == 'BUY' else '卖'} {offset} "
                    f"price={price} vol={volume}")

        # 等待成交或超时
        try:
            await asyncio.wait_for(
                self._order_events[order_ref].wait(),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.warning(f"报单超时: ref={order_ref}")
            result.status = "timeout"

        # 清理
        self._order_events.pop(order_ref, None)

        return result

    async def cancel_order(self, instrument_id: str, order_ref: str,
                           order_sys_id: str = "", exchange_id: str = ""):
        """撤单"""
        if not self._td_api:
            raise RuntimeError("交易 API 未连接")

        req = tdapi.CThostFtdcInputOrderActionField()
        req.BrokerID = self.broker_id
        req.InvestorID = self.user_id
        req.InstrumentID = instrument_id
        req.ActionFlag = tdapi.THOST_FTDC_AF_Delete

        if order_sys_id and exchange_id:
            req.OrderSysID = order_sys_id
            req.ExchangeID = exchange_id
        else:
            req.OrderRef = order_ref
            req.FrontID = self._td_spi._front_id
            req.SessionID = self._td_spi._session_id

        ret = self._td_api.ReqOrderAction(req, self._next_request_id())
        if ret != 0:
            logger.error(f"撤单发送失败, ret={ret}")
        else:
            logger.info(f"撤单已发送: {instrument_id} ref={order_ref}")

    async def market_order(
        self,
        instrument_id: str,
        direction: str,
        offset: str,
        volume: int,
    ) -> OrderResult:
        """市价单 (使用对手价模拟)"""
        tick = self.get_tick(instrument_id)
        if not tick:
            raise ValueError(f"无行情数据: {instrument_id}")

        # 使用对手价 + 滑点
        if direction == "BUY":
            price = tick.ask_price1 + tick.ask_price1 * 0.001  # 0.1% 滑点
            # 不超过涨停价
            if tick.upper_limit > 0:
                price = min(price, tick.upper_limit)
        else:
            price = tick.bid_price1 - tick.bid_price1 * 0.001
            # 不低于跌停价
            if tick.lower_limit > 0:
                price = max(price, tick.lower_limit)

        return await self.place_order(
            instrument_id=instrument_id,
            direction=direction,
            offset=offset,
            price=round(price, 1),  # 白银精度1位小数
            volume=volume,
            order_type="LIMIT",
        )

    # ==================== 查询接口 ====================

    async def query_positions(self, instrument_id: str = "") -> List[PositionData]:
        """查询持仓"""
        if not self._td_api:
            raise RuntimeError("交易 API 未连接")

        self._positions.clear()
        self._query_errors.pop('position', None)
        self._query_events['position'] = asyncio.Event()

        req = tdapi.CThostFtdcQryInvestorPositionField()
        req.BrokerID = self.broker_id
        req.InvestorID = self.user_id
        if instrument_id:
            req.InstrumentID = instrument_id

        # CTP 查询限流: 每秒1次
        await asyncio.sleep(1)
        self._td_api.ReqQryInvestorPosition(req, self._next_request_id())

        try:
            await asyncio.wait_for(
                self._query_events['position'].wait(),
                timeout=10.0
            )
        except asyncio.TimeoutError:
            logger.warning("查询持仓超时")
            self._query_events.pop('position', None)
            return []

        if self._query_errors.get('position'):
            logger.warning(f"查询持仓返回错误: {self._query_errors['position']}")
            self._query_events.pop('position', None)
            return []

        self._query_events.pop('position', None)
        return list(self._positions.values())

    async def query_account(self) -> Optional[AccountData]:
        """查询资金"""
        if not self._td_api:
            raise RuntimeError("交易 API 未连接")

        self._account = None
        self._query_errors.pop('account', None)
        self._query_events['account'] = asyncio.Event()

        req = tdapi.CThostFtdcQryTradingAccountField()
        req.BrokerID = self.broker_id
        req.InvestorID = self.user_id

        await asyncio.sleep(1)
        self._td_api.ReqQryTradingAccount(req, self._next_request_id())

        try:
            await asyncio.wait_for(
                self._query_events['account'].wait(),
                timeout=10.0
            )
        except asyncio.TimeoutError:
            logger.warning("查询资金超时")
            self._query_events.pop('account', None)
            return None

        if self._query_errors.get('account'):
            logger.warning(f"查询资金返回错误: {self._query_errors['account']}")
            self._query_events.pop('account', None)
            return None

        self._query_events.pop('account', None)
        return self._account

    # ==================== 便捷方法 ====================

    def get_mid_price(self, instrument_id: str) -> float:
        """获取中间价"""
        tick = self._ticks.get(instrument_id)
        if not tick or tick.bid_price1 <= 0 or tick.ask_price1 <= 0:
            return 0
        return (tick.bid_price1 + tick.ask_price1) / 2

    def get_last_price(self, instrument_id: str) -> float:
        """获取最新价"""
        tick = self._ticks.get(instrument_id)
        return tick.last_price if tick else 0

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def close(self):
        """关闭连接, 释放 API 并等待线程退出"""
        logger.info("关闭 CTP 网关...")
        self._connected = False

        # Release 会触发 Join() 返回, 从而结束线程
        if self._md_api:
            self._md_api.Release()
            self._md_api = None
        if self._td_api:
            self._td_api.Release()
            self._td_api = None

        # 等待线程退出 (最多 5 秒)
        for t in (self._md_thread, self._td_thread):
            if t and t.is_alive():
                t.join(timeout=5)
                if t.is_alive():
                    logger.warning(f"CTP 线程 {t.name} 未能在 5s 内退出")

        self._md_thread = None
        self._td_thread = None
        logger.info("CTP 网关已关闭")
