import os
import time
import math
import logging
import asyncio
import pandas as pd
import requests
import threading
import uvicorn
from typing import List
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from ta.trend import MACD, SMAIndicator
from ta.momentum import StochRSIIndicator
from binance.client import Client
from binance import ThreadedWebsocketManager
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor

# ================= SQLALCHEMY ORM & SQLITE =================
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, event
from sqlalchemy.orm import sessionmaker, declarative_base

load_dotenv()

# Cấu hình Engine SQLite
DATABASE_URL = "sqlite:///bot_database.db"
# check_same_thread=False là BẮT BUỘC khi dùng SQLite với Multi-threading
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

# Sự kiện bật chế độ WAL để tăng tốc độ I/O và cho phép đọc/ghi đồng thời
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 1. Bảng Trạng thái Bot (Chỉ lưu 1 Row)
class BotStateDB(Base):
    __tablename__ = "bot_state"
    id = Column(Integer, primary_key=True, index=True)
    total_trades = Column(Integer, default=0)
    winning_trades = Column(Integer, default=0)
    losing_trades = Column(Integer, default=0)
    break_even_trades = Column(Integer, default=0)
    gross_profit = Column(Float, default=0.0)
    gross_loss = Column(Float, default=0.0)
    total_pnl = Column(Float, default=0.0)
    peak_pnl = Column(Float, default=0.0)
    max_drawdown = Column(Float, default=0.0)
    current_consecutive_losses = Column(Integer, default=0)
    max_consecutive_losses = Column(Integer, default=0)
    entry_order_id = Column(String, nullable=True)
    last_trade_side = Column(String, default="NONE")
    trade_start_time = Column(Integer, default=0)
    last_closed_time = Column(Integer, default=0)
    last_tp_closed_time = Column(Integer, default=0)
    current_position = Column(String, default="NONE")
    position_amt = Column(Float, default=0.0)
    entry_price = Column(Float, default=0.0)
    kill_switch = Column(Boolean, default=False)
    current_leverage = Column(Integer, default=0)

# 2. Bảng Lịch sử Giao Dịch
class TradeHistoryDB(Base):
    __tablename__ = "trades"
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    side = Column(String)
    entry_order_id = Column(String, nullable=True)
    exit_order_id = Column(String, nullable=True)
    realized_pnl = Column(Float, default=0.0)
    fees = Column(Float, default=0.0)
    funding = Column(Float, default=0.0)
    net_pnl = Column(Float, default=0.0)
    initial_risk = Column(Float, default=0.0)
    r_multiple = Column(Float, default=0.0)
    close_time = Column(Integer)
    reason = Column(String)

# Bỏ bảng SystemLogDB vì không lưu log vào SQLite nữa

# Tạo bảng nếu chưa có
Base.metadata.create_all(bind=engine)


# ================= TẢI BIẾN MÔI TRƯỜNG & CONFIG =================
API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

KILL_SWITCH_PASSWORD = os.getenv('KILL_SWITCH_PASSWORD', 'admin123')

SYMBOL = 'ETHUSDT'
MARGIN_TYPE = 'ISOLATED'

MAX_LEVERAGE = 50          
MIN_PROFIT_PCT = 0.0015
STOP_LOSS_PCT = 0.05       

LOG_FILE = "bot.log"

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

client = Client(API_KEY, API_SECRET, testnet=True)

# ================= BIẾN TRẠNG THÁI IN-MEMORY =================
state_lock = threading.Lock()
data_lock = threading.Lock()
processing_lock = threading.Lock()  

exchange_info_data = {}
critical_api_failure_counter = 0  

# Thêm biến logs vào RAM để frontend có thể lấy hiển thị
bot_state = {
    "logs": [], # Lưu tạm log trên RAM
    "total_trades": 0, "winning_trades": 0, "losing_trades": 0, "break_even_trades": 0,
    "gross_profit": 0.0, "gross_loss": 0.0, "total_pnl": 0.0, "peak_pnl": 0.0,
    "max_drawdown": 0.0, "current_consecutive_losses": 0, "max_consecutive_losses": 0,
    "entry_order_id": None, "last_trade_side": "NONE", "trade_start_time": 0,
    "last_closed_time": 0, "last_tp_closed_time": 0, "current_position": "NONE",
    "position_amt": 0.0, "entry_price": 0.0, "kill_switch": False, "current_leverage": 0,
    
    # Các biến không lưu DB, chỉ tồn tại trong RAM
    "is_running": False, "latest_indicators": {}, "available_balance": 0.0,
    "trade_start_balance": 0.0, "current_trade_risk": 0.0, "is_order_in_flight": False   
}

live_data = {
    '1h': pd.DataFrame(), '4h': pd.DataFrame(), '1d': pd.DataFrame() 
}

bot_thread = None
ws_manager_binance = None
last_ws_update_time = time.time()
current_bnb_price = 600.0  
telegram_executor = ThreadPoolExecutor(max_workers=3)

# ================= WEBSOCKET MANAGER FRONTEND =================
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        custom_log(f"🌐 Web UI đã kết nối (Tổng: {len(self.active_connections)})")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            custom_log(f"🌐 Web UI ngắt kết nối (Tổng: {len(self.active_connections)})")

    async def broadcast(self, message: dict):
        for connection in self.active_connections.copy():
            try:
                await connection.send_json(message)
            except:
                self.disconnect(connection)

ui_ws_manager = ConnectionManager()

# ================= ĐỒNG BỘ DB & HELPER =================
def sync_memory_to_db():
    with SessionLocal() as db:
        try:
            state_record = db.query(BotStateDB).filter(BotStateDB.id == 1).first()
            if not state_record:
                state_record = BotStateDB(id=1)
                db.add(state_record)
            
            with state_lock:
                state_record.total_trades = bot_state["total_trades"]
                state_record.winning_trades = bot_state["winning_trades"]
                state_record.losing_trades = bot_state["losing_trades"]
                state_record.break_even_trades = bot_state["break_even_trades"]
                state_record.gross_profit = bot_state["gross_profit"]
                state_record.gross_loss = bot_state["gross_loss"]
                state_record.total_pnl = bot_state["total_pnl"]
                state_record.peak_pnl = bot_state["peak_pnl"]
                state_record.max_drawdown = bot_state["max_drawdown"]
                state_record.current_consecutive_losses = bot_state["current_consecutive_losses"]
                state_record.max_consecutive_losses = bot_state["max_consecutive_losses"]
                state_record.entry_order_id = bot_state["entry_order_id"]
                state_record.last_trade_side = bot_state["last_trade_side"]
                state_record.trade_start_time = bot_state["trade_start_time"]
                state_record.last_closed_time = bot_state["last_closed_time"]
                state_record.last_tp_closed_time = bot_state["last_tp_closed_time"]
                state_record.current_position = bot_state["current_position"]
                state_record.position_amt = bot_state["position_amt"]
                state_record.entry_price = bot_state["entry_price"]
                state_record.kill_switch = bot_state["kill_switch"]
                state_record.current_leverage = bot_state["current_leverage"]
                
            db.commit()
        except Exception as e:
            logging.error(f"Lỗi đồng bộ DB: {e}")
            db.rollback()

def load_state_from_db():
    global bot_state
    with SessionLocal() as db:
        state_record = db.query(BotStateDB).filter(BotStateDB.id == 1).first()
        if state_record:
            with state_lock:
                bot_state["total_trades"] = state_record.total_trades
                bot_state["winning_trades"] = state_record.winning_trades
                bot_state["losing_trades"] = state_record.losing_trades
                bot_state["break_even_trades"] = state_record.break_even_trades
                bot_state["gross_profit"] = state_record.gross_profit
                bot_state["gross_loss"] = state_record.gross_loss
                bot_state["total_pnl"] = state_record.total_pnl
                bot_state["peak_pnl"] = state_record.peak_pnl
                bot_state["max_drawdown"] = state_record.max_drawdown
                bot_state["current_consecutive_losses"] = state_record.current_consecutive_losses
                bot_state["max_consecutive_losses"] = state_record.max_consecutive_losses
                bot_state["entry_order_id"] = state_record.entry_order_id
                bot_state["last_trade_side"] = state_record.last_trade_side
                bot_state["trade_start_time"] = state_record.trade_start_time
                bot_state["last_closed_time"] = state_record.last_closed_time
                bot_state["last_tp_closed_time"] = state_record.last_tp_closed_time
                bot_state["current_position"] = state_record.current_position
                bot_state["position_amt"] = state_record.position_amt
                bot_state["entry_price"] = state_record.entry_price
                bot_state["current_leverage"] = state_record.current_leverage
                
                bot_state["kill_switch"] = False 
                bot_state["is_order_in_flight"] = False
            custom_log("✅ Đã khôi phục trạng thái từ SQLite Database!")
        else:
            custom_log("🆕 Database mới, tạo bản ghi trạng thái đầu tiên...")
            sync_memory_to_db()

def restore_logs_from_file(limit=50):
    try:
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'r', encoding='utf-8') as f:
                # Đọc tất cả các dòng và lấy các dòng cuối cùng
                lines = f.readlines()[-limit:]
                with state_lock:
                    bot_state["logs"] = [line.strip() for line in lines]
    except Exception as e:
        logging.error(f"⚠️ Không thể khôi phục log từ file: {e}")

def custom_log(message):
    logging.info(message)
    timestamp = time.strftime('%H:%M:%S')
    log_entry = f"[{timestamp}] {message}"
    
    # Chỉ lưu vào RAM để frontend query, không lưu SQLite nữa
    with state_lock:
        bot_state["logs"].append(log_entry)
        if len(bot_state["logs"]) > 300:
            bot_state["logs"].pop(0)

def send_telegram_notification(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    def _send():
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'HTML'}
        try: requests.post(url, data=payload, timeout=5)
        except Exception as e: custom_log(f"⚠️ Lỗi gửi Telegram: {e}")
    telegram_executor.submit(_send)

# ================= API RETRY & EXCHANGE INFO =================
def safe_api_call(func, retries=3, delay=1.5, is_critical=False, **kwargs):
    global critical_api_failure_counter
    for attempt in range(retries):
        try:
            res = func(**kwargs)
            if is_critical: critical_api_failure_counter = 0  
            return res
        except Exception as e:
            err_str = str(e)
            if "-4046" in err_str or "-2011" in err_str: return None  
            if "-2022" in err_str:
                custom_log(f"⚠️ Lỗi: Đã tồn tại lệnh ReduceOnly. (Mã -2022)")
                return None
            
            if is_critical:
                critical_api_failure_counter += 1
                custom_log(f"🚨 Lỗi API Quan trọng (Lần {attempt+1}/{retries}): {e}")
                if critical_api_failure_counter >= 20:
                    custom_log("🚨 KÍCH HOẠT CIRCUIT BREAKER! Bật Kill Switch!")
                    with state_lock: bot_state["kill_switch"] = True
                    sync_memory_to_db()
                    critical_api_failure_counter = 0  
            else:
                custom_log(f"⚠️ Lỗi API Data (Lần {attempt+1}/{retries}): {e}")
                
            if attempt < retries - 1: time.sleep(delay)
            else: return None 

def fetch_exchange_info():
    global exchange_info_data
    try:
        info = safe_api_call(client.futures_exchange_info, retries=5)
        if info and 'symbols' in info:
            temp_data = {}
            for s in info['symbols']:
                min_notional = 5.0
                min_qty = 0.001
                for f in s['filters']:
                    if f['filterType'] == 'MIN_NOTIONAL': min_notional = float(f.get('notional', 5.0))
                    elif f['filterType'] == 'LOT_SIZE': min_qty = float(f.get('minQty', 0.001))
                temp_data[s['symbol']] = {
                    'qty_precision': int(s['quantityPrecision']),
                    'price_precision': int(s['pricePrecision']),
                    'min_notional': min_notional,
                    'min_qty': min_qty
                }
            with data_lock: exchange_info_data = temp_data
    except Exception as e:
        custom_log(f"❌ Lỗi exchangeInfo: {e}")

# ================= QUẢN LÝ VỊ THẾ =================
def update_position_state(amt, ep, lev=None):
    changed = False
    with state_lock:
        if bot_state["position_amt"] != float(amt) or bot_state["entry_price"] != float(ep):
            changed = True
        
        bot_state["position_amt"] = float(amt)
        bot_state["entry_price"] = float(ep)
        if lev is not None:
            if bot_state["current_leverage"] != int(lev): changed = True
            bot_state["current_leverage"] = int(lev)
        
        if bot_state["position_amt"] > 0: bot_state["current_position"] = "LONG"
        elif bot_state["position_amt"] < 0: bot_state["current_position"] = "SHORT"
        else: bot_state["current_position"] = "NONE"
        
    if changed:
        sync_memory_to_db()

def sync_position_from_api():
    try:
        pos_info = safe_api_call(client.futures_position_information, symbol=SYMBOL)
        if pos_info:
            p = next((x for x in pos_info if x['symbol'] == SYMBOL), None)
            if p is not None:
                update_position_state(p.get('positionAmt', 0.0), p.get('entryPrice', 0.0), p.get('leverage'))
    except: pass

# ================= BACKGROUND LOOPS =================
def update_bnb_price_loop():
    global current_bnb_price
    while True:
        try:
            ticker = safe_api_call(client.futures_symbol_ticker, symbol="BNBUSDT")
            if ticker and 'price' in ticker:
                with state_lock: current_bnb_price = float(ticker['price'])
        except: pass
        time.sleep(60)

def update_account_balance_loop():
    while True:
        try:
            acc = safe_api_call(client.futures_account, retries=1)
            if acc:
                with state_lock: bot_state["available_balance"] = float(acc['availableBalance'])
        except: pass
        time.sleep(10)

# ================= KẾT TOÁN LỢI NHUẬN (FINALIZE TRADE) =================
def calculate_exact_trade_pnl(symbol, entry_order_id, exit_order_id, start_time, open_time):
    realized_usdt, fee_usdt, funding_usdt = 0.0, 0.0, 0.0
    entry_trades = safe_api_call(client.futures_account_trades, symbol=symbol, orderId=entry_order_id) or []
    exit_trades = safe_api_call(client.futures_account_trades, symbol=symbol, orderId=exit_order_id) or [] if exit_order_id else []
    
    if not exit_trades:
        recent_all = safe_api_call(client.futures_account_trades, symbol=symbol, startTime=start_time, limit=50) or []
        exit_trades = [t for t in recent_all if float(t.get('realizedPnl', 0)) != 0]
        
    for t in entry_trades + exit_trades:
        realized_usdt += float(t.get('realizedPnl', 0))
        comm = abs(float(t.get('commission', 0)))
        asset = t.get('commissionAsset', 'USDT')
        if asset == 'USDT': fee_usdt += comm
        elif asset == 'BNB':
            with state_lock: fee_usdt += (comm * current_bnb_price)

    funds = safe_api_call(client.futures_income_history, symbol=symbol, incomeType="FUNDING_FEE", startTime=start_time, endTime=open_time + 5000, limit=1000) or []
    for f in funds: funding_usdt += float(f.get('income', 0))

    return realized_usdt - fee_usdt + funding_usdt, fee_usdt, funding_usdt, realized_usdt

def finalize_trade_task(symbol, entry_order_id, exit_order_id, start_time, side, reason):
    custom_log(f"⏳ Đang cập nhật sổ cái DB cho {side}...")
    close_time = int(time.time() * 1000)
    
    if entry_order_id:
        time.sleep(1.5)
        net_pnl, fee_usdt, funding_fee, realized_pnl = calculate_exact_trade_pnl(symbol, entry_order_id, exit_order_id, start_time, close_time)
    else:
        net_pnl, fee_usdt, funding_fee, realized_pnl = 0.0, 0.0, 0.0, 0.0

    with state_lock:
        bot_state["total_trades"] += 1
        bot_state["total_pnl"] += net_pnl
        bot_state["last_closed_time"] = close_time
        
        initial_risk = bot_state.get("current_trade_risk", 1.0)
        r_multiple = net_pnl / initial_risk if initial_risk > 0 else 0.0
        bot_state["current_trade_risk"] = 0.0  

        if net_pnl > 0: 
            bot_state["winning_trades"] += 1
            bot_state["gross_profit"] += net_pnl
            bot_state["current_consecutive_losses"] = 0
        elif net_pnl < 0: 
            bot_state["losing_trades"] += 1
            bot_state["gross_loss"] += abs(net_pnl)
            bot_state["current_consecutive_losses"] += 1
            if bot_state["current_consecutive_losses"] > bot_state["max_consecutive_losses"]:
                bot_state["max_consecutive_losses"] = bot_state["current_consecutive_losses"]
        else:
            bot_state["break_even_trades"] += 1

        bot_state["peak_pnl"] = max(bot_state.get("peak_pnl", 0.0), bot_state["total_pnl"])
        bot_state["max_drawdown"] = max(bot_state.get("max_drawdown", 0.0), bot_state["peak_pnl"] - bot_state["total_pnl"])

    with SessionLocal() as db:
        try:
            new_trade = TradeHistoryDB(
                symbol=symbol, side=side, entry_order_id=entry_order_id, exit_order_id=exit_order_id,
                realized_pnl=round(realized_pnl, 4), fees=round(fee_usdt, 4), funding=round(funding_fee, 4),
                net_pnl=round(net_pnl, 4), initial_risk=round(initial_risk, 4),
                r_multiple=round(r_multiple, 4), close_time=close_time, reason=reason
            )
            db.add(new_trade)
            db.commit()
        except Exception as e:
            db.rollback()
            logging.error(f"Lỗi ghi TradeHistory DB: {e}")
            
    sync_memory_to_db()

    custom_log(f"🏁 ĐÃ ĐÓNG {side} | PnL ròng: {net_pnl:.4f} | Hiệu suất: {r_multiple:.2f}R")
    
    # Cập nhật thông báo đóng lệnh có đủ Phí, Funding theo yêu cầu
    send_telegram_notification(
        f"🔴 <b>ĐÓNG {side}</b>\n"
        f"Lý do: {reason}\n\n"
        f"📊 <b>Tổng kết:</b>\n"
        f"PnL ròng: {net_pnl:.4f} USDT\n"
        f"Hiệu suất lệnh: {r_multiple:.2f}R\n"
        f"Phí giao dịch: -{fee_usdt:.4f} USDT\n"
        f"Funding Fee: {funding_fee:.4f} USDT"
    )

# ================= REAL-TIME DATA STREAM =================
def init_historical_data():
    intervals = {'1h': Client.KLINE_INTERVAL_1HOUR, '4h': Client.KLINE_INTERVAL_4HOUR, '1d': Client.KLINE_INTERVAL_1DAY}
    with data_lock:
        for tf, interval in intervals.items():
            klines = safe_api_call(client.futures_klines, symbol=SYMBOL, interval=interval, limit=1500)
            if not klines: continue
            df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
            for col in ['open', 'high', 'low', 'close', 'volume']: df[col] = df[col].astype(float)
            live_data[tf] = df

def process_ws_kline(msg, tf):
    global last_ws_update_time
    if 'k' not in msg: return
    last_ws_update_time = time.time()
    k = msg['k']
    new_row = {'timestamp': k['t'], 'open': float(k['o']), 'high': float(k['h']), 'low': float(k['l']), 'close': float(k['c']), 'volume': float(k['v']), 'close_time': k['T']}
    
    with data_lock:
        df = live_data[tf]
        if df.empty: live_data[tf] = pd.DataFrame([new_row]); return
        if df['timestamp'].iat[-1] == new_row['timestamp']:
            df.iat[-1, 1], df.iat[-1, 2], df.iat[-1, 3], df.iat[-1, 4], df.iat[-1, 5] = new_row['open'], new_row['high'], new_row['low'], new_row['close'], new_row['volume']
        else:
            df.loc[len(df)] = new_row
            if len(df) > 1500: live_data[tf] = df.iloc[1:].reset_index(drop=True)

    with state_lock:
        if bot_state["kill_switch"]: return

    if processing_lock.acquire(blocking=False):
        try: check_and_execute_trading_logic()
        finally: processing_lock.release()

def process_user_data(msg):
    if msg.get('e') == 'ACCOUNT_UPDATE':
        for p in msg.get('a', {}).get('P', []):
            if p['s'] == SYMBOL: update_position_state(p['pa'], p['ep'])

def start_websockets():
    global ws_manager_binance, last_ws_update_time
    if ws_manager_binance:
        try: ws_manager_binance.stop(); ws_manager_binance.join(timeout=5.0) 
        except: pass

    ws_manager_binance = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET, testnet=True)
    ws_manager_binance.start()
    ws_manager_binance.start_kline_futures_socket(callback=lambda m: process_ws_kline(m, '1h'), symbol=SYMBOL, interval=Client.KLINE_INTERVAL_1HOUR)
    ws_manager_binance.start_kline_futures_socket(callback=lambda m: process_ws_kline(m, '4h'), symbol=SYMBOL, interval=Client.KLINE_INTERVAL_4HOUR)
    ws_manager_binance.start_kline_futures_socket(callback=lambda m: process_ws_kline(m, '1d'), symbol=SYMBOL, interval=Client.KLINE_INTERVAL_1DAY)
    ws_manager_binance.start_futures_user_socket(callback=process_user_data)
    last_ws_update_time = time.time()
    custom_log("✅ Đã kết nối Binance WebSocket!")

def ws_watchdog_loop():
    global last_ws_update_time
    while True:
        time.sleep(20)
        if time.time() - last_ws_update_time > 60:
            custom_log("⚠️ CẢNH BÁO: Mất kết nối Binance. Đang Resync...")
            try: init_historical_data(); start_websockets()
            except: pass
            last_ws_update_time = time.time()

# ================= CHỈ BÁO & GIAO DỊCH =================
def get_realtime_indicators():
    inds = {}
    with data_lock:
        for tf in ['1h', '4h', '1d']:
            df = live_data[tf].copy()
            if df.empty or len(df) < 25: return None
            macd = MACD(close=df['close'])
            stoch_rsi = StochRSIIndicator(close=df['close'])
            
            ma23 = None
            if tf == '1h':
                ma23 = SMAIndicator(close=df['close'], window=23).sma_indicator().iloc[-1]

            inds[tf] = {
                'macd': macd.macd_diff().iloc[-1], 'macd_prev': macd.macd_diff().iloc[-2], 'macd_prev2': macd.macd_diff().iloc[-3],
                'k': stoch_rsi.stochrsi_k().iloc[-1], 'd': stoch_rsi.stochrsi_d().iloc[-1],
                'k_prev': stoch_rsi.stochrsi_k().iloc[-2], 'd_prev': stoch_rsi.stochrsi_d().iloc[-2],
                'prev_high': df['high'].iloc[-2], 'prev_low': df['low'].iloc[-2],   
            }
            if tf == '1h': inds[tf]['ma23'] = ma23
    return inds

def get_dynamic_kelly_risk(window_size=30) -> float:
    with SessionLocal() as db:
        recent_trades = db.query(TradeHistoryDB).order_by(TradeHistoryDB.id.desc()).limit(window_size).all()
        
    if len(recent_trades) < window_size: return 0.02

    r_multiples = [t.r_multiple for t in recent_trades]
    wins = [r for r in r_multiples if r > 0]
    losses = [r for r in r_multiples if r < 0]
    
    if not wins: return 0.01 
    if not losses: return 0.05
        
    total_effective = len(wins) + len(losses)
    if total_effective == 0: return 0.02

    p = len(wins) / total_effective                
    avg_win_r = sum(wins) / len(wins)    
    avg_loss_r = abs(sum(losses) / len(losses)) 
    
    if avg_loss_r == 0: return 0.01

    R_ratio = avg_win_r / avg_loss_r     
    if R_ratio <= 0: return 0.01  
        
    f_fraction = p - ((1 - p) / R_ratio)     
    if f_fraction <= 0: return 0.01
        
    true_f_star = f_fraction / avg_loss_r
    return max(0.01, min(true_f_star * 0.5, 0.05))

# ================= ĐẶT LỆNH =================
def execute_trade(side, price, reason, ind_data, is_closing=False, amt_to_close=0, sl_price=None):
    try:
        now_ms = int(time.time() * 1000)
        with data_lock:
            prec = exchange_info_data.get(SYMBOL, {'qty_precision': 3, 'price_precision': 2, 'min_notional': 5.0, 'min_qty': 0.001})
        
        qty_prec = prec['qty_precision']
        min_notional = prec['min_notional']
        min_qty = prec['min_qty']
        
        if is_closing:
            qty = round(abs(amt_to_close), qty_prec)
            qty_str = f"{qty:.{qty_prec}f}"
            order_side = 'SELL' if side == 'LONG' else 'BUY'

            order = safe_api_call(client.futures_create_order, is_critical=True, symbol=SYMBOL, side=order_side, type='MARKET', quantity=qty_str, reduceOnly=True)
            
            if "CHỐT LỜI" in reason.upper() or "TRAILING" in reason.upper():
                with data_lock:
                    curr_candle = float(live_data['1h']['timestamp'].iloc[-1]) if not live_data['1h'].empty else 0
                with state_lock: bot_state["last_tp_closed_time"] = curr_candle
                sync_memory_to_db()
            
        else:
            with state_lock:
                last_closed = bot_state.get("last_closed_time", 0)
                equity = bot_state.get("available_balance", 0.0)
                curr_lev = bot_state.get("current_leverage", 0)
                
            if equity <= 1: 
                with state_lock: bot_state["is_order_in_flight"] = False
                return
                
            risk_pct = get_dynamic_kelly_risk(30)
            sl_dist = max(abs(price - sl_price) if sl_price else price * STOP_LOSS_PCT, price * 0.001)
            eff_sl_dist = sl_dist + price * 0.001 

            raw_qty = (equity * risk_pct) / eff_sl_dist
            req_lev = (raw_qty * price) / equity if equity > 0 else 1.0
            target_lev = max(1, min(math.ceil(req_lev * 1.2), MAX_LEVERAGE)) 

            safe_qty = ((equity * target_lev) / price) * (0.85 if target_lev == MAX_LEVERAGE else 0.95)
            final_qty = min(raw_qty, safe_qty)
            
            with state_lock: bot_state["current_trade_risk"] = final_qty * eff_sl_dist

            mul = 10 ** qty_prec
            qty_val = max(math.floor(final_qty * mul) / mul, min_qty) 
            
            if (qty_val * price) < min_notional:
                with state_lock: bot_state["is_order_in_flight"] = False
                return

            qty_str = f"{qty_val:.{qty_prec}f}"
            order_side = 'BUY' if side == 'LONG' else 'SELL'
            
            if target_lev != curr_lev:
                if safe_api_call(client.futures_change_leverage, is_critical=True, symbol=SYMBOL, leverage=target_lev):
                    with state_lock: bot_state["current_leverage"] = target_lev

            order = safe_api_call(client.futures_create_order, is_critical=True, symbol=SYMBOL, side=order_side, type='MARKET', quantity=qty_str)
            if order:
                with state_lock:
                    bot_state["trade_start_time"] = max(now_ms - 5000, last_closed + 1)
                    bot_state["entry_order_id"] = order['orderId']
                    bot_state["trade_start_balance"] = equity 
                    bot_state["last_trade_side"] = side
                sync_memory_to_db()

        if not order or 'orderId' not in order: 
            with state_lock: bot_state["is_order_in_flight"] = False
            return

        expected_pos_none = is_closing
        for _ in range(15):
            sync_position_from_api()
            with state_lock: curr_pos = bot_state["current_position"]
            if (expected_pos_none and curr_pos == "NONE") or (not expected_pos_none and curr_pos != "NONE"): break
            time.sleep(0.5)

        if is_closing:
            with state_lock:
                start_time = bot_state.get("trade_start_time", 0)
                entry_order_id = bot_state.get("entry_order_id")
                bot_state["entry_order_id"] = None
                bot_state["trade_start_time"] = 0
            sync_memory_to_db()
            threading.Thread(target=finalize_trade_task, args=(SYMBOL, entry_order_id, order['orderId'], start_time, side, reason), daemon=True).start()
        else:
            custom_log(f"✅ MỞ {side} | KL: {qty_str} | Đòn bẩy: {target_lev}x")
            # Cập nhật thông báo mở lệnh theo đúng cấu trúc yêu cầu
            send_telegram_notification(
                f"🟢 <b>MỞ {side}</b>\n"
                f"Giá: {price}\n"
                f"Khối lượng: {qty_str}\n"
                f"Đòn bẩy: {target_lev}x\n"
                f"Risk đi lệnh: {risk_pct*100:.2f}%\n"
                f"Lý do: {reason}\n"
                f"🛡️ Đã bật Soft SL ngầm!"
            )

    except Exception as e:
        custom_log(f"⚠️ Lỗi lệnh: {e}")
    finally:
        with state_lock: bot_state["is_order_in_flight"] = False

# ================= ĐỘNG CƠ KIỂM TRA =================
def check_and_execute_trading_logic():
    with state_lock:
        if bot_state.get("is_order_in_flight", False): return
        amt, current_pos = bot_state["position_amt"], bot_state["current_position"]
        entry_id = bot_state.get("entry_order_id")
        
    if entry_id and current_pos == "NONE" and amt == 0:
        with state_lock:
            start_time, last_side = bot_state.get("trade_start_time", 0), bot_state.get("last_trade_side", "NONE")
            bot_state["entry_order_id"] = None
            bot_state["trade_start_time"] = 0
        sync_memory_to_db()
        threading.Thread(target=finalize_trade_task, args=(SYMBOL, entry_id, None, start_time, last_side, "ĐÓNG LỆNH BÊN NGOÀI / THANH LÝ"), daemon=True).start()
        return 

    inds = get_realtime_indicators()
    if not inds: return

    with state_lock:
        bot_state["latest_indicators"] = {
            tf: {
                "macd_t0": float(d['macd']),
                "macd_t1": float(d['macd_prev']),
                "stoch_k_t0": round(float(d['k'])*100, 2),
                "stoch_d_t0": round(float(d['d'])*100, 2),
                "ma23": float(d.get('ma23', 0))
            } for tf, d in inds.items()
        }

    with data_lock:
        cp = float(live_data['1h']['close'].iloc[-1])
        ch = float(live_data['1h']['high'].iloc[-1])
        cl = float(live_data['1h']['low'].iloc[-1])
        ph = float(live_data['1h']['high'].iloc[-2])
        pl = float(live_data['1h']['low'].iloc[-2])
        c_1h_ts = float(live_data['1h']['timestamp'].iloc[-1]) if not live_data['1h'].empty else 0
        
    with state_lock:
        ep = bot_state["entry_price"]
        last_tp_ts = bot_state.get("last_tp_closed_time", 0)

    # 1. Đóng lệnh
    if current_pos != "NONE":
        is_long = current_pos == "LONG"
        
        if (is_long and cp <= ep * (1 - STOP_LOSS_PCT)) or (not is_long and cp >= ep * (1 + STOP_LOSS_PCT)):
            with state_lock: bot_state["is_order_in_flight"] = True
            threading.Thread(target=execute_trade, kwargs={"side": current_pos, "price": cp, "reason": f"CẮT LỖ KHẨN CẤP FALLBACK {STOP_LOSS_PCT*100}%", "ind_data": inds, "is_closing": True, "amt_to_close": amt}, daemon=True).start()
            return

        b_1d = (inds['1d']['macd'] > inds['1d']['macd_prev']) and (inds['1d']['macd'] >= 0 or inds['1d']['k'] > inds['1d']['d'])
        b_4h = (inds['4h']['macd'] > inds['4h']['macd_prev']) and (inds['4h']['macd'] >= 0 or inds['4h']['k'] > inds['4h']['d'])
        s_1d, s_4h = not b_1d, not b_4h

        if is_long and cp <= ep and cp < pl:
            with state_lock: bot_state["is_order_in_flight"] = True
            threading.Thread(target=execute_trade, kwargs={"side": "LONG", "price": cp, "reason": f"SOFT STOP LOSS (Thủng đáy t-1: {pl})", "ind_data": inds, "is_closing": True, "amt_to_close": amt}, daemon=True).start()
            return
        elif not is_long and cp >= ep and cp > ph:
            with state_lock: bot_state["is_order_in_flight"] = True
            threading.Thread(target=execute_trade, kwargs={"side": "SHORT", "price": cp, "reason": f"SOFT STOP LOSS (Vượt đỉnh t-1: {ph})", "ind_data": inds, "is_closing": True, "amt_to_close": amt}, daemon=True).start()
            return

        if is_long:
            is_profit = cp > ep * (1 + MIN_PROFIT_PCT)
            if not (b_1d and b_4h) and is_profit:
                with state_lock: bot_state["is_order_in_flight"] = True
                threading.Thread(target=execute_trade, kwargs={"side": "LONG", "price": cp, "reason": "CHỐT LỜI SỚM (Gãy 1D/4H)", "ind_data": inds, "is_closing": True, "amt_to_close": amt}, daemon=True).start()
            elif b_1d and b_4h and pl > ep * (1 + MIN_PROFIT_PCT) and cp < pl:
                with state_lock: bot_state["is_order_in_flight"] = True
                threading.Thread(target=execute_trade, kwargs={"side": "LONG", "price": cp, "reason": f"TRAILING STOP (Thủng {pl})", "ind_data": inds, "is_closing": True, "amt_to_close": amt}, daemon=True).start()
        else:
            is_profit = cp < ep * (1 - MIN_PROFIT_PCT)
            if not (s_1d and s_4h) and is_profit:
                with state_lock: bot_state["is_order_in_flight"] = True
                threading.Thread(target=execute_trade, kwargs={"side": "SHORT", "price": cp, "reason": "CHỐT LỜI SỚM (Gãy 1D/4H)", "ind_data": inds, "is_closing": True, "amt_to_close": amt}, daemon=True).start()
            elif s_1d and s_4h and ph < ep * (1 - MIN_PROFIT_PCT) and cp > ph:
                with state_lock: bot_state["is_order_in_flight"] = True
                threading.Thread(target=execute_trade, kwargs={"side": "SHORT", "price": cp, "reason": f"TRAILING STOP (Vượt {ph})", "ind_data": inds, "is_closing": True, "amt_to_close": amt}, daemon=True).start()

    # 2. Mở lệnh
    elif current_pos == "NONE":
        if last_tp_ts > 0 and c_1h_ts == last_tp_ts: return

        c_buy_1d = (inds['1d']['macd'] > inds['1d']['macd_prev']) and (inds['1d']['macd'] >= 0 or inds['1d']['k'] > inds['1d']['d'])
        c_buy_4h = (inds['4h']['macd'] > inds['4h']['macd_prev']) and (inds['4h']['macd'] >= 0 or inds['4h']['k'] > inds['4h']['d'])
        c_buy_1h = inds['1h']['macd'] > inds['1h']['macd_prev']

        c_sell_1d = (inds['1d']['macd'] < inds['1d']['macd_prev']) and (inds['1d']['macd'] <= 0 or inds['1d']['k'] < inds['1d']['d'])
        c_sell_4h = (inds['4h']['macd'] < inds['4h']['macd_prev']) and (inds['4h']['macd'] <= 0 or inds['4h']['k'] < inds['4h']['d'])
        c_sell_1h = inds['1h']['macd'] < inds['1h']['macd_prev']

        valid_long_wick = cl >= pl
        valid_short_wick = ch <= ph

        if c_buy_1d and c_buy_4h and c_buy_1h and valid_long_wick and cp > inds['1h']['ma23']:
            with state_lock: bot_state["is_order_in_flight"] = True
            threading.Thread(target=execute_trade, kwargs={"side": "LONG", "price": cp, "reason": "ĐỒNG THUẬN TĂNG & PRICE TREN MA23", "ind_data": inds, "sl_price": pl}, daemon=True).start()
        elif c_sell_1d and c_sell_4h and c_sell_1h and valid_short_wick and cp < inds['1h']['ma23']:
            with state_lock: bot_state["is_order_in_flight"] = True
            threading.Thread(target=execute_trade, kwargs={"side": "SHORT", "price": cp, "reason": "ĐỒNG THUẬN GIẢM & PRICE DUOI MA23", "ind_data": inds, "sl_price": ph}, daemon=True).start()

def run_bot():
    try: client.futures_change_margin_type(symbol=SYMBOL, marginType=MARGIN_TYPE)
    except: pass
    safe_api_call(client.futures_cancel_all_open_orders, is_critical=False, symbol=SYMBOL)
    
    acc = safe_api_call(client.futures_account)
    if acc:
        with state_lock: bot_state["available_balance"] = float(acc['availableBalance'])

    sync_position_from_api() 
    init_historical_data()
    start_websockets()
    custom_log("🚀 BẮT ĐẦU BOT SCANNER SQLITE (LOG TỐI ƯU RAM)")
    send_telegram_notification(f"🚀 <b>BOT ĐÃ KHỞI ĐỘNG THÀNH CÔNG</b>\nĐang theo dõi: {SYMBOL}\nKiến trúc: SQLite + RAM (Tối ưu)")

    while True:
        try:
            time.sleep(30)
            sync_position_from_api()  
        except: time.sleep(5)

def thread_monitor():
    global bot_thread
    while True:
        if bot_thread is None or not bot_thread.is_alive():
            bot_thread = threading.Thread(target=run_bot, daemon=True)
            bot_thread.start()
        time.sleep(10)

# ================= WS DATA TRUY VẤN TỪ SQLITE & RAM =================
def get_stats_data():
    c_1h = float(live_data['1h']['timestamp'].iloc[-1]) if (not live_data['1h'].empty) else 0
    
    with SessionLocal() as db:
        recent_trades = db.query(TradeHistoryDB).order_by(TradeHistoryDB.id.desc()).limit(50).all()
        # Đã bổ sung realized_pnl, fees, funding vào đây theo đúng yêu cầu
        trades_out = [{
            "symbol": t.symbol, 
            "side": t.side, 
            "close_time": t.close_time,
            "realized_pnl": t.realized_pnl,
            "fees": t.fees,
            "funding": t.funding,
            "net_pnl": t.net_pnl, 
            "r_multiple": t.r_multiple, 
            "reason": t.reason
        } for t in reversed(recent_trades)]

    with state_lock:
        tot = bot_state["total_trades"]
        wr = (bot_state["winning_trades"]/tot*100) if tot > 0 else 0
        pf = (bot_state["gross_profit"]/bot_state["gross_loss"]) if bot_state["gross_loss"] > 0 else (99 if bot_state["gross_profit"] > 0 else 0)
        avg_win = (bot_state["gross_profit"] / bot_state["winning_trades"]) if bot_state["winning_trades"] > 0 else 0
        avg_loss = (bot_state["gross_loss"] / bot_state["losing_trades"]) if bot_state["losing_trades"] > 0 else 0
        
        last_tp = bot_state.get("last_tp_closed_time", 0)
        is_wait = (last_tp > 0 and c_1h > 0 and last_tp == c_1h)
        
        # Lấy tối đa 50 log gần nhất từ RAM để truyền xuống UI
        logs_out = list(bot_state.get("logs", []))[-50:]

        return {
            "logs": logs_out,
            "trade_history": trades_out,
            "total_trades": tot,
            "winning_trades": bot_state["winning_trades"],
            "losing_trades": bot_state["losing_trades"],
            "break_even_trades": bot_state["break_even_trades"],
            "total_pnl": round(bot_state["total_pnl"], 4),
            "max_drawdown": round(bot_state["max_drawdown"], 4),
            "max_consecutive_losses": bot_state["max_consecutive_losses"],
            "avg_win": round(avg_win, 4), "avg_loss": round(avg_loss, 4), 
            "win_rate": round(wr, 1), "profit_factor": round(pf, 2),
            "position": bot_state["current_position"],
            "kill_switch_active": bot_state["kill_switch"],
            "indicators": bot_state.get("latest_indicators", {}),
            "is_waiting_next_candle": is_wait,
            "next_candle_time": c_1h + 3600000 if is_wait else 0 
        }

async def ws_broadcast_loop():
    while True:
        try:
            if len(ui_ws_manager.active_connections) > 0:
                await ui_ws_manager.broadcast(get_stats_data())
        except: pass
        await asyncio.sleep(1) 

# ================= FASTAPI =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    fetch_exchange_info()
    load_state_from_db()
    restore_logs_from_file()
    
    threading.Thread(target=thread_monitor, daemon=True).start()
    threading.Thread(target=ws_watchdog_loop, daemon=True).start()
    threading.Thread(target=update_bnb_price_loop, daemon=True).start()
    threading.Thread(target=update_account_balance_loop, daemon=True).start()
    asyncio.create_task(ws_broadcast_loop())
    
    yield
    try:
        with state_lock: bot_state["is_running"] = False
        if ws_manager_binance: ws_manager_binance.stop()
        telegram_executor.shutdown(wait=False)
    except: pass

app = FastAPI(lifespan=lifespan)
os.makedirs("templates", exist_ok=True)
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", context={})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ui_ws_manager.connect(websocket)
    try:
        while True: await websocket.receive_text()
    except: ui_ws_manager.disconnect(websocket)

class KillSwitchRequest(BaseModel): password: str

@app.post("/api/kill-switch")
async def toggle_kill_switch(req: KillSwitchRequest):
    if req.password != KILL_SWITCH_PASSWORD: return {"success": False, "message": "Sai mật khẩu!"}

    with state_lock:
        bot_state["kill_switch"] = not bot_state["kill_switch"]
        status, amt, pos = bot_state["kill_switch"], bot_state["position_amt"], bot_state["current_position"]
    
    sync_memory_to_db()
    
    if status and amt != 0:
        with data_lock: cp = float(live_data['1h']['close'].iloc[-1]) if not live_data['1h'].empty else 0.0
        with state_lock: bot_state["is_order_in_flight"] = True
        threading.Thread(target=execute_trade, args=(pos, cp, "KILL SWITCH", {}, True, amt)).start()
            
    custom_log(f"🚨 KILL SWITCH: {'BẬT' if status else 'TẮT'}")
    return {"success": True, "kill_switch_active": status}

if __name__ == "__main__":              
    uvicorn.run(app, host="0.0.0.0", port=5005, log_level="warning")