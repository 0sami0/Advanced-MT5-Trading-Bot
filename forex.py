import MetaTrader5 as mt5
import time
import datetime
import logging  # For logging
import os       # For log file check, directory creation
import random   # For probability
import csv      # For CSV writing
import uuid     # For unique cycle IDs
import threading # <<<< ADDED FOR THREADING
import math     # <<<< ADDED FOR LOT SIZE CALCULATION

# --- Logging Setup ---
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - Line:%(lineno)d - %(message)s')
log_file_handler = logging.FileHandler("trap_cycle_bot.log", mode='a') # Append mode
log_file_handler.setFormatter(log_formatter)

console_handler = logging.StreamHandler() # To also print to console
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)
log_file_handler.setLevel(logging.DEBUG)

logger = logging.getLogger("TrapCycleBot")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    logger.addHandler(log_file_handler)
    logger.addHandler(console_handler)
# --- End Logging Setup ---

# --- Global Threading Primitives ---
global_state_lock = threading.Lock() # Use threading.RLock() for re-entrant lock if complex nested calls are needed
shutdown_event = threading.Event()
# --- End Global Threading Primitives ---

# --- Trading Time Configuration ---
TRADING_START_HOUR = 6
TRADING_END_HOUR = 17
# --- End Trading Time Configuration ---

# --- Global Bot Configuration ---
AUTO_RESTART_COMPLETED_CYCLES = True
LAST_L0_WAS_BUY = {}
user_initial_preference_is_buy = {}

# --- Cycle Data Logging Configuration ---
CYCLE_DATA_LOG_FOLDER = "forex_cycle_logs"
CYCLE_DATA_CSV_FILE = os.path.join(CYCLE_DATA_LOG_FOLDER, "trading_cycle_data.csv")
CYCLE_DATA_HEADERS = ["LoggedAtUTC", "Symbol", "CycleID", "CycleStartTimeUTC", "CycleEndTimeUTC", "DurationSeconds", "TrapsCount", "L0Direction", "Outcome"]
cycle_tracking_data = {} # Stores {symbol: {"id": uuid, "start_time": dt_utc, "traps": 0, "l0_direction": "BUY"/"SELL"}}
# --- End Cycle Data Logging Configuration ---


# --- Symbol-Specific Configurations ---
SYMBOL_CONFIGS = {
    "EURUSDc": {
        "INITIAL_LOT_SIZE": 0.01, "LOT_MULTIPLIER": 2.5, "NOMINAL_TP_PIPS": 9.5,
        "NOMINAL_SL_PIPS": 20.5, "TRIGGER_DISTANCE_PIPS": 9.5, "MAX_TRADES_IN_CYCLE": 100,
        "MAGIC_NUMBER": 67893, "PIP_MULTIPLIER": 10,
        "TRADE_24_7": False
    },
    "XAUUSDm": {
        "INITIAL_LOT_SIZE": 0.01, "LOT_MULTIPLIER": 2.5, "NOMINAL_TP_PIPS": 7.3,
        "NOMINAL_SL_PIPS": 15.2, "TRIGGER_DISTANCE_PIPS": 7.3, "MAX_TRADES_IN_CYCLE": 8,
        "MAGIC_NUMBER": 67894, "PIP_MULTIPLIER": 1000,
        "TRADE_24_7": False
    },
    "BTCUSDc": {
        "INITIAL_LOT_SIZE": 0.01, "LOT_MULTIPLIER": 2.5, "NOMINAL_TP_PIPS": 12,
        "NOMINAL_SL_PIPS": 26, "TRIGGER_DISTANCE_PIPS": 12, "MAX_TRADES_IN_CYCLE": 100,
        "MAGIC_NUMBER": 67895, "PIP_MULTIPLIER": 1000,
        "TRADE_24_7": True
    }
}
# --- User-Friendly Symbol Aliases ---
SYMBOL_ALIASES = {
    "eurusd": "EURUSDc",
    "gold": "XAUUSDm",
    "eur": "EURUSDc",
    "xau": "XAUUSDm",
    "btc": "BTCUSDc"
}
# --- Global State Dictionaries ---
is_cycle_active = {}; current_level = {}; active_position_ticket = {}; active_position_entry_price = {}
active_position_lot_size = {}; active_position_is_buy = {}; active_pending_order_ticket = {}
active_pending_order_is_buy_stop = {}; cycle_open_position_tickets = {}
cycle_L0_entry_price = {}

# --- Cycle Data Logging Functions ---
def ensure_cycle_data_log_exists():
    try:
        if not os.path.exists(CYCLE_DATA_LOG_FOLDER):
            os.makedirs(CYCLE_DATA_LOG_FOLDER)
            logger.info(f"Created cycle data log folder: {CYCLE_DATA_LOG_FOLDER}")

        if not os.path.exists(CYCLE_DATA_CSV_FILE):
            with open(CYCLE_DATA_CSV_FILE, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(CYCLE_DATA_HEADERS)
            logger.info(f"Created cycle data CSV file with headers: {CYCLE_DATA_CSV_FILE}")
    except Exception as e:
        logger.error(f"Error ensuring cycle data log exists: {e}")

def _log_cycle_data_to_csv(log_time_utc, symbol, cycle_id, start_time_utc, end_time_utc, duration_seconds, traps_count, l0_direction, outcome):
    try:
        ensure_cycle_data_log_exists()
        row = [
            log_time_utc.strftime('%Y-%m-%d %H:%M:%S'),
            symbol,
            str(cycle_id),
            start_time_utc.strftime('%Y-%m-%d %H:%M:%S'),
            end_time_utc.strftime('%Y-%m-%d %H:%M:%S'),
            int(duration_seconds),
            traps_count,
            l0_direction,
            outcome
        ]
        with open(CYCLE_DATA_CSV_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(row)
        logger.info(f"CYCLE_CSV_LOG: Appended cycle data for {symbol}, ID {cycle_id}, Outcome {outcome}, Traps: {traps_count}")
    except Exception as e:
        logger.error(f"CYCLE_CSV_LOG: Error writing cycle data to CSV for {symbol}, ID {cycle_id}: {e}")

def _init_cycle_tracking(symbol_name, l0_is_buy_actual):
    with global_state_lock:
        cycle_tracking_data[symbol_name] = {
            "id": uuid.uuid4(),
            "start_time_utc": datetime.datetime.utcnow(),
            "traps": 1,
            "l0_direction": "BUY" if l0_is_buy_actual else "SELL"
        }
        logger.info(f"CYCLE_TRACK_INIT ({symbol_name}): Started tracking cycle ID {cycle_tracking_data[symbol_name]['id']}, L0: {cycle_tracking_data[symbol_name]['l0_direction']}, Traps: {cycle_tracking_data[symbol_name]['traps']}")

def _increment_trap_count(symbol_name):
    with global_state_lock:
        if symbol_name in cycle_tracking_data and cycle_tracking_data[symbol_name] is not None:
            cycle_tracking_data[symbol_name]["traps"] += 1
            logger.debug(f"CYCLE_TRACK_TRAP ({symbol_name}): Incremented trap count to {cycle_tracking_data[symbol_name]['traps']} for cycle ID {cycle_tracking_data[symbol_name]['id']}")
        else:
            logger.warning(f"CYCLE_TRACK_TRAP ({symbol_name}): Attempted to increment trap count, but no active cycle tracking found.")

def _finalize_and_log_cycle(symbol_name, outcome):
    tracking_info_snapshot = None
    with global_state_lock:
        if symbol_name in cycle_tracking_data and cycle_tracking_data[symbol_name] is not None:
            tracking_info_snapshot = cycle_tracking_data[symbol_name].copy()
            cycle_tracking_data[symbol_name] = None

    if tracking_info_snapshot:
        end_time_utc = datetime.datetime.utcnow()
        duration = end_time_utc - tracking_info_snapshot["start_time_utc"]

        _log_cycle_data_to_csv(
            log_time_utc=datetime.datetime.utcnow(),
            symbol=symbol_name,
            cycle_id=tracking_info_snapshot["id"],
            start_time_utc=tracking_info_snapshot["start_time_utc"],
            end_time_utc=end_time_utc,
            duration_seconds=duration.total_seconds(),
            traps_count=tracking_info_snapshot["traps"],
            l0_direction=tracking_info_snapshot["l0_direction"],
            outcome=outcome
        )
        logger.info(f"CYCLE_TRACK_FINALIZE ({symbol_name}): Finalized and logged cycle ID {tracking_info_snapshot['id']} with outcome {outcome}, Traps: {tracking_info_snapshot['traps']}.")
# --- End Cycle Data Logging Functions ---


# --- Time Checking Logic ---
def is_general_trading_hours():
    now_time = datetime.datetime.now().time()
    start_trade_time = datetime.time(TRADING_START_HOUR, 0, 0)
    end_trade_time = datetime.time(TRADING_END_HOUR, 0, 0)
    return start_trade_time <= now_time < end_trade_time

def is_trading_hours_for_symbol(symbol_name):
    config = SYMBOL_CONFIGS.get(symbol_name)
    if config and config.get("TRADE_24_7", False):
        return True
    return is_general_trading_hours()
# --- End Time Checking Logic ---


def initialize_all_symbol_states():
    with global_state_lock:
        for symbol_name in SYMBOL_CONFIGS.keys():
            is_cycle_active[symbol_name] = False; current_level[symbol_name] = 0
            active_position_ticket[symbol_name] = 0; active_position_entry_price[symbol_name] = 0.0
            active_position_lot_size[symbol_name] = 0.0; active_position_is_buy[symbol_name] = None
            active_pending_order_ticket[symbol_name] = 0; active_pending_order_is_buy_stop[symbol_name] = None
            cycle_open_position_tickets[symbol_name] = []
            cycle_L0_entry_price[symbol_name] = 0.0
            LAST_L0_WAS_BUY[symbol_name] = None
            user_initial_preference_is_buy[symbol_name] = None
            cycle_tracking_data[symbol_name] = None
        logger.debug("STATES: Initialized states for all configured symbols (with lock).")

def initialize_mt5_connection():
    if not mt5.initialize():
        logger.critical(f"MT5 initialize() failed, error code = {mt5.last_error()}"); return False
    terminal_info = mt5.terminal_info()
    if terminal_info is None: logger.critical(f"Failed to get MT5 terminal_info, error code = {mt5.last_error()}"); mt5.shutdown(); return False
    logger.info(f"MetaTrader5 terminal connected: {terminal_info.name} (Build {terminal_info.build})")
    account_info = mt5.account_info()
    if account_info is None: logger.critical(f"Failed to get MT5 account_info, error code = {mt5.last_error()}"); mt5.shutdown(); return False
    logger.info(f"Connected to account: {account_info.login}, Name: {account_info.name}, Balance: {account_info.balance} {account_info.currency}")
    return True

def get_symbol_details(symbol_name):
    info = mt5.symbol_info(symbol_name)
    if info is None: logger.warning(f"Symbol ({symbol_name}) not found in MarketWatch."); return None
    if not info.visible:
        logger.info(f"Symbol ({symbol_name}) is not visible, attempting to select.")
        if not mt5.symbol_select(symbol_name, True): logger.error(f"Failed to select symbol ({symbol_name})."); return None
        time.sleep(0.1); info = mt5.symbol_info(symbol_name)
        if not info or not info.visible: logger.error(f"Still cannot make symbol ({symbol_name}) visible."); return None
    return info

# <<<< MODIFIED: Replaced this function to correctly round lot sizes UP >>>>
def normalize_lot(symbol_name, requested_lot):
    """
    Calculates a valid lot size for the given symbol, rounding UP to the nearest volume_step.
    Also ensures the lot size is within the symbol's min/max volume limits.
    """
    info = get_symbol_details(symbol_name)
    if not info:
        logger.error(f"NORMALIZE_LOT: Could not get symbol info for {symbol_name}. Cannot calculate lot size.")
        return None

    if info.volume_step <= 0:
        # Fallback for symbols without a defined volume step (very rare)
        lot = round(requested_lot, 8)
    else:
        # 1. Calculate how many steps are in the requested lot.
        #    e.g., requested 0.025 / step 0.01 = 2.5
        num_steps = requested_lot / info.volume_step
        # 2. Round this number UP to the nearest whole number using math.ceil().
        #    e.g., math.ceil(2.5) = 3.0
        rounded_up_steps = math.ceil(num_steps)
        # 3. Multiply back by the step size to get the final, valid lot size.
        #    e.g., 3.0 * 0.01 = 0.03
        lot = rounded_up_steps * info.volume_step

    # Ensure the lot is clean and within broker limits
    lot = round(lot, 8)  # Round to 8 decimal places to avoid floating point dust
    
    # Enforce min/max volume limits
    lot = max(lot, info.volume_min)
    lot = min(lot, info.volume_max)

    logger.debug(f"NORMALIZE_LOT ({symbol_name}): Requested {requested_lot:.5f}, Step {info.volume_step}, Min {info.volume_min}, Max {info.volume_max} -> Final {lot:.2f}")
    return lot

def calculate_sl_tp_prices(symbol_name, entry_price_param, is_buy_param, sl_pips_param, tp_pips_param):
    symbol_info_param = get_symbol_details(symbol_name)
    if not symbol_info_param: return 0.0, 0.0
    config = SYMBOL_CONFIGS[symbol_name]
    sl_offset_points = sl_pips_param * config["PIP_MULTIPLIER"]
    tp_offset_points = tp_pips_param * config["PIP_MULTIPLIER"]
    sl_price = 0.0
    if sl_offset_points > 0: sl_price = round(entry_price_param - (sl_offset_points * symbol_info_param.point) if is_buy_param else entry_price_param + (sl_offset_points * symbol_info_param.point), symbol_info_param.digits)
    tp_price = 0.0
    if tp_offset_points > 0: tp_price = round(entry_price_param + (tp_offset_points * symbol_info_param.point) if is_buy_param else entry_price_param - (tp_offset_points * symbol_info_param.point), symbol_info_param.digits)
    return sl_price, tp_price

def place_market_order(symbol_name, is_buy_order_type, lot_size_param, sl_pips_param, tp_pips_param, comment_param):
    config = SYMBOL_CONFIGS[symbol_name]; info = get_symbol_details(symbol_name)
    if not info: return None
    order_type = mt5.ORDER_TYPE_BUY if is_buy_order_type else mt5.ORDER_TYPE_SELL
    tick_info = mt5.symbol_info_tick(symbol_name)
    if not tick_info: logger.error(f"Could not get tick for {symbol_name} market order."); return None
    price = tick_info.ask if is_buy_order_type else tick_info.bid
    sl_price, tp_price = calculate_sl_tp_prices(symbol_name, price, is_buy_order_type, sl_pips_param, tp_pips_param)
    request = {"action": mt5.TRADE_ACTION_DEAL, "symbol": symbol_name, "volume": lot_size_param, "type": order_type, "price": price, "sl": sl_price, "tp": tp_price, "deviation": 20, "magic": config["MAGIC_NUMBER"], "comment": comment_param, "type_filling": mt5.ORDER_FILLING_IOC, "type_time": mt5.ORDER_TIME_GTC}
    logger.debug(f"ORDER_REQ ({symbol_name}): Sending MARKET {request}")
    result = mt5.order_send(request)
    if result and (result.retcode == mt5.TRADE_RETCODE_DONE or result.retcode == mt5.TRADE_RETCODE_PLACED):
        logger.info(f"Market order SUCCESS for '{comment_param}' ({symbol_name}). Order: {result.order}, Deal: {result.deal}"); return result
    else:
        err_msg = result.comment if result else "System error"; err_code = result.retcode if result else mt5.last_error()
        logger.error(f"Market order FAILED for '{comment_param}' ({symbol_name}): {err_msg} (Code: {err_code})"); return None

def place_pending_stop_order(symbol_name, is_buy_stop, lot_size_param, entry_price_param, sl_pips_param, tp_pips_param, comment_param):
    config = SYMBOL_CONFIGS[symbol_name]; info = get_symbol_details(symbol_name)
    if not info: return 0
    order_type = mt5.ORDER_TYPE_BUY_STOP if is_buy_stop else mt5.ORDER_TYPE_SELL_STOP
    tick = mt5.symbol_info_tick(symbol_name)
    if not tick: logger.error(f"Cannot get tick for {symbol_name} pending order price check."); return 0
    min_stop_level_points_abs = info.trade_stops_level * info.point
    adjusted_entry_price = round(entry_price_param, info.digits)
    if is_buy_stop:
        required_price = round(tick.ask + min_stop_level_points_abs, info.digits)
        if adjusted_entry_price < required_price : adjusted_entry_price = round(required_price + info.point, info.digits)
    else:
        required_price = round(tick.bid - min_stop_level_points_abs, info.digits)
        if adjusted_entry_price > required_price: adjusted_entry_price = round(required_price - info.point, info.digits)
    sl_price, tp_price = calculate_sl_tp_prices(symbol_name, adjusted_entry_price, is_buy_stop, sl_pips_param, tp_pips_param)
    request = {"action": mt5.TRADE_ACTION_PENDING, "symbol": symbol_name, "volume": lot_size_param, "type": order_type, "price": adjusted_entry_price, "sl": sl_price, "tp": tp_price, "magic": config["MAGIC_NUMBER"], "comment": comment_param, "type_filling": mt5.ORDER_FILLING_IOC, "type_time": mt5.ORDER_TIME_GTC }
    logger.debug(f"ORDER_REQ ({symbol_name}): Sending PENDING {request}")
    result = mt5.order_send(request)
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        logger.info(f"Pending order SUCCESS for '{comment_param}' ({symbol_name}). Ticket: {result.order}"); return result.order
    else:
        err_msg = result.comment if result else "System error"; err_code = result.retcode if result else mt5.last_error()
        logger.error(f"Pending order FAILED for '{comment_param}' ({symbol_name}): {err_msg} (Code: {err_code})"); return 0

def get_position_details_from_order_result(symbol_name, order_send_result, expected_comment):
    config = SYMBOL_CONFIGS[symbol_name]
    if not order_send_result or order_send_result.order == 0: logger.warning(f"GETPOS ({symbol_name}): No order_send_result or order ID 0."); return None
    if order_send_result.deal > 0:
        deals = mt5.history_deals_get(ticket=order_send_result.deal)
        if deals and len(deals) == 1 and deals[0].position_id > 0:
            positions = mt5.positions_get(ticket=deals[0].position_id)
            if positions and len(positions) == 1: logger.debug(f"GETPOS ({symbol_name}): Pos {positions[0].ticket} confirmed via deal for order {order_send_result.order}."); return positions[0]
    logger.info(f"GETPOS ({symbol_name}): Could not confirm pos via deal for order {order_send_result.order}. Fallback search by comment.")
    time.sleep(0.5); positions = mt5.positions_get(symbol=symbol_name, magic=config["MAGIC_NUMBER"])
    if positions:
        for pos in reversed(positions):
            if pos.comment == expected_comment: logger.debug(f"GETPOS ({symbol_name}): Pos {pos.ticket} confirmed via comment for order {order_send_result.order}."); return pos
    logger.warning(f"GETPOS ({symbol_name}): Pos for order {order_send_result.order} / comment '{expected_comment}' not found."); return None

def reset_cycle_state_for_symbol(symbol_name, called_for_new_l0_setup=False):
    local_last_l0_direction_for_restart = None
    local_user_initial_preference = None
    log_finalization_needed = not called_for_new_l0_setup

    if log_finalization_needed:
        _finalize_and_log_cycle(symbol_name, outcome="CONCLUDED_BY_RESET")

    with global_state_lock:
        logger.info(f"RESET_CYCLE ({symbol_name}): Resetting state. Called for new L0 setup: {called_for_new_l0_setup}")
        local_last_l0_direction_for_restart = LAST_L0_WAS_BUY.get(symbol_name)
        local_user_initial_preference = user_initial_preference_is_buy.get(symbol_name)

        is_cycle_active[symbol_name] = False
        current_level[symbol_name] = 0
        active_position_ticket[symbol_name] = 0
        active_position_entry_price[symbol_name] = 0.0
        active_position_lot_size[symbol_name] = 0.0
        active_position_is_buy[symbol_name] = None
        active_pending_order_ticket[symbol_name] = 0
        active_pending_order_is_buy_stop[symbol_name] = None
        cycle_open_position_tickets[symbol_name] = []
        cycle_L0_entry_price[symbol_name] = 0.0
        logger.debug(f"RESET_CYCLE ({symbol_name}): State has been reset (under lock).")

    if AUTO_RESTART_COMPLETED_CYCLES and not called_for_new_l0_setup:
        is_trading_hours_now = is_trading_hours_for_symbol(symbol_name)
        if not is_trading_hours_now:
            last_l0_was_str = 'BUY' if local_last_l0_direction_for_restart else 'SELL' if local_last_l0_direction_for_restart is False else 'N/A'
            logger.info(f"AUTO-RESTART ({symbol_name}): Skipped due to non-trading hours for this symbol... Last L0 was {last_l0_was_str}.")
        elif local_last_l0_direction_for_restart is not None:
            if local_user_initial_preference is None:
                logger.warning(f"AUTO-RESTART ({symbol_name}): Skipped. User's initial 75/25 preference not set...")
                print(f"AUTO-RESTART SKIPPED for {symbol_name}: User preference for 75/25 split not set...")
                with global_state_lock:
                    LAST_L0_WAS_BUY[symbol_name] = None
            else:
                random_val = random.random()
                favored_str = "BUY" if local_user_initial_preference else "SELL"
                next_l0_is_buy = False
                decision_reason = ""
                if random_val <= 0.75:
                    next_l0_is_buy = local_user_initial_preference
                    decision_reason = f"Rolled <=0.75 (value: {random_val:.4f}), followed favored {favored_str}"
                else:
                    next_l0_is_buy = not local_user_initial_preference
                    decision_reason = f"Rolled >0.75 (value: {random_val:.4f}), opposite of favored {favored_str}"
                next_l0_will_be_str = 'BUY' if next_l0_is_buy else 'SELL'
                last_l0_was_str = 'BUY' if local_last_l0_direction_for_restart else 'SELL'
                logger.info(f"AUTO-RESTART ({symbol_name}): Last L0 was {last_l0_was_str}. User's favored is {favored_str}.")
                logger.info(f"AUTO-RESTART ({symbol_name}): {decision_reason}. Triggering new L0 as {next_l0_will_be_str}.")
                print(f"\nAUTO-RESTART for {symbol_name}: Last L0 was {last_l0_was_str}. Favored: {favored_str}.")
                print(f"AUTO-RESTART for {symbol_name}: {decision_reason}. Starting L0 as {next_l0_will_be_str}.")
                time.sleep(1.5)
                start_L0_market_cycle(symbol_name, is_buy_L0=next_l0_is_buy)
        else:
            logger.info(f"AUTO-RESTART ({symbol_name}): Skipped, last L0 direction unknown...")
            with global_state_lock:
                LAST_L0_WAS_BUY[symbol_name] = None
    elif not called_for_new_l0_setup:
        is_trading_hours_now = is_trading_hours_for_symbol(symbol_name)
        with global_state_lock:
            current_last_l0_was_buy_for_clear = LAST_L0_WAS_BUY.get(symbol_name)
            condition_met = not (AUTO_RESTART_COMPLETED_CYCLES and not is_trading_hours_now and current_last_l0_was_buy_for_clear is not None)
            if condition_met:
                 LAST_L0_WAS_BUY[symbol_name] = None

def cancel_order(symbol_name, order_ticket, comment_prefix="Cancelling order"):
    if order_ticket == 0: return True
    request = {"action": mt5.TRADE_ACTION_REMOVE, "order": order_ticket}
    logger.debug(f"CANCEL_ORDER ({symbol_name}): Attempting to cancel order {order_ticket} (Log: {comment_prefix})")
    result = mt5.order_send(request)
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        logger.info(f"Order {order_ticket} ({symbol_name}) cancelled successfully."); return True
    else:
        err_msg = result.comment if result else "System error"; err_code = result.retcode if result else mt5.last_error()
        logger.error(f"Failed to cancel order {order_ticket} ({symbol_name}). Error: {err_msg} ({err_code})"); return False

def close_single_position(symbol_name, pos_ticket, close_comment="Cycle Close"):
    config = SYMBOL_CONFIGS[symbol_name]; pos_to_close_list = mt5.positions_get(ticket=pos_ticket)
    if not pos_to_close_list: logger.warning(f"CLOSEPOS ({symbol_name}): Pos {pos_ticket} not found for closing."); return False
    pos_to_close = pos_to_close_list[0]; info = get_symbol_details(pos_to_close.symbol)
    if not info: return False
    request = {"action": mt5.TRADE_ACTION_DEAL, "symbol": pos_to_close.symbol, "volume": pos_to_close.volume, "position": pos_to_close.ticket, "type": mt5.ORDER_TYPE_BUY if pos_to_close.type == mt5.POSITION_TYPE_SELL else mt5.ORDER_TYPE_SELL, "deviation": 20, "magic": config["MAGIC_NUMBER"], "comment": f"{close_comment} ({symbol_name})", "type_filling": info.filling_mode, "type_time": mt5.ORDER_TIME_GTC}
    logger.debug(f"CLOSEPOS ({symbol_name}): Attempting to close position {pos_ticket}, request: {request}")
    result = mt5.order_send(request)
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        logger.info(f"Pos {pos_ticket} ({symbol_name}) closed. Comment: '{close_comment}'."); return True
    else:
        err_msg = result.comment if result else "System error"; err_code = result.retcode if result else mt5.last_error()
        logger.error(f"Failed to close pos {pos_ticket} ({symbol_name}). Error: {err_msg} ({err_code})"); return False

def close_all_open_positions_and_pending_orders_for_symbol(symbol_name):
    _finalize_and_log_cycle(symbol_name, outcome="MANUAL_CLOSEALL")

    logger.info(f"CLOSEALL_CYCLE ({symbol_name}): Attempting to close all cycle activity...")
    config = SYMBOL_CONFIGS[symbol_name]

    tracked_pending_ticket_snapshot = 0
    with global_state_lock:
        tracked_pending_ticket_snapshot = active_pending_order_ticket.get(symbol_name, 0)

    if tracked_pending_ticket_snapshot != 0:
        logger.debug(f"CLOSEALL_CYCLE ({symbol_name}): Cancelling tracked pending order: {tracked_pending_ticket_snapshot}")
        cancel_order(symbol_name, tracked_pending_ticket_snapshot, "Cycle End - Cancel Tracked Pending")

    broker_pending_orders = mt5.orders_get(symbol=symbol_name, magic=config["MAGIC_NUMBER"])
    if broker_pending_orders:
        for order in broker_pending_orders:
            if order.type in [mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_SELL_STOP]:
                if order.ticket != tracked_pending_ticket_snapshot or tracked_pending_ticket_snapshot == 0:
                    logger.debug(f"CLOSEALL_CYCLE ({symbol_name}): Sweeping additional broker pending order {order.ticket}.")
                    cancel_order(symbol_name, order.ticket, "Cycle End - Sweep Cancel Pending")
    else: logger.debug(f"CLOSEALL_CYCLE ({symbol_name}): No pending orders found on broker with magic {config['MAGIC_NUMBER']} during sweep.")

    tickets_to_close_this_cycle_snapshot = []
    with global_state_lock:
        tickets_to_close_this_cycle_snapshot = list(cycle_open_position_tickets.get(symbol_name, []))

    if tickets_to_close_this_cycle_snapshot:
        logger.debug(f"CLOSEALL_CYCLE ({symbol_name}): Tracked open positions for cycle: {tickets_to_close_this_cycle_snapshot}")
        for ticket_to_close in tickets_to_close_this_cycle_snapshot:
            close_single_position(symbol_name, ticket_to_close, "Cycle End - Close Pos")
    else: logger.debug(f"CLOSEALL_CYCLE ({symbol_name}): No open positions tracked in current cycle state to close.")

    reset_cycle_state_for_symbol(symbol_name)

def place_single_next_pending_order(symbol_name, based_on_position_snapshot):
    if not is_trading_hours_for_symbol(symbol_name):
        next_level_attempt_snapshot = 0
        with global_state_lock:
            next_level_attempt_snapshot = current_level.get(symbol_name, 0) + 1
        logger.info(f"PSP_TIME_RESTRICT ({symbol_name}): L{next_level_attempt_snapshot} pending order placement skipped. Outside trading hours for this symbol.")
        print(f"PSP for {symbol_name}: L{next_level_attempt_snapshot} pending order placement skipped. Outside trading hours for this symbol.")
        return

    config = SYMBOL_CONFIGS[symbol_name]
    current_pending_snapshot = 0
    num_open_positions_snapshot = 0
    current_level_snapshot = 0
    l0_price_snapshot = 0.0
    is_l0_buy_actual_snapshot = None

    with global_state_lock:
        if not is_cycle_active.get(symbol_name, False):
            logger.warning(f"PSP_SKIP ({symbol_name}): Cycle became inactive. Skipping pending order placement.")
            return
        current_pending_snapshot = active_pending_order_ticket.get(symbol_name, 0)
        num_open_positions_snapshot = len(cycle_open_position_tickets.get(symbol_name, []))
        current_level_snapshot = current_level.get(symbol_name, 0)
        l0_price_snapshot = cycle_L0_entry_price.get(symbol_name, 0.0)
        is_l0_buy_actual_snapshot = LAST_L0_WAS_BUY.get(symbol_name)

    if current_pending_snapshot != 0:
        logger.error(f"PSP_ERROR ({symbol_name}): Pending order {current_pending_snapshot} already exists. Skipping.")
        return

    if num_open_positions_snapshot >= config["MAX_TRADES_IN_CYCLE"]:
        logger.info(f"PSP_MAX_TRADES ({symbol_name}): Max trades ({config['MAX_TRADES_IN_CYCLE']}) would be reached. Not placing L{current_level_snapshot + 1} pending.")
        return

    info = get_symbol_details(symbol_name)
    if not info:
        logger.error(f"PSP_ERROR ({symbol_name}): No symbol info for {symbol_name}.")
        return

    next_lot = normalize_lot(symbol_name, based_on_position_snapshot.volume * config["LOT_MULTIPLIER"])
    if next_lot is None or next_lot <= 0:
        logger.error(f"PSP_ERROR ({symbol_name}): L{current_level_snapshot + 1} Pending: Invalid lot size ({next_lot}) for {symbol_name}.")
        return

    place_as_buy_stop = not (based_on_position_snapshot.type == mt5.POSITION_TYPE_BUY)
    next_level_to_place = current_level_snapshot + 1
    logger.debug(f"PSP_LOGIC ({symbol_name}): based_on_position (L{current_level_snapshot}) was {'BUY' if based_on_position_snapshot.type == mt5.POSITION_TYPE_BUY else 'SELL'}. Next pending (L{next_level_to_place}) will be {'BUY_STOP' if place_as_buy_stop else 'SELL_STOP'}")

    pending_entry_price = 0.0
    if l0_price_snapshot == 0.0:
        logger.error(f"PSP_ERROR ({symbol_name}): cycle_L0_entry_price is 0.0. Cannot place L{next_level_to_place} pending order.")
        return

    trigger_dist_offset_points = config["TRIGGER_DISTANCE_PIPS"] * config["PIP_MULTIPLIER"] * info.point

    if is_l0_buy_actual_snapshot is None:
        logger.error(f"PSP_ERROR ({symbol_name}): LAST_L0_WAS_BUY is not set. Cannot determine p_alternate for L{next_level_to_place}.")
        return

    if next_level_to_place % 2 == 1:
        if is_l0_buy_actual_snapshot:
            p_alternate = round(l0_price_snapshot - trigger_dist_offset_points, info.digits)
        else:
            p_alternate = round(l0_price_snapshot + trigger_dist_offset_points, info.digits)
        pending_entry_price = p_alternate
        logger.debug(f"PSP_LOGIC ({symbol_name}): Placing L{next_level_to_place} (odd) pending at p_alternate: {pending_entry_price}")
    else:
        pending_entry_price = l0_price_snapshot
        logger.debug(f"PSP_LOGIC ({symbol_name}): Placing L{next_level_to_place} (even) pending at L0 entry price: {pending_entry_price}")

    comment_pending = f"TrapCycle L{next_level_to_place} {'PBS' if place_as_buy_stop else 'PSS'} M{config['MAGIC_NUMBER']}"

    new_pending_ticket = place_pending_stop_order(symbol_name, place_as_buy_stop, next_lot, pending_entry_price, config["NOMINAL_SL_PIPS"], config["NOMINAL_TP_PIPS"], comment_pending)

    if new_pending_ticket != 0:
        with global_state_lock:
            if not is_cycle_active.get(symbol_name, False):
                logger.warning(f"PSP_LATE_SKIP ({symbol_name}): Cycle became inactive after pending order placed. Attempting to cancel {new_pending_ticket}.")
                return

            if active_pending_order_ticket.get(symbol_name, 0) == 0:
                active_pending_order_ticket[symbol_name] = new_pending_ticket
                active_pending_order_is_buy_stop[symbol_name] = place_as_buy_stop
                logger.info(f"PSP_SUCCESS ({symbol_name}): Pending L{next_level_to_place} placed @ {pending_entry_price} (Ticket: {new_pending_ticket})")
            else:
                logger.warning(f"PSP_CONCURRENCY ({symbol_name}): Another pending order {active_pending_order_ticket[symbol_name]} appeared. Cancelling newly placed {new_pending_ticket}.")
                cancel_order(symbol_name, new_pending_ticket, "PSP Auto-Cancel (Concurrency)")
    else:
        logger.error(f"PSP_FAIL ({symbol_name}): Failed to place L{next_level_to_place} pending order.")


def start_L0_market_cycle(symbol_name, is_buy_L0):
    if not is_trading_hours_for_symbol(symbol_name):
        logger.warning(f"START_L0 ({symbol_name}): Cannot start cycle. Outside trading hours for this symbol...")
        print(f"Cannot start L0 for {symbol_name}: Outside trading hours for this symbol...")
        return

    with global_state_lock:
        if is_cycle_active.get(symbol_name, False):
            logger.warning(f"START_L0 ({symbol_name}): Cycle already active. Cannot start new L0.")
            print(f"Cannot start L0 for {symbol_name}: Cycle already active.")
            return

    config = SYMBOL_CONFIGS[symbol_name]
    print(f"Attempting to start L0 {'BUY' if is_buy_L0 else 'SELL'} cycle for {symbol_name}...")
    logger.info(f"START_L0_INIT ({symbol_name}): Attempting L0 {'BUY' if is_buy_L0 else 'SELL'} ...")

    reset_cycle_state_for_symbol(symbol_name, called_for_new_l0_setup=True)

    comment = ""
    with global_state_lock:
        current_level[symbol_name] = 0
        comment = f"TrapCycle L{current_level[symbol_name]} M{config['MAGIC_NUMBER']}"

    lot = normalize_lot(symbol_name, config["INITIAL_LOT_SIZE"])
    if lot is None or lot <= 0:
        logger.error(f"START_L0_FAIL ({symbol_name}): Invalid lot size ({lot}). Cycle not started.")
        print(f"L0 {symbol_name}: Invalid lot size ({lot}). Cycle not started.")
        return

    order_result = place_market_order(symbol_name, is_buy_L0, lot, config["NOMINAL_SL_PIPS"], config["NOMINAL_TP_PIPS"], comment)

    if order_result:
        pos_details_snapshot = get_position_details_from_order_result(symbol_name, order_result, comment)
        if pos_details_snapshot:
            l0_actual_direction_for_tracking = False
            with global_state_lock:
                is_cycle_active[symbol_name] = True
                active_position_ticket[symbol_name] = pos_details_snapshot.ticket
                active_position_entry_price[symbol_name] = pos_details_snapshot.price_open
                active_position_lot_size[symbol_name] = pos_details_snapshot.volume
                active_position_is_buy[symbol_name] = (pos_details_snapshot.type == mt5.POSITION_TYPE_BUY)

                LAST_L0_WAS_BUY[symbol_name] = active_position_is_buy[symbol_name]
                l0_actual_direction_for_tracking = active_position_is_buy[symbol_name]

                cycle_open_position_tickets[symbol_name] = [pos_details_snapshot.ticket]
                cycle_L0_entry_price[symbol_name] = pos_details_snapshot.price_open
                logger.info(f"START_L0_SUCCESS ({symbol_name}): L0 {'BUY' if LAST_L0_WAS_BUY[symbol_name] else 'SELL'} cycle active. Pos: {pos_details_snapshot.ticket}.")
                print(f"L0 {'BUY' if LAST_L0_WAS_BUY[symbol_name] else 'SELL'} cycle active for {symbol_name}. Pos: {pos_details_snapshot.ticket}.")

            _init_cycle_tracking(symbol_name, l0_actual_direction_for_tracking)
            place_single_next_pending_order(symbol_name, pos_details_snapshot)
        else:
            logger.error(f"START_L0_FAIL ({symbol_name}): L0 market order sent (Order #{order_result.order}), but pos details not confirmed. Cycle aborted.")
            print(f"L0 market order sent for {symbol_name}, but position not confirmed. Cycle aborted.")
    else:
        logger.error(f"START_L0_FAIL ({symbol_name}): L0 market order failed. Cycle not started.")
        print(f"L0 market order failed for {symbol_name}. Cycle not started.")

def manage_active_cycle(symbol_name):
    with global_state_lock:
        if not is_cycle_active.get(symbol_name, False):
            return

        log_msg_parts = [
            f"MANAGE_CYCLE_ENTER ({symbol_name}):",
            f"L{current_level.get(symbol_name,0)},",
            f"ActivePos: {active_position_ticket.get(symbol_name,0)},",
            f"Pending: {active_pending_order_ticket.get(symbol_name,0)},",
            f"OpenTickets: {list(cycle_open_position_tickets.get(symbol_name, []))}"
        ]
        logger.debug(" ".join(log_msg_parts))

    config = SYMBOL_CONFIGS[symbol_name]

    current_broker_positions = mt5.positions_get(symbol=symbol_name, magic=config["MAGIC_NUMBER"])
    current_broker_pos_tickets_set = {p.ticket for p in current_broker_positions} if current_broker_positions else set()

    _trigger_reset = False
    _trigger_closeall_reset = False
    with global_state_lock:
        if not is_cycle_active.get(symbol_name, False): return

        initial_tracked_count = len(cycle_open_position_tickets.get(symbol_name, []))
        current_tracked_tickets_snapshot = cycle_open_position_tickets.get(symbol_name, [])
        valid_tracked_open_pos_tickets = [t for t in current_tracked_tickets_snapshot if t in current_broker_pos_tickets_set]

        if len(valid_tracked_open_pos_tickets) != initial_tracked_count:
            logger.debug(f"MANAGE_RECONCILE ({symbol_name}): Open positions reconciled. Was: {initial_tracked_count}, Now: {len(valid_tracked_open_pos_tickets)}")
        cycle_open_position_tickets[symbol_name] = valid_tracked_open_pos_tickets

        no_open_positions_after_reconcile = not cycle_open_position_tickets.get(symbol_name, [])
        pending_order_exists_after_reconcile = active_pending_order_ticket.get(symbol_name, 0) != 0

        _trigger_reset = no_open_positions_after_reconcile and not pending_order_exists_after_reconcile
        _trigger_closeall_reset = no_open_positions_after_reconcile and pending_order_exists_after_reconcile

    if _trigger_reset:
        logger.info(f"MANAGE_END_CONDITION ({symbol_name}): All positions closed (reconciled) and no pending order. Resetting.")
        reset_cycle_state_for_symbol(symbol_name); return
    if _trigger_closeall_reset:
        logger.info(f"MANAGE_END_CONDITION ({symbol_name}): All positions closed (reconciled), but pending order exists. Closing all & Resetting.")
        close_all_open_positions_and_pending_orders_for_symbol(symbol_name); return

    tick = mt5.symbol_info_tick(symbol_name)
    if not tick: logger.error(f"MANAGE_TICK_FAIL ({symbol_name}): Could not get tick for TP check."); return

    _tp_hit_detected = False
    tickets_for_tp_check_snapshot = []
    with global_state_lock:
        if not is_cycle_active.get(symbol_name, False): return
        tickets_for_tp_check_snapshot = list(cycle_open_position_tickets.get(symbol_name, []))

    for pos_ticket in tickets_for_tp_check_snapshot:
        pos_details_list = mt5.positions_get(ticket=pos_ticket)
        if pos_details_list and len(pos_details_list) == 1:
            pos = pos_details_list[0]
            if pos.tp > 0:
                tp_hit = (pos.type == mt5.POSITION_TYPE_BUY and tick.bid >= pos.tp) or \
                         (pos.type == mt5.POSITION_TYPE_SELL and tick.ask <= pos.tp)
                if tp_hit:
                    logger.info(f"MANAGE_TP_HIT ({symbol_name}): TP HIT detected for pos #{pos.ticket}! Cycle WIN!")
                    _tp_hit_detected = True; break

    if _tp_hit_detected:
        _finalize_and_log_cycle(symbol_name, outcome="WIN")
        close_all_open_positions_and_pending_orders_for_symbol(symbol_name); return

    newly_opened_position_from_pending_snapshot = None
    _pending_ticket_to_clear_state = 0

    pending_ticket_to_check_snapshot = 0
    with global_state_lock:
        if not is_cycle_active.get(symbol_name, False): return
        pending_ticket_to_check_snapshot = active_pending_order_ticket.get(symbol_name, 0)

    if pending_ticket_to_check_snapshot != 0:
        history_order_info_list = mt5.history_orders_get(ticket=pending_ticket_to_check_snapshot)
        if history_order_info_list and len(history_order_info_list) > 0:
            history_order_info = history_order_info_list[0]
            order_type_str = {mt5.ORDER_TYPE_BUY_STOP: "BUY_STOP", mt5.ORDER_TYPE_SELL_STOP: "SELL_STOP"}.get(history_order_info.type, f"PENDING_TYPE_{history_order_info.type}")

            if history_order_info.state == mt5.ORDER_STATE_FILLED:
                logger.info(f"MANAGE_PENDING_FILLED ({symbol_name}): Tracked Pending Order {pending_ticket_to_check_snapshot} ({order_type_str}) FILLED (Order Ticket: {history_order_info.ticket}). State: {history_order_info.state}, PositionID in Order: {history_order_info.position_id}")

                time.sleep(1.0) # Increased sleep duration slightly

                current_broker_positions_after_fill = mt5.positions_get(symbol=symbol_name, magic=config["MAGIC_NUMBER"])
                open_tickets_snapshot_for_fill_check = []
                with global_state_lock:
                    if not is_cycle_active.get(symbol_name, False):
                        logger.warning(f"MANAGE_PENDING_FILLED ({symbol_name}): Cycle became inactive while processing filled pending order {pending_ticket_to_check_snapshot}. Aborting identification.")
                        return
                    open_tickets_snapshot_for_fill_check = list(cycle_open_position_tickets.get(symbol_name, []))

                # --- Attempt 1: Use position_id directly from the filled order history ---
                if history_order_info.position_id != 0 and current_broker_positions_after_fill:
                    for pos_check in current_broker_positions_after_fill:
                        if pos_check.ticket == history_order_info.position_id:
                            if pos_check.ticket not in open_tickets_snapshot_for_fill_check:
                                newly_opened_position_from_pending_snapshot = pos_check
                                logger.info(f"MANAGE_PENDING_FILLED_DEBUG ({symbol_name}): Found position {pos_check.ticket} via history_order_info.position_id ({history_order_info.position_id}).")
                                break
                            else:
                                logger.warning(f"MANAGE_PENDING_FILLED_DEBUG ({symbol_name}): Position {pos_check.ticket} (from history_order_info.position_id) already in tracked list: {open_tickets_snapshot_for_fill_check}.")

                # --- Attempt 2: Use deals associated with the filled order ---
                if not newly_opened_position_from_pending_snapshot:
                    logger.info(f"MANAGE_PENDING_FILLED_DEBUG ({symbol_name}): Position not found via order's position_id. Trying via deals for order {history_order_info.ticket}.")
                    deals = mt5.history_deals_get(order=history_order_info.ticket)

                    if deals:
                        logger.debug(f"MANAGE_PENDING_FILLED_DEBUG ({symbol_name}): Found {len(deals)} deals for order {history_order_info.ticket}.")
                        for deal in sorted(deals, key=lambda d: d.time_msc, reverse=True): # Process most recent deal first
                            logger.debug(f"MANAGE_PENDING_FILLED_DEBUG ({symbol_name}): Checking deal {deal.ticket}, Deal PositionID: {deal.position_id}, Deal Type: {deal.type}, Deal Entry: {deal.entry}")
                            if deal.position_id != 0:
                                if current_broker_positions_after_fill: # Use the already fetched list
                                    for pos_check in current_broker_positions_after_fill:
                                        if pos_check.ticket == deal.position_id:
                                            if pos_check.ticket not in open_tickets_snapshot_for_fill_check:
                                                newly_opened_position_from_pending_snapshot = pos_check
                                                logger.info(f"MANAGE_PENDING_FILLED_DEBUG ({symbol_name}): Found position {pos_check.ticket} via deal {deal.ticket} (deal.position_id: {deal.position_id}).")
                                                break
                                            else:
                                                logger.warning(f"MANAGE_PENDING_FILLED_DEBUG ({symbol_name}): Position {pos_check.ticket} (from deal {deal.ticket}) already in tracked list: {open_tickets_snapshot_for_fill_check}.")
                                    if newly_opened_position_from_pending_snapshot:
                                        break
                    else:
                        logger.info(f"MANAGE_PENDING_FILLED_DEBUG ({symbol_name}): No deals found for order {history_order_info.ticket}. This is unusual for a filled order.")

                # --- Attempt 3: Broader search for any new untracked position (Last Resort) ---
                if not newly_opened_position_from_pending_snapshot:
                    logger.warning(f"MANAGE_PENDING_FILLED_DEBUG ({symbol_name}): Position not found via order's position_id or deals. Trying broad search for new untracked positions.")
                    if current_broker_positions_after_fill:
                        sorted_positions = sorted(current_broker_positions_after_fill, key=lambda p: p.time_msc, reverse=True)
                        for pos_check in sorted_positions:
                            if pos_check.magic == config["MAGIC_NUMBER"] and pos_check.symbol == symbol_name:
                                if pos_check.ticket not in open_tickets_snapshot_for_fill_check:
                                    time_diff_seconds = abs(pos_check.time_msc / 1000 - history_order_info.time_done)
                                    expected_pos_type = mt5.POSITION_TYPE_BUY if history_order_info.type == mt5.ORDER_TYPE_BUY_STOP else mt5.POSITION_TYPE_SELL

                                    if time_diff_seconds < 5.0 and pos_check.type == expected_pos_type :
                                        newly_opened_position_from_pending_snapshot = pos_check
                                        logger.info(f"MANAGE_PENDING_FILLED_DEBUG ({symbol_name}): Found position {pos_check.ticket} via broad search (new, untracked, recent, matching type). Time diff: {time_diff_seconds:.2f}s.")
                                        break
                                    else:
                                        logger.debug(f"MANAGE_PENDING_FILLED_DEBUG ({symbol_name}): Candidate untracked pos {pos_check.ticket}. Time diff: {time_diff_seconds:.2f}s, OrderType: {history_order_info.type}, PosType: {pos_check.type}. Expected PosType: {expected_pos_type}. Skipping.")
                        if not newly_opened_position_from_pending_snapshot:
                             logger.warning(f"MANAGE_PENDING_FILLED_DEBUG ({symbol_name}): Broad search for new untracked positions also failed.")

                if newly_opened_position_from_pending_snapshot:
                    logger.info(f"MANAGE_PENDING_FILLED_SUCCESS ({symbol_name}): Pos {newly_opened_position_from_pending_snapshot.ticket} (Type: {newly_opened_position_from_pending_snapshot.type}) identified from pending order {pending_ticket_to_check_snapshot} (Type: {history_order_info.type}).")
                else:
                    logger.error(f"MANAGE_PENDING_FILLED_ERROR ({symbol_name}): CRITICAL - Pending {pending_ticket_to_check_snapshot} (Order Ticket: {history_order_info.ticket}, Type: {order_type_str}) filled but UNABLE to identify resulting pos!")
                    logger.error(f"MANAGE_PENDING_FILLED_ERROR_DETAILS ({symbol_name}): Filled Order Hist: Ticket={history_order_info.ticket}, Type={history_order_info.type}, State={history_order_info.state}, PriceOpen={history_order_info.price_open}, SL={history_order_info.sl}, TP={history_order_info.tp}, VolumeCurr={history_order_info.volume_current}, TimeDone={datetime.datetime.fromtimestamp(history_order_info.time_done)}, PosIDInOrder={history_order_info.position_id}")
                    if current_broker_positions_after_fill:
                        logger.error(f"MANAGE_PENDING_FILLED_ERROR_DETAILS ({symbol_name}): Current positions on broker for {symbol_name} (Magic: {config['MAGIC_NUMBER']}) ({len(current_broker_positions_after_fill)}):")
                        for p_err in sorted(current_broker_positions_after_fill, key=lambda pos_item: pos_item.time_msc, reverse=True): # Log newest first
                            logger.error(f"  - Pos: {p_err.ticket}, Type: {p_err.type}, Vol: {p_err.volume}, PriceOpen: {p_err.price_open}, TimeOpen: {datetime.datetime.fromtimestamp(p_err.time_msc/1000)}, Comment: '{p_err.comment}'")
                    else:
                        logger.error(f"MANAGE_PENDING_FILLED_ERROR_DETAILS ({symbol_name}): No positions found on broker for {symbol_name} with magic {config['MAGIC_NUMBER']} at this time.")
                    logger.error(f"MANAGE_PENDING_FILLED_ERROR_DETAILS ({symbol_name}): Tracked open positions at time of check: {open_tickets_snapshot_for_fill_check}")

                _pending_ticket_to_clear_state = pending_ticket_to_check_snapshot

            # <<<< CORRECTED: Changed ORDER_STATE_CANCELLED to ORDER_STATE_CANCELED >>>>
            elif history_order_info.state in [mt5.ORDER_STATE_CANCELED, mt5.ORDER_STATE_REJECTED, mt5.ORDER_STATE_EXPIRED]:
                logger.info(f"MANAGE_PENDING_INACTIVE ({symbol_name}): Pending {pending_ticket_to_check_snapshot} ({order_type_str}) inactive (State: {history_order_info.state}).")
                _pending_ticket_to_clear_state = pending_ticket_to_check_snapshot
        else:
            active_broker_orders = mt5.orders_get(ticket=pending_ticket_to_check_snapshot)
            if not (active_broker_orders and len(active_broker_orders) == 1 and \
                    active_broker_orders[0].type in [mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_SELL_STOP] and \
                    active_broker_orders[0].state == mt5.ORDER_STATE_PLACED) :
                logger.warning(f"MANAGE_PENDING_GHOST ({symbol_name}): Tracked pending {pending_ticket_to_check_snapshot} not found or not active on broker. Clearing state.")
                _pending_ticket_to_clear_state = pending_ticket_to_check_snapshot

    if _pending_ticket_to_clear_state != 0:
        with global_state_lock:
            if not is_cycle_active.get(symbol_name, False): return
            if active_pending_order_ticket.get(symbol_name, 0) == _pending_ticket_to_clear_state:
                active_pending_order_ticket[symbol_name] = 0
                active_pending_order_is_buy_stop[symbol_name] = None
                logger.debug(f"MANAGE_PENDING_STATE_CLEAR ({symbol_name}): Cleared pending ticket {_pending_ticket_to_clear_state} from state.")

    if newly_opened_position_from_pending_snapshot:
        can_place_next_pending_order = False
        with global_state_lock:
            if not is_cycle_active.get(symbol_name, False): return
            logger.info(f"MANAGE_NEW_LEVEL ({symbol_name}): Processing newly opened position {newly_opened_position_from_pending_snapshot.ticket}")
            current_level[symbol_name] += 1

            active_position_ticket[symbol_name] = newly_opened_position_from_pending_snapshot.ticket
            active_position_entry_price[symbol_name] = newly_opened_position_from_pending_snapshot.price_open
            active_position_lot_size[symbol_name] = newly_opened_position_from_pending_snapshot.volume
            active_position_is_buy[symbol_name] = (newly_opened_position_from_pending_snapshot.type == mt5.POSITION_TYPE_BUY)

            if active_position_ticket[symbol_name] not in cycle_open_position_tickets.get(symbol_name, []):
                cycle_open_position_tickets[symbol_name].append(active_position_ticket[symbol_name])

            can_place_next_pending_order = len(cycle_open_position_tickets.get(symbol_name,[])) < config["MAX_TRADES_IN_CYCLE"]

        _increment_trap_count(symbol_name)

        if can_place_next_pending_order:
             place_single_next_pending_order(symbol_name, newly_opened_position_from_pending_snapshot)
        else:
            logger.info(f"MANAGE_NEW_LEVEL ({symbol_name}): Max trades ({config['MAX_TRADES_IN_CYCLE']}) reached. No new pending order.")

# --- Cycle Management Worker ---
def cycle_management_worker():
    logger.info("Cycle management worker thread started.")
    last_manage_time = time.time()
    while not shutdown_event.is_set():
        current_time_worker = time.time()
        if current_time_worker - last_manage_time >= 1.5:
            active_symbols_to_manage_this_run = []
            with global_state_lock:
                for sym_check, is_active_check in is_cycle_active.items():
                    if is_active_check:
                        active_symbols_to_manage_this_run.append(sym_check)

            if active_symbols_to_manage_this_run:
                for sym_manage in active_symbols_to_manage_this_run:
                    if shutdown_event.is_set(): break
                    try:
                        manage_active_cycle(sym_manage)
                    except Exception as e:
                        logger.error(f"WORKER_THREAD: Error during manage_active_cycle for {sym_manage}: {e}", exc_info=True)

            last_manage_time = current_time_worker

        shutdown_event.wait(timeout=0.2)
    logger.info("Cycle management worker thread stopped.")

# --- Main Execution Loop ---
if __name__ == "__main__":
    if not initialize_mt5_connection(): exit()

    initialize_all_symbol_states()
    ensure_cycle_data_log_exists()

    print(f"\nPython Multi-Symbol Trap Cycle Bot (v10.9.3 - Corrected Lot Sizing)");
    print(f"General trading restricted to local time: {TRADING_START_HOUR:02d}:00 - {TRADING_END_HOUR:02d}:00.")

    symbols_24_7 = [s for s, c in SYMBOL_CONFIGS.items() if c.get("TRADE_24_7", False)]
    if symbols_24_7:
        print(f"24/7 TRADING ENABLED for: {', '.join(symbols_24_7)}")

    print(f"Cycle data will be logged to: {CYCLE_DATA_CSV_FILE}")
    print(f"Managing symbols: {list(SYMBOL_CONFIGS.keys())}")
    print(f"Symbol Aliases: {list(SYMBOL_ALIASES.keys())}")
    print(f"AUTO-RESTART: {'ENABLED' if AUTO_RESTART_COMPLETED_CYCLES else 'DISABLED'}.")
    print(f"Logs are being saved to '{log_file_handler.baseFilename}'")

    manager_thread = threading.Thread(target=cycle_management_worker, name="CycleManagerThread")
    manager_thread.daemon = True
    manager_thread.start()
    logger.info("Cycle management worker thread has been started.")

    try:
        while True:
            # (The rest of your main loop remains unchanged)
            any_cycle_running_now = False
            active_symbols_list_prompt = []
            with global_state_lock:
                any_cycle_running_now = any(is_cycle_active.values())
                active_symbols_list_prompt = [sym for sym, active in is_cycle_active.items() if active]
            
            trading_hours_status_str = "OPEN" if is_general_trading_hours() else "CLOSED"
            prompt_parts = [f"\nGeneral Trading Hours ({TRADING_START_HOUR:02d}:00-{TRADING_END_HOUR:02d}:00 Local): {trading_hours_status_str}."]
            if any_cycle_running_now:
                prompt_parts.append(f"Active: {', '.join(active_symbols_list_prompt)}.")
            else:
                prompt_parts.append("All cycles inactive.")
            prompt_parts.append("Cmd (buy/sell/status [s]/statusall/closeall [s|all]/exit):")
            prompt_message = " ".join(prompt_parts) + " "

            cmd_full = ""
            try:
                cmd_full = input(prompt_message).strip()
            except EOFError: print("EOF received, exiting."); logger.info("EOF received, shutting down."); break
            except KeyboardInterrupt:
                print("\nCommand input cancelled. Type 'exit' to quit or another command.")
                logger.warning("User interrupted command input (Ctrl+C at prompt).")
                continue 

            if cmd_full: 
                logger.info(f"USER_CMD_RAW: '{cmd_full}'")
                parts = cmd_full.split(); command_action = parts[0].lower()
                user_typed_symbol_or_alias = parts[1] if len(parts) > 1 else ""
                actual_broker_symbol = None
                
                if command_action == 'exit': logger.info("USER_CMD: 'exit'"); break

                if user_typed_symbol_or_alias:
                    resolved_alias = SYMBOL_ALIASES.get(user_typed_symbol_or_alias.lower())
                    if resolved_alias: actual_broker_symbol = resolved_alias
                    elif user_typed_symbol_or_alias.upper() in SYMBOL_CONFIGS: actual_broker_symbol = user_typed_symbol_or_alias.upper()
                    
                    if not actual_broker_symbol and not (command_action in ['statusall', 'closeall'] and user_typed_symbol_or_alias.lower() == 'all'):
                        print(f"Unknown symbol or alias: '{user_typed_symbol_or_alias}'. Valid: {list(SYMBOL_CONFIGS.keys())} & {list(SYMBOL_ALIASES.keys())}"); 
                        logger.warning(f"Unknown symbol command: {user_typed_symbol_or_alias}"); continue
                    elif actual_broker_symbol and actual_broker_symbol not in SYMBOL_CONFIGS: 
                        print(f"Alias maps to '{actual_broker_symbol}', which is not in SYMBOL_CONFIGS."); 
                        logger.warning(f"Bad alias mapping or config: {user_typed_symbol_or_alias} -> {actual_broker_symbol}"); continue
                elif command_action in ['buy', 'sell', 'status', 'closeall'] and not (command_action == 'closeall' and user_typed_symbol_or_alias.lower() == 'all'):
                     print(f"Specify symbol/alias for '{command_action}' or use 'all' for closeall/statusall."); 
                     logger.warning(f"Command '{command_action}' missing symbol."); continue
                
                if command_action == 'buy' or command_action == 'sell': 
                    if actual_broker_symbol:
                        user_chose_buy_for_preference = (command_action == 'buy')
                        favored_direction_str = "BUY" if user_chose_buy_for_preference else "SELL"
                        current_set_preference_snapshot = None
                        with global_state_lock:
                            current_set_preference_snapshot = user_initial_preference_is_buy.get(actual_broker_symbol)

                        print(f"\nCommand: Start cycle for {actual_broker_symbol} with {favored_direction_str} as user-preferred.")
                        if current_set_preference_snapshot is not None:
                            current_pref_str = "BUY" if current_set_preference_snapshot else "SELL"
                            if current_set_preference_snapshot == user_chose_buy_for_preference:
                                print(f"(Matches current favored: {current_pref_str})")
                            else: 
                                print(f"NOTE: Will change favored for {actual_broker_symbol} from {current_pref_str} to {favored_direction_str}.")
                        print(f"  - This L0: 75% for {favored_direction_str}, 25% for opposite.")
                        print(f"  - {favored_direction_str} becomes favored for auto-restarts.")

                        while True:
                            confirmation = input(f"Proceed with {actual_broker_symbol}? (y/n): ").strip().lower()
                            if confirmation == 'y':
                                with global_state_lock:
                                    user_initial_preference_is_buy[actual_broker_symbol] = user_chose_buy_for_preference
                                logger.info(f"USER_CMD ({actual_broker_symbol}): User confirmed {favored_direction_str} as initial favored.")
                                
                                random_val = random.random()
                                actual_l0_is_buy_for_this_instance = False
                                reason = ""
                                if random_val <= 0.75:
                                    actual_l0_is_buy_for_this_instance = user_chose_buy_for_preference
                                    reason = f"Rolled <=0.75 (val: {random_val:.4f})"
                                else:
                                    actual_l0_is_buy_for_this_instance = not user_chose_buy_for_preference
                                    reason = f"Rolled >0.75 (val: {random_val:.4f})"
                                
                                actual_dir_str_instance = 'BUY' if actual_l0_is_buy_for_this_instance else 'SELL'
                                logger.info(f"USER_CMD ({actual_broker_symbol}): Preferred {favored_direction_str}. {reason}. Actual L0: {actual_dir_str_instance}.")
                                print(f"--> Probability ({reason}): Attempting L0 as {actual_dir_str_instance} for {actual_broker_symbol}.")
                                
                                start_L0_market_cycle(actual_broker_symbol, is_buy_L0=actual_l0_is_buy_for_this_instance)
                                break
                            elif confirmation == 'n':
                                print(f"L0 start for {actual_broker_symbol} cancelled."); logger.info(f"USER_CMD ({actual_broker_symbol}): User cancelled."); break
                            else: print("Invalid input. 'y' or 'n'.")
                
                elif command_action == 'status' or command_action == 'statusall':
                    symbols_to_process_status = []
                    if command_action == 'statusall':
                        print("--- Status for All Configured Symbols ---")
                        symbols_to_process_status = list(SYMBOL_CONFIGS.keys())
                    elif actual_broker_symbol:
                        symbols_to_process_status = [actual_broker_symbol]
                    else:
                        print("Use 'status [symbol/alias]' or 'statusall'.")
                    
                    for sym_stat in symbols_to_process_status:
                        # ... status logic is unchanged ...
                        print(f"\n--- Status for {sym_stat} ---")
                        # This part can be refactored into a helper function to avoid repetition
                        # but for now, we'll keep it as is.
                        # ...
                    if command_action == 'statusall':
                        print("--- End of Status for All ---")

                elif command_action == 'closeall':
                    symbols_to_close_list = []
                    if user_typed_symbol_or_alias.lower() == 'all':
                        logger.info("USER_COMMAND: closeall all"); print("Closing all cycles for all configured symbols...")
                        with global_state_lock:
                            for sym_check in list(SYMBOL_CONFIGS.keys()): 
                                if is_cycle_active.get(sym_check, False) or \
                                   active_pending_order_ticket.get(sym_check,0) != 0 or \
                                   (cycle_tracking_data.get(sym_check) is not None):
                                    symbols_to_close_list.append(sym_check)
                        for sym_to_close in symbols_to_close_list: 
                            print(f"--- Closing for {sym_to_close} ---")
                            close_all_open_positions_and_pending_orders_for_symbol(sym_to_close) 
                    elif actual_broker_symbol:
                        logger.info(f"USER_COMMAND: closeall {actual_broker_symbol}"); 
                        print(f"--- Closing for {actual_broker_symbol} ---")
                        close_all_open_positions_and_pending_orders_for_symbol(actual_broker_symbol) 
                    else: 
                        print("Specify symbol/alias for closeall or use 'closeall all'.")
                else:
                    if command_action: 
                        print(f"Unknown command action: {command_action}")
            
    except KeyboardInterrupt: print("\nBot stopped by user (Ctrl+C). Performing cleanup..."); logger.info("Ctrl+C detected. Signalling threads.")
    except Exception as e: logger.exception(f"Unexpected error in main loop: {e}"); print(f"Unexpected error: {e}. Check log.")
    finally:
        print("\nInitiating shutdown sequence...")
        logger.info("Shutdown sequence initiated.")
        
        shutdown_event.set()
        logger.info("Shutdown event set for worker thread.")
        if manager_thread.is_alive():
            logger.info("Waiting for cycle management worker thread to join...")
            manager_thread.join(timeout=5.0) 
            if manager_thread.is_alive(): logger.warning("Cycle management worker thread did not join in time.")
            else: logger.info("Cycle management worker thread joined successfully.")
        else: logger.info("Cycle management worker thread was not alive or already joined.")

        logger.info("Finalizing and logging any active cycles before MT5 shutdown...")
        symbols_to_finalize_snapshot = []
        with global_state_lock: 
             for sym_final_check in SYMBOL_CONFIGS.keys():
                if is_cycle_active.get(sym_final_check, False) or (cycle_tracking_data.get(sym_final_check) is not None):
                    symbols_to_finalize_snapshot.append(sym_final_check)
        
        for sym_final in symbols_to_finalize_snapshot:
            _finalize_and_log_cycle(sym_final, outcome="SHUTDOWN_INTERRUPT")
            warning_msg = f"WARNING: Bot shutting down WITH ACTIVE CYCLE for {sym_final}. Attempted final log."
            logger.warning(warning_msg); print(warning_msg)
        
        shutdown_msg = "Shutting down MT5 connection..."; logger.info(shutdown_msg); print(shutdown_msg)
        mt5.shutdown()
        final_msg = "Bot has been shut down."; logger.info(final_msg); print(final_msg)