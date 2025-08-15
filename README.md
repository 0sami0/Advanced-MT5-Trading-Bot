# Advanced Multi-Symbol MT5 Trading Bot

![Python](https://img.shields.io/badge/Python-3.10%2B-blue) ![Platform](https://img.shields.io/badge/Platform-MetaTrader%205-orange) ![License](https://img.shields.io/badge/License-MIT-green)

A robust, multi-symbol automated trading bot for the MetaTrader 5 platform, built with Python. This project demonstrates advanced software engineering principles including thread-safe concurrency, persistent data logging for performance analysis, and a highly modular configuration system.

---

## Key Features

This isn't just a simple script; it's a stable, long-running application designed for reliability and maintainability.

*   **Multi-Symbol Management**: Trade multiple symbols (e.g., EURUSD, XAUUSD, BTCUSD) simultaneously from a single instance, each with its own unique configuration.
*   **Thread-Safe Concurrency**: A dedicated management thread runs the core trading logic asynchronously, ensuring the main user interface remains responsive while the bot actively manages trades. Shared data is protected using `threading.Lock` to prevent race conditions.
*   **Robust Logging**: Comprehensive logging to both console and a file (`trap_cycle_bot.log`) with detailed context (module, function, line number) for easy debugging and monitoring.
*   **Persistent Cycle Analytics**: Every trading cycle's outcome (Win, Manual Close, etc.), duration, and performance metrics are automatically logged to a CSV file (`trading_cycle_data.csv`) for later analysis.
*   **Graceful Shutdown**: The bot can be stopped safely with `Ctrl+C` or an `exit` command, ensuring all threads are properly terminated and the connection to the MT5 terminal is closed cleanly.
*   **Dynamic Lot Sizing**: The bot correctly calculates and normalizes lot sizes based on broker-specific volume steps and limits.
*   **Flexible Configuration**: All trading parameters (lot sizes, take profit/stop loss pips, magic numbers) are managed in a central configuration dictionary, making it easy to add new symbols or adjust strategies without changing the core code.

## The "Trap Cycle" Strategy

The core logic is based on a "Trap Cycle" hedging strategy:
1.  An initial Level 0 (L0) market order is placed (either BUY or SELL).
2.  A pending stop order is immediately placed in the opposite direction at a key price level.
3.  If the market moves against the active position and triggers the pending order, a new, larger position is opened, creating a "trap". This new position becomes the active one.
4.  The process repeats, with the bot placing a new pending order to "trap" the market again.
5.  The cycle concludes when any position's Take Profit is hit, which then closes all open positions and pending orders for that symbol's cycle.

## How to Run

### Prerequisites
- Python 3.10+
- A running MetaTrader 5 terminal
- An MT5 account with API trading enabled

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/0sami0/Advanced-MT5-Trading-Bot.git
    cd Advanced-MT5-Trading-Bot
    ```

2.  **Install the required dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

### Execution

1.  Ensure your MT5 terminal is open and logged in.
2.  Run the bot from your terminal:
    ```bash
    python forex.py
    ```

3.  Follow the on-screen commands to start, monitor, and close trading cycles.

## Disclaimer

This software is for educational and demonstration purposes only. Automated trading involves significant risk. I am not responsible for any financial losses incurred from using this bot.

This strategy was profitable for a period but ultimately failed due to a catastrophic loss during an unexpected market trend. This project taught me a critical lesson in the dangers of Martingale-style risk and the importance of implementing a system-wide stop-loss. My future work focuses on developing strategies with a more robust risk management framework.
