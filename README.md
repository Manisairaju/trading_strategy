# Quotex Confluence Trading Bot

A Python-based automated signal generator for Fixed-Time Trading (Quotex). It uses TradingView's real-time WebSocket data to calculate a triple-confluence strategy (SMA 50, Envelopes, Stochastic Oscillator) and sends alerts to Telegram.

## Setup Instructions

1. Install Python 3.8+
2. Clone this repository:
   `git clone https://github.com/YOUR_USERNAME/quotex-bot.git`
3. Install requirements:
   `pip install -r requirements.txt`
4. Set your Telegram environment variables:
   - Linux/Mac: 
     `export TELEGRAM_TOKEN="your_bot_token"`
     `export TELEGRAM_CHAT_ID="your_chat_id"`
   - Windows CMD: 
     `set TELEGRAM_TOKEN="your_bot_token"`
     `set TELEGRAM_CHAT_ID="your_chat_id"`
5. Run the bot:
   `python bot.py`
   
