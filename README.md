# 🇮🇳 Indian Corporate Bond Risk & Yield Engine

**A high-frequency quantitative risk engine for the Indian Corporate Bond (NCD) market, capable of calculating real-time XIRR for illiquid securities using complex cashflow modeling.**

---

### 🚀 Overview
This system solves a critical problem in the Indian fixed-income market: **Liquidity & Data Opacity.**
Unlike equities, corporate bonds trade offline or with low liquidity, and their cashflow structures (partial redemptions, step-up coupons) are often buried in PDF term sheets.

This engine:
1.  **Scrapes & Normalizes** unstructured bond data from NSDL/BSE using Python & LLMs.
2.  **Models Cashflows** accurately, accounting for partial redemptions and irregular payment dates.
3.  **Streams Real-Time Prices** via the Zerodha Kite Connect API.
4.  **Computes XIRR (Yield)** largely in real-time to identify arbitrage opportunities against the G-Sec curve.

---

### 🏗 System Architecture

The system follows a modular Microservices-like architecture:

* **Ingestion Layer:**
    * `bond_meta_cache_by_isin.py`: Fetches static metadata (ISIN, Face Value, Maturity) from BSE APIs.
    * `redemption_data_check_and_load.py`: The heavy lifter. Scrapes detailed redemption schedules from NSDL. **Uses OpenAI GPT-4o / Llama 3** to parse unstructured frequency strings (e.g., "Quarterly starting 2024") into integer frequencies for the math engine.
* **Pricing Engine:**
    * `bonds_trade_data.py`: Connects to Kite WebSocket/API to fetch live Market Depth (L2 Data).
    * **Math Kernel (`common_util.py`):** Implements a custom **XIRR (Extended Internal Rate of Return)** algorithm using `scipy.optimize` (Newton-Raphson & BrentQ) to solve for yield on irregular dates.
* **Storage Layer:**
    * **PostgreSQL:** Relational storage for 5000+ active ISINs, linked cashflows, and historical ticks.
* **Visualization Layer:**
    * **Streamlit (`streamline_app.py`):** Real-time dashboard for scanning yields `> 18%`.
    * **Flask (`future_cashflows.py`):** Lightweight web app to visualize future cashflow waterfalls for the portfolio.

---

### 🛠 Tech Stack

* **Core:** Python 3.10+, Pandas, NumPy
* **Quant Math:** SciPy (Optimization for XIRR/YTM)
* **Data Engineering:** SQLAlchemy, PostgreSQL
* **AI/NLP:** OpenAI API / Local Llama (via Ollama) for unstructured financial text parsing
* **Web/UI:** Streamlit, Flask
* **Broker Connectivity:** Zerodha Kite Connect API

---

### 🧩 Key Modules

#### 1. The LLM-Powered Parser
*File: `redemption_data_check_and_load.py`*
Standard regex often fails on complex bond term sheets. This module uses an LLM to interpret natural language payment terms:
```python
# Concept:
prompt = "Raw frequency: 'Semi-annual starting 2025' -> Return Integer"
# Result: 2 (Payments per year)
