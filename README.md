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
```
### 2. The Custom XIRR Engine
*File: `common_util.py`*
Standard Excel `XIRR` often fails on the complex, irregular payment schedules of Indian NCDs. This engine implements a robust vectorized solution:
* **Algorithm:** Uses `scipy.optimize.newton` (Newton-Raphson) and `brentq` for root-finding to solve the non-linear XNPV equation.
* **Irregular Dates:** Accurately handles precise day-count conventions between uneven cashflow dates.
* **Complex Structures:** Natively handles **Zero-Coupon Bonds** (calculating implied IRR from issue price vs. maturity) and **Partial Redemption (Amortizing) Bonds** where face value reduces over time.

#### 3. Market Scheduler
*File: `bond_trade_data_scheduler.py`*
An intelligent scheduler that manages the data fetching lifecycle to respect broker API limits while capturing market moves.
* **Market Window:** Automatically starts scanning at **09:15 IST** and performs a graceful shutdown at **15:30 IST**.
* **Interval Management:** Runs distinct fetch cycles every **15 minutes**.
* **Drift Correction:** Calculates the exact `sleep` time required to align with the next wall-clock interval, correcting for execution latency.

---

### ⚙️ Installation & Setup

1.  **Clone the Repository**
    ```bash
    git clone [https://github.com/maagalamharsha/indianncdbonds.git](https://github.com/maagalamharsha/indianncdbonds.git)
    cd indianncdbonds
    ```

2.  **Install Dependencies**
    *Note: This project requires `kiteconnect`, `scipy`, and `openai` in addition to the base requirements.*
    ```bash
    pip install -r requirements.txt
    pip install kiteconnect scipy openai python-dotenv flask picologging
    ```

3.  **Environment Configuration**
    Create a `.env` file in the root directory to store your credentials securely (do not commit this file):
    ```ini
    # Broker Credentials
    KITE_API_KEY=your_kite_api_key
    KITE_API_SECRET=your_kite_api_secret
    
    # Database (PostgreSQL)
    DATABASE_URL=postgresql+psycopg2://username:password@localhost/production
    
    # AI Services
    OPENAI_API_KEY=your_openai_key_here
    ```

4.  **Database Initialization**
    Ensure your local PostgreSQL instance is running. The scripts assume a database named `production`.
    * The schema tables (`bond_metadata_prod`, `bond_cashflows`, `security_ids`) will be auto-generated by SQLAlchemy on the first run of the ingestion scripts.

5.  **Run the System**
    * **Step 1: Ingest Metadata:** `python bond_meta_cache_by_isin.py`
    * **Step 2: Start Scheduler:** `python bond_trade_data_scheduler.py`
    * **Step 3: Launch Dashboard:** `streamlit run streamline_app.py`

---

### ⚖️ Disclaimer
This software is for **educational and research purposes only**. It involves automated financial calculations that may contain errors. The calculated yields (XIRR) and risk metrics should **not** be used as the sole basis for real-world investment decisions. The author is not responsible for any financial losses incurred from the use of this code or algorithmic trading errors.

---

### 👤 Author
**Harsha Vardhan Maagalam**
*Quant Developer & Risk Application Developer*
[LinkedIn](https://www.linkedin.com/in/mharsha-vardhan/) | [Email](mailto:maagalamharsha@gmail.com)
