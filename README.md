# SET Financial Analyzer

วิเคราะห์งบการเงินหุ้นไทยจาก SET.or.th แบบอัตโนมัติ — ดาวน์โหลด, วิเคราะห์, และเปรียบเทียบข้อมูลหลายปี/ไตรมาส พร้อม Interactive Charts

## Features

### 6 แท็บวิเคราะห์

| แท็บ | รายละเอียด |
|------|-----------|
| **Overview Charts** | Dashboard รวมกราฟสำคัญทั้งหมดในหน้าเดียว |
| **Income Statement** | งบกำไรขาดทุน + กราฟ Revenue, EBITDA, Net Profit |
| **Balance Sheet** | งบฐานะการเงินแบบ hierarchical + กราฟ Assets/Liabilities/Equity |
| **Financial Ratios** | อัตราส่วนทางการเงิน (Margin, ROE, ROA, D/E, Current Ratio) |
| **Core Profit Analysis** | แยก Core Profit vs รายการพิเศษ พร้อมเลือกรายการได้แบบ interactive |
| **Cash Flow** | งบกระแสเงินสด (Operating/Investing/Financing) + trend chart |

### Charts (11+ กราฟ)

- Revenue & Profit (Bar + Line)
- YoY / QoQ Growth %
- Margin Trends (Gross, Net, Core)
- ROE & ROA
- D/E Ratio
- Tax Rate (Dual-axis)
- Finance Cost Analysis
- Core Profit vs Reported Net Income
- Balance Sheet Composition (Stacked Bar)
- Cash & Inventories Breakdown
- Cash Flow Trends

### Financial Metrics

- **Margins:** Gross, EBIT, EBITDA, Net, Core Profit Margin
- **Returns:** ROE, ROA (annualized สำหรับ quarterly)
- **Leverage:** D/E Ratio, Current Ratio, Quick Ratio
- **Growth:** Revenue, Net Profit, Core Profit (YoY / QoQ)
- **Tax:** Effective Tax Rate
- **Per Share:** EPS

### Core Profit Engine

วิเคราะห์แยก **กำไรจากธุรกิจหลัก** ออกจาก **รายการพิเศษ** โดยอัตโนมัติ:

- กำไรจากการขายสินทรัพย์
- กำไร/ขาดทุนจาก FV ของเงินลงทุน
- ส่วนแบ่งกำไรจากบริษัทร่วม/JV
- รายได้ดอกเบี้ย / เงินปันผลรับ
- กำไร/ขาดทุนจากอัตราแลกเปลี่ยน
- กำไร/ขาดทุนจากตราสารอนุพันธ์

ผู้ใช้สามารถเลือก/ไม่เลือกรายการแต่ละตัวได้ — กราฟและตัวเลขจะอัพเดทแบบ real-time

## Tech Stack

- **Framework:** [Streamlit](https://streamlit.io/)
- **Charts:** [Plotly](https://plotly.com/python/)
- **Data:** SET.or.th API + XLSX financial statements
- **Units:** พันบาท (Thousands THB)

## Quick Start

### รันแบบ Development

```bash
# ติดตั้ง dependencies
pip install -r requirements.txt

# รัน
streamlit run app.py
```

### รันแบบ Desktop App

ดาวน์โหลด `.app` (macOS) หรือ `.exe` (Windows) จาก [Releases](../../releases/latest):

| Platform | ไฟล์ |
|----------|------|
| macOS (Apple Silicon M1/M2/M3) | `SET-Financial-Analyzer-macOS-arm64.zip` |
| macOS (Intel) | `SET-Financial-Analyzer-macOS-x64.zip` |
| Windows | `SET-Financial-Analyzer-Windows-x64.zip` |

**วิธีใช้:**
1. ดาวน์โหลด ZIP สำหรับ platform ของคุณ
2. แตกไฟล์ ZIP
3. **macOS:** ดับเบิ้ลคลิก `SET-Financial-Analyzer.app`
   - ถ้าถูก Gatekeeper บล็อก: คลิกขวา > Open > Open
4. **Windows:** ดับเบิ้ลคลิก `SET-Financial-Analyzer.exe`
   - ถ้าถูก SmartScreen บล็อก: กด "More info" > "Run anyway"
5. แอปจะเปิดใน browser อัตโนมัติ

## Build จาก Source

### Build ในเครื่อง (macOS)

```bash
pip install pyinstaller
./build_app.sh              # Build สำหรับ arch ปัจจุบัน
./build_app.sh universal2   # Build Universal (Intel + Apple Silicon)
./build_app.sh clean        # ลบ build artifacts
```

### Build ผ่าน CI/CD (ทุก platform)

Push tag เพื่อ trigger GitHub Actions build อัตโนมัติ:

```bash
git tag v1.1.0
git push origin v1.1.0
```

CI/CD จะ build `.app` (Intel + Apple Silicon) และ `.exe` (Windows) แล้วสร้าง GitHub Release พร้อมไฟล์ดาวน์โหลดอัตโนมัติ

## Testing

ระบบ test ตรวจสอบความถูกต้องของข้อมูลแบบ 3 ชั้น — เทียบกับ API, เทียบกับ XLSX ต้นฉบับ, และตรวจสอบความสมเหตุสมผลทางบัญชี

### ไฟล์ Test

| ไฟล์ | จุดประสงค์ | Symbols | วิธีรัน |
|------|-----------|---------|--------|
| `test_accuracy.py` | ตรวจความถูกต้องแบบละเอียด เทียบ App vs API vs XLSX | 220 ตัว | Sequential (live API) |
| `test_200.py` | ตรวจความถูกต้องแบบ parallel สำหรับ 200 บริษัท | ~200 ตัว | Parallel (8 threads, cache only) |
| `test_data_quality.py` | ตรวจคุณภาพข้อมูล ความสมเหตุสมผล ความครบถ้วน | ทุกตัวที่มี cache | Sequential (cache only) |

### วิธีรัน

```bash
# Accuracy test — ตรวจทุก symbol (ใช้เวลานาน ต้องมี internet)
python3 test_accuracy.py

# Accuracy test — เฉพาะ 200 symbols ใหม่
python3 test_accuracy.py --new-only

# Parallel test — ต้องมี /tmp/symbols_200.json และ cache files
python3 test_200.py

# Data quality test — ทุก symbol ที่มี cache
python3 test_data_quality.py

# Data quality test — เฉพาะบางตัว
python3 test_data_quality.py AOT CPALL PTT

# Data quality test — แบบ batch (ทีละ 20 ตัว)
python3 test_data_quality.py --batch 1
```

### สิ่งที่ Test ตรวจสอบ

#### 1. `test_accuracy.py` — Exhaustive Accuracy Test

ตรวจทุกตัวเลขที่แสดงในแอป โดยเทียบกับ 3 แหล่งข้อมูล:

**Annual (เทียบกับ API):**
- Income Statement: Revenue, Sales, Expense, EBIT, EBITDA, Net Profit, EPS
- Balance Sheet: Total Assets, Total Liabilities, Equity, สมการ A = L + E
- Ratios: Gross/Net/Core Margin, ROE, ROA, D/E, Current Ratio, Quick Ratio
- Core Profit: สูตร `Core = NP - PFO × (1 - tax_rate)`
- Cash Flow: Operating, Investing, Financing, Net

**Quarterly (เทียบกับ XLSX ต้นฉบับ):**
- เทียบทุกรายการกับ XLSX โดยตรง (Q1/Q2/Q3)
- ตรวจ Q4 = FY - Q1 - Q2 - Q3
- จัดการ Cumulative Cash Flow (Q2 = cum6M - Q1, Q3 = cum9M - cum6M)
- ตรวจสูตร EBITDA = Operating Profit + Depreciation

**Cross-mode (Annual vs Quarterly):**
- ผลรวม Q1-Q4 ต้องตรงกับตัวเลขรายปี (Revenue, Net Profit, Cash Flow, Core Profit)

**Output:** `test_results.json`

#### 2. `test_200.py` — Parallel Scale Test

Logic เดียวกับ `test_accuracy.py` แต่รันแบบ parallel 8 threads สำหรับ 200 บริษัท:

- ใช้ cache เท่านั้น (ไม่เรียก API ใหม่ — เร็วมาก)
- ต้องมี `/tmp/symbols_200.json` (list ของ symbols)
- ข้าม symbol ที่ยังไม่มี cache

**Output:** `test_200_results.json`

#### 3. `test_data_quality.py` — Data Quality & Sanity

ตรวจว่าข้อมูลสมเหตุสมผลทางบัญชีและการเงิน:

- **Completeness** — ข้อมูลครบทุก field ที่จำเป็นหรือไม่ (>50% ของ periods)
- **XLSX vs Processed** — ค่าที่ประมวลผลแล้วตรงกับ XLSX ต้นฉบับมั้ย
- **Sanity (Quarterly)** — Revenue > 0, NP ไม่เกิน 2x Revenue, A = L + E, D/E >= 0, Tax Rate 0-60%
- **Sanity (Annual)** — Revenue YoY ไม่กระโดดเกิน 10 เท่า, Finance Cost ไม่หายไปกะทันหัน
- **Consistency** — ผลรวมรายไตรมาสตรงกับรายปี

มีระบบ **Curated Symbols** (20 ตัวหลัก) ที่ใช้เกณฑ์เข้มงวด (FAIL) ส่วนตัวอื่นใช้เกณฑ์ผ่อนปรน (WARN) เพราะอาจมีปัญหา NCI หรือ parent-only XLSX

**Output:** `test_quality_results.json`

### Tolerance Levels

| การตรวจสอบ | Tolerance | หมายเหตุ |
|-----------|-----------|---------|
| Income Statement vs API/XLSX | 1-2% | ค่าเริ่มต้น |
| EPS | 2% | ผ่อนปรนกว่าเพราะมี rounding |
| Core Profit formula | 3% | มี tax rate estimation |
| Balance Sheet A = L + E | 2-5% | กว้างขึ้นเพราะ NCI |
| Ratios (Margin, ROE, ROA) | 1-2% | |
| Cross-mode (Q sum vs Annual) | 2-5% | parent vs consolidated |
| Data Quality XLSX match | 2% PASS, 10% WARN | >10% = FAIL |

## Project Structure

```
set-financial-app/
├── app.py                         # Streamlit app หลัก (UI + charts)
├── financial_data.py              # Data processing & transformation
├── set_scraper.py                 # SET.or.th API scraper + XLSX parser
├── version.py                     # Version number (auto-synced with git tag)
├── launcher.py                    # Desktop app launcher (PyInstaller wrapper)
├── requirements.txt               # Python dependencies
├── test_accuracy.py               # Exhaustive accuracy test (220 symbols)
├── test_200.py                    # Parallel accuracy test (200 symbols)
├── test_data_quality.py           # Data quality & sanity checks
├── SET-Financial-Analyzer.spec    # PyInstaller build config
├── build_app.sh                   # Local build script
└── .github/workflows/build.yml   # CI/CD multi-platform build
```

## Data Source

ข้อมูลทั้งหมดดึงจาก [SET.or.th](https://www.set.or.th) ผ่าน internal API:

1. **Company Highlight API** — ข้อมูลสรุปรายปี/ไตรมาส
2. **Factsheet API** — งบการเงิน, อัตราส่วน, การเติบโต
3. **News API** — ดาวน์โหลด ZIP ที่มี FINANCIAL_STATEMENTS.XLSX
4. **XLSX Parser** — แยกข้อมูลจาก Excel (รองรับทั้ง .xlsx และ .xls)
