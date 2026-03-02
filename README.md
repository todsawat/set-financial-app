# SET Financial Analyzer

วิเคราะห์งบการเงินหุ้นไทยจาก SET.or.th แบบอัตโนมัติ — ดาวน์โหลด, วิเคราะห์, และเปรียบเทียบข้อมูลหลายปี/ไตรมาส พร้อม Interactive Charts

**[>>> เปิดใช้งาน <<<](https://set-financial-analyzer.streamlit.app/)**

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
- **Hosting:** [Streamlit Community Cloud](https://streamlit.io/cloud)
- **Units:** พันบาท (Thousands THB)

## Development

```bash
# ติดตั้ง dependencies
pip install -r requirements.txt

# รัน
streamlit run app.py
```

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
```

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
├── app.py                  # Streamlit app หลัก (UI + charts)
├── financial_data.py       # Data processing & transformation
├── set_scraper.py          # SET.or.th API scraper + XLSX parser
├── version.py              # Version number
├── requirements.txt        # Python dependencies
├── test_accuracy.py        # Exhaustive accuracy test (220 symbols)
├── test_200.py             # Parallel accuracy test (200 symbols)
└── test_data_quality.py    # Data quality & sanity checks
```

## Data Source

ข้อมูลทั้งหมดดึงจาก [SET.or.th](https://www.set.or.th) ผ่าน internal API:

1. **Company Highlight API** — ข้อมูลสรุปรายปี/ไตรมาส
2. **Factsheet API** — งบการเงิน, อัตราส่วน, การเติบโต
3. **News API** — ดาวน์โหลด ZIP ที่มี FINANCIAL_STATEMENTS.XLSX
4. **XLSX Parser** — แยกข้อมูลจาก Excel (รองรับทั้ง .xlsx และ .xls)
