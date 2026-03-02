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

## Project Structure

```
set-financial-app/
├── app.py                         # Streamlit app หลัก (UI + charts)
├── financial_data.py              # Data processing & transformation
├── set_scraper.py                 # SET.or.th API scraper + XLSX parser
├── version.py                     # Version number (auto-synced with git tag)
├── launcher.py                    # Desktop app launcher (PyInstaller wrapper)
├── requirements.txt               # Python dependencies
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
