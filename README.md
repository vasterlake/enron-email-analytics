# Enron Email Analytics (ETL + Analysis)

A robust ETL pipeline that ingests raw RFC‑822 email messages from a CSV into SQLite, cleans person names, parses timestamps to UTC, and provides a starter analysis toolkit (daily trends, top senders, keyword hits).

## Why this exists

Email datasets—especially historical corpora—are messy:
- Headers can be malformed or missing (`Message-ID`, `Date`, names).
- Display names often contain Exchange/X.500 **legacy DNs** (e.g., `</O=ENRON/OU=.../CN=...>` or HTML-escaped `&lt;...&gt;`).
- Names may appear as **`Surname, Name`** or **`Name Surname`** inconsistently.
- CSVs can be huge and irregular (embedded newlines, different delimiters).

This project solves those pain points and gives you a clean SQLite database for analysis.

---

## Key Features

- **CSV handling**
  - **Reads only the 2nd column** of the CSV as the raw email `message` (ignores the first).
  - Works whether the CSV has a header or not (`CSV_HAS_HEADER` flag).
  - **Delimiter auto-detection** (`engine="python", sep=None`) for commas, semicolons, tabs, etc.
  - Chunked loading for very large files.

- **Email parsing**
  - Extracts `From`, `To`, `Cc`, `Bcc`, `Subject`, `Date`, `In-Reply-To`, `References`, and the **plain-text body** (prefers `text/plain`, safe fallback when HTML-only).
  - Converts `Date` to **UTC ISO** and populates `year`, `month`, `day`, `hour` for time-series analysis.
  - **Guarantees `message_id` uniqueness** with a fallback to `body_sha256` when `Message-ID` is missing (dedup friendly).

- **Display name cleanup**
  - Decodes RFC‑2047 encoded names (e.g., `=?utf-8?...?=`).
  - **Removes Exchange/X.500 legacy DNs** and angle blocks.
  - Normalizes **`Surname, Name` → `Name Surname`**.
  - Fallback to a **derived name from the email local-part** (`john_smith` → `John Smith`) when needed.
  - Writes names only when they’re better than existing values (avoids overwriting good names with junk).

- **Relational model**
  - `emails`, `persons`, `domains`, `email_recipients` with practical indexes.
  - Optional **convenience view** `emails_flat` (sender + grouped recipients).

- **Starter analysis**
  - Daily trend chart, top senders table, simple keyword hits.

---

## Repository Layout
enron-email-analytics/ ├─ README.md ├─ LICENSE ├─ .gitignore ├─ requirements.txt ├─ etl/ │ ├─ init.py │ ├─ etl.py # robust ETL (uses CSV column 2 as message) │ └─ config.py # CSV path, header flag, chunk size, DB path ├─ sql/ │ ├─ schema.sql │ ├─ sanity_checks.sql │ └─ views.sql # creates emails_flat view ├─ analysis/ │ └─ analysis_starter.py # daily trend, top senders, keyword hits ├─ data/ │ └─ raw/ # put your CSV here (ignored by git) ├─ db/ │ └─ enron.db # generated SQLite DB (ignored by git) └─ .github/ └─ workflows/ci.yml # simple CI smoke check 
---

## Quickstart

### 1) Python environment

```bash
python -m venv .venv
# Windows PowerShell:
. .venv/Scripts/Activate.ps1
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
