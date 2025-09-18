from pathlib import Path

# --- Paths / flags (edit these) ---
CSV_PATH = Path("data/raw/emails.csv")  # <-- your CSV path inside repo
CSV_HAS_HEADER = True     # Set to False if your CSV has NO header row
CHUNK_SIZE = 50_000
DB_PATH = Path("db/enron.db")

# If your CSV is not comma-separated, the ETL auto-detects the delimiter (engine="python", sep=None).
