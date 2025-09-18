import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

DB_PATH = Path("db/enron.db")

def q(sql, params=None):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(sql, conn, params=params or {})

pd.set_option("display.max_colwidth", 120)

# 1) Daily volume
daily = q("""
  SELECT date(sent_utc) AS day, COUNT(*) AS n
  FROM emails
  WHERE sent_utc IS NOT NULL
  GROUP BY day
  ORDER BY day
""")
if not daily.empty:
    daily['day'] = pd.to_datetime(daily['day'])
    plt.figure(figsize=(12,4))
    plt.plot(daily['day'], daily['n'])
    plt.title("Emails per day"); plt.xlabel("Day"); plt.ylabel("# Emails"); plt.grid(alpha=0.3)
    plt.tight_layout(); plt.show()
print(daily.head())

# 2) Top senders
top_senders = q("""
  SELECT COALESCE(p.display_name, p.email) AS sender, COUNT(*) AS n
  FROM emails e JOIN persons p ON e.from_person_id = p.person_id
  GROUP BY sender
  ORDER BY n DESC
  LIMIT 25
""")
print(top_senders)

# 3) Simple keyword hits
term = "%contract%"
hits = q("""
  SELECT email_id, sent_utc, subject,
         substr(body, max(1, instr(lower(body), lower(:t)) - 40), 160) AS context
  FROM emails
  WHERE subject LIKE :t OR body LIKE :t
  ORDER BY sent_utc
  LIMIT 50
""", params={'t': term})
print(hits.head())
