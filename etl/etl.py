import re
import csv
import html
import sqlite3
import hashlib
import pandas as pd
from datetime import timezone
from email import message_from_string
from email.policy import compat32
from email.utils import getaddresses, parsedate_to_datetime
from email.header import decode_header, make_header

from pathlib import Path
from .config import CSV_PATH, CSV_HAS_HEADER, CHUNK_SIZE, DB_PATH

# CSV field size fix
try:
    csv.field_size_limit(2_147_483_647)
except OverflowError:
    limit = 2_147_483_647
    while True:
        try:
            csv.field_size_limit(limit)
            break
        except OverflowError:
            limit //= 10

CREATE_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;
PRAGMA foreign_keys=ON;

DROP TABLE IF EXISTS email_recipients;
DROP TABLE IF EXISTS emails;
DROP TABLE IF EXISTS persons;
DROP TABLE IF EXISTS domains;

CREATE TABLE persons (
    person_id INTEGER PRIMARY KEY,
    display_name TEXT,
    email TEXT NOT NULL UNIQUE
);

CREATE TABLE domains (
    domain_id INTEGER PRIMARY KEY,
    domain TEXT NOT NULL UNIQUE
);

CREATE TABLE emails (
    email_id INTEGER PRIMARY KEY,
    message_id TEXT UNIQUE,
    from_person_id INTEGER,
    from_domain_id INTEGER,
    sent_utc TEXT,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    subject TEXT,
    body TEXT,
    body_sha256 TEXT,
    in_reply_to TEXT,
    msg_references TEXT,
    folder TEXT,
    is_auto BOOLEAN DEFAULT 0,
    is_internal BOOLEAN,
    FOREIGN KEY (from_person_id) REFERENCES persons(person_id),
    FOREIGN KEY (from_domain_id) REFERENCES domains(domain_id)
);

CREATE TABLE email_recipients (
    email_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    domain_id INTEGER,
    recipient_type TEXT CHECK (recipient_type IN ('to','cc','bcc')),
    PRIMARY KEY (email_id, person_id, recipient_type),
    FOREIGN KEY (email_id) REFERENCES emails(email_id),
    FOREIGN KEY (person_id) REFERENCES persons(person_id),
    FOREIGN KEY (domain_id) REFERENCES domains(domain_id)
);

CREATE INDEX IF NOT EXISTS idx_emails_sent_utc ON emails(sent_utc);
CREATE INDEX IF NOT EXISTS idx_emails_from_person ON emails(from_person_id);
CREATE INDEX IF NOT EXISTS idx_emails_from_domain ON emails(from_domain_id);
CREATE INDEX IF NOT EXISTS idx_recip_person ON email_recipients(person_id);
CREATE INDEX IF NOT EXISTS idx_recip_email ON email_recipients(email_id);
CREATE INDEX IF NOT EXISTS idx_emails_body_hash ON emails(body_sha256);
"""

def ensure_schema(conn: sqlite3.Connection):
    conn.executescript(CREATE_SQL)

def email_domain(addr: str | None) -> str | None:
    if not addr or "@" not in addr:
        return None
    return addr.split("@", 1)[1].lower().strip()

_X500_PAT = re.compile(r"""
    (?:<\s*)?                # optional '<'
    (?:/)?O=[^/>]+           # /O=Org
    (?:/(?:OU|CN|GQ|DD)=[^/>]+)+  # /OU=... /CN=... repeated segments
    (?:\s*>)?                # optional '>'
    """, re.IGNORECASE | re.VERBOSE)

ANGLE_BLOCK_PAT = re.compile(r"<[^>]*>")
MULTISPACE_PAT = re.compile(r"\s+")
TRAILING_PUNCT_PAT = re.compile(r'^["'(\[\s]+|["')\]\s]+$')

def _smart_title(word: str) -> str:
    if len(word) >= 3 and word.isupper():
        return word
    parts = re.split(r"([-'])", word)
    parts = [p.capitalize() if p.isalpha() else p for p in parts]
    return "".join(parts)

def _looks_like_email(text: str) -> bool:
    return "@" in text

def _normalize_commas(name: str) -> str:
    if "," not in name:
        return name
    if any(tok in name.lower() for tok in ["inc", "ltd", "dept", "department", "company"]):
        return name
    last, rest = [x.strip() for x in name.split(",", 1)]
    if not last or not rest:
        return name
    if any(tok in rest.lower() for tok in ["inc", "ltd", "dept", "department", "company"]):
        return name
    return f"{rest} {last}".strip()

def derive_name_from_email(email_addr: str | None) -> str | None:
    if not email_addr or "@" not in email_addr:
        return None
    local = email_addr.split("@", 1)[0]
    import re as _re
    local = _re.sub(r"[._\-+]+", " ", local)
    local = _re.sub(r"\s+", " ", local).strip()
    if not local:
        return None
    parts = [_smart_title(p) for p in local.split(" ")]
    candidate = " ".join(parts)
    letters = sum(ch.isalpha() for ch in candidate)
    return candidate if letters >= 2 else None

def normalize_display_name(raw_name: str | None, email_addr: str | None = None) -> str | None:
    if not raw_name:
        return None
    try:
        decoded = str(make_header(decode_header(raw_name))).strip()
    except Exception:
        decoded = str(raw_name).strip()
    import html as _html
    decoded = _html.unescape(decoded)
    if not decoded:
        return None
    cleaned = _X500_PAT.sub("", decoded)
    cleaned = ANGLE_BLOCK_PAT.sub("", cleaned)
    cleaned = cleaned.strip(" \t\r\n\"'()[]")
    cleaned = TRAILING_PUNCT_PAT.sub("", cleaned)
    cleaned = MULTISPACE_PAT.sub(" ", cleaned)
    if not cleaned or _looks_like_email(cleaned):
        return None
    cleaned = _normalize_commas(cleaned)
    tokens = cleaned.split(" ")
    tokens = [_smart_title(t) for t in tokens if t]
    cleaned = " ".join(tokens)
    if (not cleaned or len(cleaned) < 2) and email_addr:
        return derive_name_from_email(email_addr)
    letters = sum(ch.isalpha() for ch in cleaned)
    if letters < 2:
        return None
    return cleaned

def parse_addresses(header_value: str | None):
    if not header_value:
        return []
    pairs = getaddresses([header_value])
    items = []
    for raw_name, email in pairs:
        email = (email or "").strip().lower()
        if not email:
            continue
        name_clean = normalize_display_name(raw_name, email)
        if not name_clean:
            name_clean = derive_name_from_email(email)
        items.append((name_clean, email))
    return items

def extract_body(msg) -> str:
    try:
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_disposition() == 'attachment':
                    continue
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True) or b""
                    return payload.decode(errors="replace")
            for part in msg.walk():
                if part.get_content_disposition() == 'attachment':
                    continue
                payload = part.get_payload(decode=True) or b""
                return payload.decode(errors="replace")
            return ""
        else:
            payload = msg.get_payload(decode=True) or b""
            return payload.decode(errors="replace")
    except Exception:
        return ""

def parse_email(raw: str) -> dict:
    try:
        msg = message_from_string(raw, policy=compat32)
    except Exception:
        return {}
    date_hdr = msg.get("Date", "")
    sent_iso, y, m, d, h = None, None, None, None, None
    if date_hdr:
        try:
            dt = parsedate_to_datetime(date_hdr)
            if dt and dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt_utc = dt.astimezone(timezone.utc)
            sent_iso = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            y, m, d, h = dt_utc.year, dt_utc.month, dt_utc.day, dt_utc.hour
        except Exception:
            pass
    return {
        "message_id": (msg.get("Message-ID", "") or msg.get("Message-Id", "")).strip(),
        "from": msg.get("From", ""),
        "to": msg.get("To", ""),
        "cc": msg.get("Cc", ""),
        "bcc": msg.get("Bcc", ""),
        "subject": msg.get("Subject", ""),
        "date": sent_iso,
        "year": y, "month": m, "day": d, "hour": h,
        "in_reply_to": msg.get("In-Reply-To", ""),
        "references": msg.get("References", ""),
        "x_from": msg.get("X-From", ""),
        "x_to": msg.get("X-To", ""),
        "x_cc": msg.get("X-cc", ""),
        "x_bcc": msg.get("X-bcc", ""),
        "body": extract_body(msg),
    }

def upsert_domain(conn: sqlite3.Connection, domain: str | None) -> int | None:
    if not domain:
        return None
    cur = conn.execute("""
        INSERT INTO domains(domain)
        VALUES (?)
        ON CONFLICT(domain) DO NOTHING
        RETURNING domain_id;
    """, (domain,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur = conn.execute("SELECT domain_id FROM domains WHERE domain=?", (domain,))
    r = cur.fetchone()
    return r[0] if r else None

def upsert_person(conn: sqlite3.Connection, email: str | None, display_name: str | None = None) -> int | None:
    if not email:
        return None
    if display_name and (("<" in display_name or ">" in display_name) or _X500_PAT.search(display_name)):
        display_name = None
    cur = conn.execute("""
        INSERT INTO persons(email, display_name)
        VALUES (?, ?)
        ON CONFLICT(email) DO UPDATE SET
            display_name = CASE
                WHEN (excluded.display_name IS NOT NULL AND excluded.display_name <> '')
                     AND (
                         persons.display_name IS NULL OR persons.display_name = ''
                         OR persons.display_name LIKE '%/O=%'
                         OR persons.display_name LIKE '%CN=%'
                         OR persons.display_name LIKE '%<%'
                         OR persons.display_name LIKE '%>%'
                         OR persons.display_name LIKE '%@%'
                         OR length(excluded.display_name) > length(persons.display_name)
                     )
                THEN excluded.display_name
                ELSE persons.display_name
            END
        RETURNING person_id;
    """, (email, (display_name or None)))
    row = cur.fetchone()
    if row:
        return row[0]
    cur = conn.execute("SELECT person_id FROM persons WHERE email=?", (email,))
    r = cur.fetchone()
    return r[0] if r else None

def insert_email(conn: sqlite3.Connection, row, parsed: dict,
                 from_person_id: int | None, from_domain_id: int | None) -> int | None:
    body_hash = hashlib.sha256((parsed.get("body") or "").encode()).hexdigest()
    msgid = (parsed.get("message_id") or body_hash)
    cur = conn.execute("""
        INSERT INTO emails(
            message_id, from_person_id, from_domain_id, sent_utc,
            year, month, day, hour, subject, body, body_sha256,
            in_reply_to, msg_references, folder, is_auto, is_internal
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(message_id) DO NOTHING
        RETURNING email_id;
    """, (
        msgid,
        from_person_id,
        from_domain_id,
        parsed.get("date"),
        parsed.get("year"), parsed.get("month"), parsed.get("day"), parsed.get("hour"),
        parsed.get("subject"),
        parsed.get("body"),
        body_hash,
        parsed.get("in_reply_to"),
        parsed.get("references"),
        getattr(row, "get", lambda *_: None)("file"),
        0,
        0
    ))
    res = cur.fetchone()
    if res:
        return res[0]
    cur = conn.execute("SELECT email_id FROM emails WHERE message_id=?", (msgid,))
    r = cur.fetchone()
    return r[0] if r else None

def insert_recipients(conn: sqlite3.Connection, email_id: int,
                      recipients: str | None, rtype: str):
    if not recipients:
        return
    for display_name, addr in parse_addresses(recipients):
        pid = upsert_person(conn, addr, display_name)
        did = upsert_domain(conn, email_domain(addr))
        conn.execute("""
            INSERT OR IGNORE INTO email_recipients(email_id, person_id, domain_id, recipient_type)
            VALUES (?,?,?,?)
        """, (email_id, pid, did, rtype))

def run_etl():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys=ON;")
        ensure_schema(conn)

        processed = 0
        inserted = 0

        if CSV_HAS_HEADER:
            chunk_iter = pd.read_csv(
                CSV_PATH,
                chunksize=CHUNK_SIZE,
                engine="python",
                sep=None,
                usecols=[1]
            )
        else:
            chunk_iter = pd.read_csv(
                CSV_PATH,
                chunksize=CHUNK_SIZE,
                engine="python",
                sep=None,
                header=None,
                usecols=[1],
                names=["message"]
            )

        for chunk in chunk_iter:
            if CSV_HAS_HEADER:
                chunk.columns = ["message"]

            for _, r in chunk.iterrows():
                parsed = parse_email(r["message"])
                if not parsed:
                    continue

                sender_pairs = parse_addresses(parsed.get("from"))
                sender_email = sender_pairs[0][1] if sender_pairs else None
                sender_name  = sender_pairs[0][0] if sender_pairs else None

                if sender_email and (not sender_name or not sender_name.strip()):
                    x_from = parsed.get("x_from")
                    if x_from and '@' not in x_from:
                        sender_name = normalize_display_name(x_from, sender_email)
                    if not sender_name:
                        sender_name = derive_name_from_email(sender_email)

                from_person_id = upsert_person(conn, sender_email, sender_name) if sender_email else None
                from_domain_id = upsert_domain(conn, email_domain(sender_email)) if sender_email else None

                eid = insert_email(conn, r, parsed, from_person_id, from_domain_id)
                if not eid:
                    continue

                insert_recipients(conn, eid, parsed.get("to"), "to")
                insert_recipients(conn, eid, parsed.get("cc"), "cc")
                insert_recipients(conn, eid, parsed.get("bcc"), "bcc")

                inserted += 1
                processed += 1

            conn.commit()
            print(f"Processed rows: {processed:,} | Inserted emails: {inserted:,}")

        print(f"ETL complete. Rows processed: {processed:,} | Emails inserted: {inserted:,}")

if __name__ == "__main__":
    run_etl()
