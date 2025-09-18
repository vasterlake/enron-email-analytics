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
