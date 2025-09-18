DROP VIEW IF EXISTS emails_flat;
CREATE VIEW emails_flat AS
SELECT
  e.email_id, e.sent_utc, e.year, e.month, e.day, e.hour,
  e.subject,
  (SELECT email FROM persons WHERE person_id=e.from_person_id) AS from_email,
  (SELECT display_name FROM persons WHERE person_id=e.from_person_id) AS from_name,
  (SELECT GROUP_CONCAT(p.email)
   FROM email_recipients er JOIN persons p ON er.person_id=p.person_id
   WHERE er.email_id=e.email_id AND er.recipient_type='to') AS to_emails,
  (SELECT GROUP_CONCAT(p.email)
   FROM email_recipients er JOIN persons p ON er.person_id=p.person_id
   WHERE er.email_id=e.email_id AND er.recipient_type='cc') AS cc_emails
FROM emails e;
