SELECT COUNT(*) AS emails FROM emails;
SELECT COUNT(*) AS persons FROM persons;
SELECT COUNT(*) AS domains FROM domains;

SELECT COUNT(*) AS with_date FROM emails WHERE sent_utc IS NOT NULL;
SELECT SUM(year IS NOT NULL) AS year_ok,
       SUM(month IS NOT NULL) AS month_ok,
       SUM(day IS NOT NULL) AS day_ok,
       SUM(hour IS NOT NULL) AS hour_ok
FROM emails;

SELECT body_sha256, COUNT(*) c
FROM emails
GROUP BY body_sha256
HAVING c>1
ORDER BY c DESC
LIMIT 20;

SELECT COUNT(*) AS named_persons
FROM persons
WHERE display_name IS NOT NULL AND display_name <> '';
