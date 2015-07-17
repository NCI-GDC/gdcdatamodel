ALTER TABLE filereport ADD COLUMN requested_bytes bigint DEFAULT 0;
UPDATE filereport SET requested_bytes = streamed_bytes;
