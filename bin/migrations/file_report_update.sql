ALTER TABLE filereport ADD COLUMN requested_bytes DEFAULT 0;
UPDATE filereport SET requested_bytes = streamed_bytes;
