-- Migration 002: Upgrade to Compound Indexes and Refactor 'event_tags' to 'tags'
-- Target DB Version: 2

BEGIN TRANSACTION;

-- 1. EVENTS TABLE INDEX CHANGES

-- Drop old, single-column indexes (they are no longer needed and are inefficient)
DROP INDEX IF EXISTS pubkey_idx;
DROP INDEX IF EXISTS time_idx;
DROP INDEX IF EXISTS kind_idx;

-- Create new, compound, sort-aware indexes on the 'events' table
CREATE INDEX IF NOT EXISTS time_idx ON events(created_at DESC, id ASC);
CREATE INDEX IF NOT EXISTS kind_sorted_idx ON events(kind, created_at DESC, id ASC);
CREATE INDEX IF NOT EXISTS pubkey_kind_sorted_idx ON events(pubkey, kind, created_at DESC, id ASC);

-- 2. TAGS TABLE REBUILD (Rename table and reorder Primary Key)

-- Create the new 'tags' table with the optimized structure:
-- Primary Key (key, value, event_id) acts as a covering index for tag lookups.
CREATE TABLE IF NOT EXISTS tags (
event_id TEXT NOT NULL,
key TEXT NOT NULL,
value TEXT NOT NULL,

PRIMARY KEY (key, value, event_id),
FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
);

-- Copy data from the old table into the new table
INSERT INTO tags (event_id, key, value)
SELECT event_id, key, value FROM event_tags;

-- Drop the old table
DROP TABLE event_tags;

-- Create the required index to support event lookups and the FOREIGN KEY ON DELETE CASCADE
CREATE INDEX IF NOT EXISTS tags_event_id_idx ON tags(event_id);

-- 3. TRIGGER AND VERSION UPDATE

-- The old trigger references the old table name ('event_tags'), so it must be dropped.
DROP TRIGGER IF EXISTS d_tags_ai;

-- Recreate the trigger, now targeting the new 'tags' table name
CREATE TRIGGER IF NOT EXISTS d_tags_ai AFTER INSERT ON events
WHEN NEW.kind BETWEEN 30000 AND 39999
BEGIN
INSERT INTO tags (event_id, key, value)
SELECT NEW.id, 'd', json_extract(value, '$[1]')
FROM json_each(NEW.tags)
WHERE json_type(value) = 'array' AND json_array_length(value) > 1 AND json_extract(value, '$[0]') = 'd'
LIMIT 1;
END;

-- Final Step: Update the schema version number to 2
PRAGMA user_version = 2;
COMMIT;