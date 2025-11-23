CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    pubkey TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    kind INTEGER NOT NULL,
    tags JSONB NOT NULL,
    content TEXT NOT NULL,
    sig TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS time_idx ON events(created_at DESC, id ASC);
CREATE INDEX IF NOT EXISTS kind_sorted_idx ON events(kind, created_at DESC, id ASC);
CREATE INDEX IF NOT EXISTS pubkey_kind_sorted_idx ON events(pubkey, kind, created_at DESC, id ASC);

CREATE TABLE IF NOT EXISTS tags (
    event_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    
    PRIMARY KEY (key, value, event_id),
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS tags_event_id_idx ON tags(event_id);

CREATE TRIGGER IF NOT EXISTS d_tags_ai AFTER INSERT ON events
WHEN NEW.kind BETWEEN 30000 AND 39999 
BEGIN
INSERT INTO tags (event_id, key, value)
    SELECT NEW.id, 'd', json_extract(value, '$[1]')
    FROM json_each(NEW.tags)
    WHERE json_type(value) = 'array' AND json_array_length(value) > 1 AND json_extract(value, '$[0]') = 'd'
    LIMIT 1;
END;