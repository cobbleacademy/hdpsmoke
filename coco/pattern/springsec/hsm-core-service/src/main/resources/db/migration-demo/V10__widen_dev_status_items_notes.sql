-- Pre-existing latent bug, surfaced by this round: several dev-status-seed.json
-- entries' notes text has grown past VARCHAR(1024) (the detailed historical-record
-- style established as this doc got used more heavily), but this only ever failed
-- silently -- DevStatusSeedInitializer only runs INSERTs against a genuinely empty
-- table, and the live demo DB already had rows, so the too-long INSERT was never
-- actually attempted until a fresh-DB test context hit it. Widened generously
-- rather than trimming the historical notes short.
ALTER TABLE dev_status_items ALTER COLUMN notes VARCHAR(8192);
