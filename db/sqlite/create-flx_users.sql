CREATE TABLE IF NOT EXISTS flx_users (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email           TEXT NOT NULL,
    phone           TEXT NOT NULL,
    password        TEXT NOT NULL,
    name            TEXT NOT NULL,
    photo           TEXT,
    email_verified  BOOLEAN NOT NULL DEFAULT 0,
    enabled         BOOLEAN DEFAULT 1,

    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME,
    deleted_at      DATETIME,
    created_by_id   INTEGER,
    updated_by_id   INTEGER,
    deleted_by_id   INTEGER,

    UNIQUE (email),
    UNIQUE (phone)
    -- Note: SQLite doesn't support named constraints or index inline creation with custom names like MySQL
);
