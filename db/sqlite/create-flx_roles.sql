CREATE TABLE IF NOT EXISTS flx_roles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    id_users        INTEGER NOT NULL,
    endpoint        TEXT NOT NULL,
    role            INTEGER NOT NULL,

    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME,
    deleted_at      DATETIME,
    created_by_id   INTEGER,
    updated_by_id   INTEGER,
    deleted_by_id   INTEGER,

    UNIQUE (id_users, endpoint)
);
