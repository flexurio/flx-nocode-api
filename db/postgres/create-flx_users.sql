CREATE TABLE IF NOT EXISTS flx_users (
    id             BIGSERIAL PRIMARY KEY,
    email          VARCHAR(100) NOT NULL,
    phone          VARCHAR(15) NOT NULL,
    password       TEXT NOT NULL,
    name           VARCHAR(50) NOT NULL,
    photo          VARCHAR(50),
    email_verified BOOLEAN NOT NULL DEFAULT FALSE,
    enabled        BOOLEAN DEFAULT TRUE,

    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP,
    deleted_at     TIMESTAMP,
    created_by_id  BIGINT,
    updated_by_id  BIGINT,
    deleted_by_id  BIGINT,

    CONSTRAINT idx_user_email UNIQUE (email),
    CONSTRAINT idx_user_phone UNIQUE (phone)
);
