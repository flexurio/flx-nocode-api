CREATE TABLE IF NOT EXISTS flx_users (
    id              BIGINT IDENTITY(1,1) PRIMARY KEY,
    email           VARCHAR(100) NOT NULL,
    phone           VARCHAR(15) NOT NULL,
    password        NVARCHAR(MAX) NOT NULL,
    name            VARCHAR(50) NOT NULL,
    photo           VARCHAR(50) NULL,
    email_verified  BIT NOT NULL DEFAULT 0,
    enabled         BIT NULL DEFAULT 1,

    created_at      DATETIME2 DEFAULT SYSDATETIME(),
    updated_at      DATETIME2 NULL,
    deleted_at      DATETIME2 NULL,
    created_by_id   BIGINT NULL,
    updated_by_id   BIGINT NULL,
    deleted_by_id   BIGINT NULL,

    CONSTRAINT idx_user_email UNIQUE (email),
    CONSTRAINT idx_user_phone UNIQUE (phone)
);
