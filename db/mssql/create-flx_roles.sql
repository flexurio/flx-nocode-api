CREATE TABLE IF NOT EXISTS flx_roles (
    id             BIGINT IDENTITY(1,1) PRIMARY KEY,
    id_users       BIGINT NOT NULL,
    endpoint       VARCHAR(100) NOT NULL,
    role           TINYINT NOT NULL,

    created_at     DATETIME2 DEFAULT SYSDATETIME(),
    updated_at     DATETIME2 NULL,
    deleted_at     DATETIME2 NULL,
    created_by_id  BIGINT NULL,
    updated_by_id  BIGINT NULL,
    deleted_by_id  BIGINT NULL,

    CONSTRAINT idx_role_name UNIQUE (id_users, endpoint)
);
