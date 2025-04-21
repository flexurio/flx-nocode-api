CREATE TABLE IF NOT EXISTS flx_roles (
    id             BIGSERIAL PRIMARY KEY,
    id_users       BIGINT NOT NULL,
    endpoint       VARCHAR(100) NOT NULL,
    role           SMALLINT NOT NULL,

    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP,
    deleted_at     TIMESTAMP,
    created_by_id  BIGINT,
    updated_by_id  BIGINT,
    deleted_by_id  BIGINT,

    CONSTRAINT idx_role_name UNIQUE (id_users, endpoint)
);
