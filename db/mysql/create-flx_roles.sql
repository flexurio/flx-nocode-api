    CREATE TABLE IF NOT EXISTS flx_roles (
        id            BIGINT AUTO_INCREMENT PRIMARY KEY,
        id_users      BIGINT NOT NULL,
        endpoint      VARCHAR(100) NOT NULL,
        role          TINYINT NOT NULL,

        created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at    DATETIME null,
        deleted_at    DATETIME null,
        created_by_id bigint unsigned null,
        updated_by_id bigint unsigned null,
        deleted_by_id bigint unsigned null,

        CONSTRAINT idx_role_name UNIQUE (id_users, endpoint)
    );