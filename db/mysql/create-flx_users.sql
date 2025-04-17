CREATE TABLE IF NOT EXISTS flx_users (
       id             BIGINT signed AUTO_INCREMENT PRIMARY KEY,
       email          VARCHAR(100) NOT NULL,
       phone          VARCHAR(15) NOT NULL,
       password       LONGTEXT NOT NULL,
       name           VARCHAR(50) NOT NULL,
       photo          VARCHAR(50) NULL,
       email_verified TINYINT NOT NULL DEFAULT 0,
       enabled        TINYINT DEFAULT 1 NULL,

       created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
       updated_at    DATETIME null,
       deleted_at    DATETIME null,
       created_by_id bigint signed null,
       updated_by_id bigint signed null,
       deleted_by_id bigint signed null,

       CONSTRAINT idx_user_email UNIQUE (email),
       CONSTRAINT idx_user_phone UNIQUE (phone)
);
