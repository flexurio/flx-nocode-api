IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='flx_roles' AND xtype='U')
BEGIN
    CREATE TABLE flx_roles (
        id             BIGINT IDENTITY(1,1) PRIMARY KEY,
        id_users       BIGINT NOT NULL,
        endpoint       VARCHAR(100) NOT NULL,
        role           TINYINT NOT NULL,

        created_at     DATETIME2 NOT NULL CONSTRAINT df_role_created_at DEFAULT SYSDATETIME(),
        updated_at     DATETIME2 NULL,
        deleted_at     DATETIME2 NULL,
        created_by_id  BIGINT NULL,
        updated_by_id  BIGINT NULL,
        deleted_by_id  BIGINT NULL,

        CONSTRAINT uq_role_user_endpoint UNIQUE (id_users, endpoint)
    );
END
