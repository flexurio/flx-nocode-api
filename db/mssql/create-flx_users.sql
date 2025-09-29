IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='flx_users' AND xtype='U')
BEGIN
    CREATE TABLE flx_users (
        id              BIGINT IDENTITY(1,1) PRIMARY KEY,
        email           VARCHAR(100) NOT NULL,
        phone           VARCHAR(15) NOT NULL,
        password        NVARCHAR(MAX) NOT NULL,
        name            VARCHAR(50) NOT NULL,
        photo           VARCHAR(50) NULL,
        email_verified  BIT NOT NULL CONSTRAINT df_user_email_verified DEFAULT 0,
        enabled         BIT NULL CONSTRAINT df_user_enabled DEFAULT 1,

        created_at      DATETIME NOT NULL CONSTRAINT df_user_created_at DEFAULT SYSDATETIME(),
        updated_at      DATETIME NULL,
        deleted_at      DATETIME NULL,
        created_by_id   BIGINT NULL,
        updated_by_id   BIGINT NULL,
        deleted_by_id   BIGINT NULL,

        CONSTRAINT uq_user_email UNIQUE (email),
        CONSTRAINT uq_user_phone UNIQUE (phone)
    );
END
