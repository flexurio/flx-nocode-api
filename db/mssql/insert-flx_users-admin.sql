INSERT INTO flx_users
    (email, phone, password, name, created_at, updated_at, enabled)
VALUES
    ('admin', '5758', '{{password}}', 'Admin Flexurio', SYSDATETIME(), SYSDATETIME(), 1);
