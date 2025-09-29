SELECT TOP 1 id, name, CAST(password as NVARCHAR(255)) as password
FROM flx_users
WHERE email = '{{email}}' AND enabled = 1;
