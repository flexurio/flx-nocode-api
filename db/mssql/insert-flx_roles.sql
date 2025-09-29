INSERT INTO flx_roles (id_users, endpoint, role, created_at)
VALUES ({{id_user}}, 'flx_users', 127, SYSDATETIME());

INSERT INTO flx_roles (id_users, endpoint, role, created_at)
VALUES ({{id_user}}, 'flx_roles', 127, SYSDATETIME());
