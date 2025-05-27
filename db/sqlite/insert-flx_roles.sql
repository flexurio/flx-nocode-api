INSERT INTO flx_roles 
(id_users, endpoint, role, created_at) 
VALUES ({{id_user}}, 'flx_users', 127, CURRENT_TIMESTAMP);
INSERT INTO flx_roles
(id_users, endpoint, role, created_at)
VALUES ({{id_user}}, 'flx_roles', 127, CURRENT_TIMESTAMP);