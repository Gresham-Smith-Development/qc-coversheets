INSERT INTO qc_coversheet.app_role (role_name, description, created_at)
VALUES
    ('admin', 'Full administrative access', now()),
    ('reviewer', 'Assigned reviewer access', now()),
    ('internal_readonly', 'Internal read-only form access for PM/PP assignments', now()),
    ('user', 'Signed-in user with no permissions by default', now())
ON CONFLICT (role_name) DO NOTHING;

INSERT INTO qc_coversheet.app_permission (permission_key, description, created_at)
VALUES
    ('admin.access', 'Global admin access', now()),
    ('admin.templates.read', 'Read admin form templates', now()),
    ('admin.templates.write', 'Write admin form templates', now()),
    ('admin.review_requests.read', 'Read admin review requests', now()),
    ('admin.review_requests.write', 'Write admin review requests', now()),
    ('reviewer.access', 'Reviewer area access', now()),
    ('reviewer.form.read', 'Read assigned review form', now()),
    ('reviewer.form.validate', 'Validate assigned review form', now()),
    ('reviewer.form.submit', 'Submit assigned review form', now()),
    ('internal.form.read', 'Read internal assigned form', now()),
    ('internal.assignment.read', 'Read internal assignment context', now())
ON CONFLICT (permission_key) DO NOTHING;

WITH admin_role AS (
    SELECT id FROM qc_coversheet.app_role WHERE role_name = 'admin'
),
all_permissions AS (
    SELECT id FROM qc_coversheet.app_permission
)
INSERT INTO qc_coversheet.app_role_permission (role_id, permission_id, created_at)
SELECT ar.id, p.id, now()
FROM admin_role ar
CROSS JOIN all_permissions p
ON CONFLICT (role_id, permission_id) DO NOTHING;

WITH reviewer_role AS (
    SELECT id FROM qc_coversheet.app_role WHERE role_name = 'reviewer'
),
reviewer_permissions AS (
    SELECT id
    FROM qc_coversheet.app_permission
    WHERE permission_key IN (
        'reviewer.access',
        'reviewer.form.read',
        'reviewer.form.validate',
        'reviewer.form.submit'
    )
)
INSERT INTO qc_coversheet.app_role_permission (role_id, permission_id, created_at)
SELECT rr.id, rp.id, now()
FROM reviewer_role rr
CROSS JOIN reviewer_permissions rp
ON CONFLICT (role_id, permission_id) DO NOTHING;

WITH internal_role AS (
    SELECT id FROM qc_coversheet.app_role WHERE role_name = 'internal_readonly'
),
internal_permissions AS (
    SELECT id
    FROM qc_coversheet.app_permission
    WHERE permission_key IN (
        'internal.form.read',
        'internal.assignment.read'
    )
)
INSERT INTO qc_coversheet.app_role_permission (role_id, permission_id, created_at)
SELECT ir.id, ip.id, now()
FROM internal_role ir
CROSS JOIN internal_permissions ip
ON CONFLICT (role_id, permission_id) DO NOTHING;
