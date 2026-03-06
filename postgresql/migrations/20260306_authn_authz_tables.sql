CREATE TABLE qc_coversheet.app_user (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id text NOT NULL,
    entra_object_id text NOT NULL,
    email public.citext,
    display_name text,
    given_name text,
    family_name text,
    preferred_username text,
    is_active boolean DEFAULT true NOT NULL,
    is_approved boolean DEFAULT false NOT NULL,
    last_login_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE qc_coversheet.app_user
    ADD CONSTRAINT app_user_pkey PRIMARY KEY (id);

ALTER TABLE qc_coversheet.app_user
    ADD CONSTRAINT uq_app_user_tenant_object UNIQUE (tenant_id, entra_object_id);

CREATE INDEX ix_app_user_email ON qc_coversheet.app_user USING btree (email);

CREATE TABLE qc_coversheet.app_role (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    role_name text NOT NULL,
    description text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE qc_coversheet.app_role
    ADD CONSTRAINT app_role_pkey PRIMARY KEY (id);

ALTER TABLE qc_coversheet.app_role
    ADD CONSTRAINT uq_app_role_name UNIQUE (role_name);

CREATE TABLE qc_coversheet.app_user_role (
    app_user_id uuid NOT NULL,
    role_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE qc_coversheet.app_user_role
    ADD CONSTRAINT app_user_role_pkey PRIMARY KEY (app_user_id, role_id);

ALTER TABLE qc_coversheet.app_user_role
    ADD CONSTRAINT fk_app_user_role_user FOREIGN KEY (app_user_id)
    REFERENCES qc_coversheet.app_user(id) ON DELETE CASCADE;

ALTER TABLE qc_coversheet.app_user_role
    ADD CONSTRAINT fk_app_user_role_role FOREIGN KEY (role_id)
    REFERENCES qc_coversheet.app_role(id) ON DELETE CASCADE;

CREATE INDEX ix_app_user_role_role_id ON qc_coversheet.app_user_role USING btree (role_id);

CREATE TABLE qc_coversheet.app_permission (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    permission_key text NOT NULL,
    description text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE qc_coversheet.app_permission
    ADD CONSTRAINT app_permission_pkey PRIMARY KEY (id);

ALTER TABLE qc_coversheet.app_permission
    ADD CONSTRAINT uq_app_permission_key UNIQUE (permission_key);

CREATE TABLE qc_coversheet.app_role_permission (
    role_id uuid NOT NULL,
    permission_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE qc_coversheet.app_role_permission
    ADD CONSTRAINT app_role_permission_pkey PRIMARY KEY (role_id, permission_id);

ALTER TABLE qc_coversheet.app_role_permission
    ADD CONSTRAINT fk_app_role_permission_role FOREIGN KEY (role_id)
    REFERENCES qc_coversheet.app_role(id) ON DELETE CASCADE;

ALTER TABLE qc_coversheet.app_role_permission
    ADD CONSTRAINT fk_app_role_permission_permission FOREIGN KEY (permission_id)
    REFERENCES qc_coversheet.app_permission(id) ON DELETE CASCADE;

CREATE INDEX ix_app_role_permission_permission_id ON qc_coversheet.app_role_permission USING btree (permission_id);

CREATE TABLE qc_coversheet.app_user_contact_link (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    app_user_id uuid NOT NULL,
    contact_id uuid NOT NULL,
    link_reason text DEFAULT 'manual'::text NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE qc_coversheet.app_user_contact_link
    ADD CONSTRAINT app_user_contact_link_pkey PRIMARY KEY (id);

ALTER TABLE qc_coversheet.app_user_contact_link
    ADD CONSTRAINT fk_app_user_contact_link_user FOREIGN KEY (app_user_id)
    REFERENCES qc_coversheet.app_user(id) ON DELETE CASCADE;

ALTER TABLE qc_coversheet.app_user_contact_link
    ADD CONSTRAINT fk_app_user_contact_link_contact FOREIGN KEY (contact_id)
    REFERENCES qc_coversheet.contact(id) ON DELETE CASCADE;

CREATE UNIQUE INDEX uq_app_user_contact_link_active
    ON qc_coversheet.app_user_contact_link USING btree (app_user_id, contact_id)
    WHERE (is_active = true);

CREATE INDEX ix_app_user_contact_link_contact_id
    ON qc_coversheet.app_user_contact_link USING btree (contact_id);

CREATE TABLE qc_coversheet.app_session_audit (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    app_user_id uuid NOT NULL,
    session_id text NOT NULL,
    event_type text NOT NULL,
    ip_address text,
    user_agent text,
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE qc_coversheet.app_session_audit
    ADD CONSTRAINT app_session_audit_pkey PRIMARY KEY (id);

ALTER TABLE qc_coversheet.app_session_audit
    ADD CONSTRAINT fk_app_session_audit_user FOREIGN KEY (app_user_id)
    REFERENCES qc_coversheet.app_user(id) ON DELETE CASCADE;

CREATE INDEX ix_app_session_audit_user_created
    ON qc_coversheet.app_session_audit USING btree (app_user_id, created_at DESC);

CREATE INDEX ix_app_session_audit_session_id
    ON qc_coversheet.app_session_audit USING btree (session_id);

CREATE TRIGGER trg_app_user_set_updated_at
    BEFORE UPDATE ON qc_coversheet.app_user
    FOR EACH ROW EXECUTE FUNCTION qc_coversheet.set_updated_at();

CREATE TRIGGER trg_app_user_contact_link_set_updated_at
    BEFORE UPDATE ON qc_coversheet.app_user_contact_link
    FOR EACH ROW EXECUTE FUNCTION qc_coversheet.set_updated_at();
