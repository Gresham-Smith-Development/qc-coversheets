--
-- PostgreSQL database dump
--

\restrict THdsmKxq3Wmsb2BjraaXfaHegxBOjtyX7TPieUhEnZTSbCFPmxuqBFf3xEuRAVx

-- Dumped from database version 18.3
-- Dumped by pg_dump version 18.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: qc_coversheet; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA qc_coversheet;


ALTER SCHEMA qc_coversheet OWNER TO postgres;


--
-- Name: citext; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;


--
-- Name: EXTENSION citext; Type: COMMENT; Schema: -
--

COMMENT ON EXTENSION citext IS 'data type for case-insensitive character strings';

--
-- Name: enforce_submission_matches_request(); Type: FUNCTION; Schema: qc_coversheet; Owner: postgres
--

CREATE FUNCTION qc_coversheet.enforce_submission_matches_request() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  exp_template uuid;
  exp_version  integer;
BEGIN
  SELECT expected_form_template_id, expected_form_version
    INTO exp_template, exp_version
  FROM qc_coversheet.review_request
  WHERE id = NEW.review_request_id;

  IF exp_template IS NULL THEN
    RAISE EXCEPTION 'review_request % not found or missing expected form', NEW.review_request_id;
  END IF;

  -- If caller didn't provide values, set them automatically
  IF NEW.form_template_id IS NULL THEN
    NEW.form_template_id := exp_template;
  END IF;

  IF NEW.form_version IS NULL THEN
    NEW.form_version := exp_version;
  END IF;

  -- If caller did provide values, enforce they match expectation
  IF NEW.form_template_id <> exp_template OR NEW.form_version <> exp_version THEN
    RAISE EXCEPTION
      'Submission template/version (%,%) does not match expected (%,%) for review_request %',
      NEW.form_template_id, NEW.form_version, exp_template, exp_version, NEW.review_request_id;
  END IF;

  RETURN NEW;
END;
$$;


ALTER FUNCTION qc_coversheet.enforce_submission_matches_request() OWNER TO postgres;

--
-- Name: set_updated_at(); Type: FUNCTION; Schema: qc_coversheet; Owner: postgres
--

CREATE FUNCTION qc_coversheet.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;


ALTER FUNCTION qc_coversheet.set_updated_at() OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: contact; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.contact (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    erp_contact_id text NOT NULL,
    email public.citext NOT NULL,
    display_name text,
    company_erp_id text,
    erp_company_name text,
    last_seen_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE qc_coversheet.contact OWNER TO postgres;

--
-- Name: discipline; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.discipline (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    erp_discipline_code text NOT NULL,
    discipline_name text NOT NULL,
    active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE qc_coversheet.discipline OWNER TO postgres;

--
-- Name: form_template; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.form_template (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    template_key text NOT NULL,
    display_name text NOT NULL,
    description text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE qc_coversheet.form_template OWNER TO postgres;

--
-- Name: form_template_version; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.form_template_version (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    form_template_id uuid NOT NULL,
    version integer NOT NULL,
    schema_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_form_template_version_positive CHECK ((version > 0))
);


ALTER TABLE qc_coversheet.form_template_version OWNER TO postgres;

--
-- Name: ingest_event; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.ingest_event (
    event_id uuid NOT NULL,
    qc_udic_id text NOT NULL,
    event_type text NOT NULL,
    event_time timestamp with time zone NOT NULL,
    correlation_id text NOT NULL,
    status text NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ingest_event_status_check CHECK ((status = ANY (ARRAY['received'::text, 'processing'::text, 'processed'::text, 'failed'::text])))
);


ALTER TABLE qc_coversheet.ingest_event OWNER TO postgres;

--
-- Name: ingest_job; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.ingest_job (
    qc_udic_id text NOT NULL,
    latest_event_id uuid NOT NULL,
    status text NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    run_after timestamp with time zone,
    locked_at timestamp with time zone,
    locked_by text,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ingest_job_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'processing'::text, 'done'::text, 'failed'::text])))
);


ALTER TABLE qc_coversheet.ingest_job OWNER TO postgres;

--
-- Name: project; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.project (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    project_wbs text NOT NULL,
    project_name_current text,
    market_current text,
    location_current text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE qc_coversheet.project OWNER TO postgres;

--
-- Name: project_execution_record; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.project_execution_record (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    pep_udic_id text NOT NULL,
    received_at timestamp with time zone DEFAULT now() NOT NULL,
    source_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE qc_coversheet.project_execution_record OWNER TO postgres;

--
-- Name: qc_coversheet_coversheet; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.qc_coversheet_coversheet (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    project_execution_record_id uuid NOT NULL,
    project_id uuid,
    qc_coversheet_udic_id text NOT NULL,
    ingested_at timestamp with time zone DEFAULT now() NOT NULL,
    source_created_at timestamp with time zone,
    project_wbs text NOT NULL,
    submittal_name text NOT NULL,
    submittal_date date,
    constructability_start_date date,
    project_name_snapshot text,
    client_name_snapshot text,
    client_id_snapshot text,
    market_snapshot text,
    location_snapshot text,
    pm_name_snapshot text,
    pm_email_snapshot public.citext,
    pp_name_snapshot text,
    pp_email_snapshot public.citext,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE qc_coversheet.qc_coversheet_coversheet OWNER TO postgres;

--
-- Name: review_request; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.review_request (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    qc_coversheet_coversheet_id uuid NOT NULL,
    reviewer_contact_id uuid NOT NULL,
    status text DEFAULT 'draft'::text NOT NULL,
    due_at timestamp with time zone,
    sent_at timestamp with time zone,
    completed_at timestamp with time zone,
    reviewer_name_used text,
    expected_form_template_id uuid NOT NULL,
    expected_form_version integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_review_request_status CHECK ((status = ANY (ARRAY['draft'::text, 'queued'::text, 'sent'::text, 'opened'::text, 'in_progress'::text, 'submitted'::text, 'overdue'::text, 'cancelled'::text, 'failed'::text])))
);


ALTER TABLE qc_coversheet.review_request OWNER TO postgres;

--
-- Name: review_request_discipline; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.review_request_discipline (
    review_request_id uuid NOT NULL,
    discipline_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE qc_coversheet.review_request_discipline OWNER TO postgres;

--
-- Name: review_request_event; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.review_request_event (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    review_request_id uuid NOT NULL,
    event_type text NOT NULL,
    occurred_at timestamp with time zone DEFAULT now() NOT NULL,
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE qc_coversheet.review_request_event OWNER TO postgres;

--
-- Name: review_form_validation_event; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.review_form_validation_event (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    review_request_id uuid NOT NULL,
    errors jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE qc_coversheet.review_form_validation_event OWNER TO postgres;

--
-- Name: review_submission; Type: TABLE; Schema: qc_coversheet; Owner: postgres
--

CREATE TABLE qc_coversheet.review_submission (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    review_request_id uuid NOT NULL,
    form_template_id uuid NOT NULL,
    form_version integer NOT NULL,
    submitted_at timestamp with time zone DEFAULT now() NOT NULL,
    answers jsonb DEFAULT '{}'::jsonb NOT NULL,
    overall_result text,
    risk_level text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_review_submission_form_version_positive CHECK ((form_version > 0))
);


ALTER TABLE qc_coversheet.review_submission OWNER TO postgres;

--
-- Name: contact contact_pkey; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.contact
    ADD CONSTRAINT contact_pkey PRIMARY KEY (id);


--
-- Name: discipline discipline_pkey; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.discipline
    ADD CONSTRAINT discipline_pkey PRIMARY KEY (id);


--
-- Name: form_template form_template_pkey; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.form_template
    ADD CONSTRAINT form_template_pkey PRIMARY KEY (id);


--
-- Name: form_template_version form_template_version_pkey; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.form_template_version
    ADD CONSTRAINT form_template_version_pkey PRIMARY KEY (id);


--
-- Name: ingest_event ingest_event_pkey; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.ingest_event
    ADD CONSTRAINT ingest_event_pkey PRIMARY KEY (event_id);


--
-- Name: ingest_job ingest_job_pkey; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.ingest_job
    ADD CONSTRAINT ingest_job_pkey PRIMARY KEY (qc_udic_id);


--
-- Name: review_form_validation_event review_form_validation_event_pkey; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_form_validation_event
    ADD CONSTRAINT review_form_validation_event_pkey PRIMARY KEY (id);


--
-- Name: review_request_discipline pk_review_request_discipline; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_request_discipline
    ADD CONSTRAINT pk_review_request_discipline PRIMARY KEY (review_request_id, discipline_id);


--
-- Name: project_execution_record project_execution_record_pkey; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.project_execution_record
    ADD CONSTRAINT project_execution_record_pkey PRIMARY KEY (id);


--
-- Name: project project_pkey; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.project
    ADD CONSTRAINT project_pkey PRIMARY KEY (id);


--
-- Name: qc_coversheet_coversheet qc_coversheet_coversheet_pkey; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.qc_coversheet_coversheet
    ADD CONSTRAINT qc_coversheet_coversheet_pkey PRIMARY KEY (id);


--
-- Name: review_request_event review_request_event_pkey; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_request_event
    ADD CONSTRAINT review_request_event_pkey PRIMARY KEY (id);


--
-- Name: review_request review_request_pkey; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_request
    ADD CONSTRAINT review_request_pkey PRIMARY KEY (id);


--
-- Name: review_submission review_submission_pkey; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_submission
    ADD CONSTRAINT review_submission_pkey PRIMARY KEY (id);




--
-- Name: discipline uq_discipline_erp_code; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.discipline
    ADD CONSTRAINT uq_discipline_erp_code UNIQUE (erp_discipline_code);


--
-- Name: form_template uq_form_template_key; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.form_template
    ADD CONSTRAINT uq_form_template_key UNIQUE (template_key);


--
-- Name: form_template_version uq_form_template_version; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.form_template_version
    ADD CONSTRAINT uq_form_template_version UNIQUE (form_template_id, version);


--
-- Name: project_execution_record uq_project_execution_record_pep; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.project_execution_record
    ADD CONSTRAINT uq_project_execution_record_pep UNIQUE (pep_udic_id);


--
-- Name: project uq_project_project_wbs; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.project
    ADD CONSTRAINT uq_project_project_wbs UNIQUE (project_wbs);


--
-- Name: qc_coversheet_coversheet uq_qc_coversheet_coversheet_per_pep; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.qc_coversheet_coversheet
    ADD CONSTRAINT uq_qc_coversheet_coversheet_per_pep UNIQUE (project_execution_record_id, qc_coversheet_udic_id);


--
-- Name: review_request uq_review_request_unique_reviewer_per_coversheet; Type: CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_request
    ADD CONSTRAINT uq_review_request_unique_reviewer_per_coversheet UNIQUE (qc_coversheet_coversheet_id, reviewer_contact_id);


--
-- Name: ix_contact_company_erp_id; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_contact_company_erp_id ON qc_coversheet.contact USING btree (company_erp_id);




--
-- Name: uq_contact_erp_contact_id; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_contact_erp_contact_id ON qc_coversheet.contact USING btree (erp_contact_id);

--
-- Name: ix_discipline_name; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_discipline_name ON qc_coversheet.discipline USING btree (discipline_name);


--
-- Name: ix_form_template_version_template; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_form_template_version_template ON qc_coversheet.form_template_version USING btree (form_template_id);


--
-- Name: ix_ingest_event_qc_udic_id; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_ingest_event_qc_udic_id ON qc_coversheet.ingest_event USING btree (qc_udic_id);


--
-- Name: ix_ingest_event_status_last_seen; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_ingest_event_status_last_seen ON qc_coversheet.ingest_event USING btree (status, last_seen_at DESC);


--
-- Name: ix_ingest_job_status_run_after; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_ingest_job_status_run_after ON qc_coversheet.ingest_job USING btree (status, run_after);


--
-- Name: ix_project_execution_record_received_at; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_project_execution_record_received_at ON qc_coversheet.project_execution_record USING btree (received_at);


--
-- Name: ix_project_project_wbs; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_project_project_wbs ON qc_coversheet.project USING btree (project_wbs);


--
-- Name: ix_qc_coversheet_coversheet_per_id; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_qc_coversheet_coversheet_per_id ON qc_coversheet.qc_coversheet_coversheet USING btree (project_execution_record_id);


--
-- Name: ix_qc_coversheet_coversheet_project_id; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_qc_coversheet_coversheet_project_id ON qc_coversheet.qc_coversheet_coversheet USING btree (project_id);


--
-- Name: ix_qc_coversheet_coversheet_project_wbs; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_qc_coversheet_coversheet_project_wbs ON qc_coversheet.qc_coversheet_coversheet USING btree (project_wbs);


--
-- Name: ix_qc_coversheet_coversheet_qc_coversheet_udic_id; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_qc_coversheet_coversheet_qc_coversheet_udic_id ON qc_coversheet.qc_coversheet_coversheet USING btree (qc_coversheet_udic_id);


--
-- Name: ix_qc_coversheet_coversheet_submittal_date; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_qc_coversheet_coversheet_submittal_date ON qc_coversheet.qc_coversheet_coversheet USING btree (submittal_date);


--
-- Name: ix_review_form_validation_event_created_at; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_review_form_validation_event_created_at ON qc_coversheet.review_form_validation_event USING btree (created_at);


--
-- Name: ix_review_form_validation_event_review_request_id; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_review_form_validation_event_review_request_id ON qc_coversheet.review_form_validation_event USING btree (review_request_id);


--
-- Name: ix_review_request_coversheet; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_review_request_coversheet ON qc_coversheet.review_request USING btree (qc_coversheet_coversheet_id);


--
-- Name: ix_review_request_due_at; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_review_request_due_at ON qc_coversheet.review_request USING btree (due_at);


--
-- Name: ix_review_request_event_request_time; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_review_request_event_request_time ON qc_coversheet.review_request_event USING btree (review_request_id, occurred_at DESC);


--
-- Name: ix_review_request_event_type; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_review_request_event_type ON qc_coversheet.review_request_event USING btree (event_type);


--
-- Name: ix_review_request_reviewer; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_review_request_reviewer ON qc_coversheet.review_request USING btree (reviewer_contact_id);


--
-- Name: ix_review_request_status; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_review_request_status ON qc_coversheet.review_request USING btree (status);


--
-- Name: ix_review_submission_request; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_review_submission_request ON qc_coversheet.review_submission USING btree (review_request_id, submitted_at DESC);


--
-- Name: ix_rrd_discipline; Type: INDEX; Schema: qc_coversheet; Owner: postgres
--

CREATE INDEX ix_rrd_discipline ON qc_coversheet.review_request_discipline USING btree (discipline_id);


--
-- Name: contact trg_contact_set_updated_at; Type: TRIGGER; Schema: qc_coversheet; Owner: postgres
--

CREATE TRIGGER trg_contact_set_updated_at BEFORE UPDATE ON qc_coversheet.contact FOR EACH ROW EXECUTE FUNCTION qc_coversheet.set_updated_at();


--
-- Name: discipline trg_discipline_set_updated_at; Type: TRIGGER; Schema: qc_coversheet; Owner: postgres
--

CREATE TRIGGER trg_discipline_set_updated_at BEFORE UPDATE ON qc_coversheet.discipline FOR EACH ROW EXECUTE FUNCTION qc_coversheet.set_updated_at();


--
-- Name: form_template trg_form_template_set_updated_at; Type: TRIGGER; Schema: qc_coversheet; Owner: postgres
--

CREATE TRIGGER trg_form_template_set_updated_at BEFORE UPDATE ON qc_coversheet.form_template FOR EACH ROW EXECUTE FUNCTION qc_coversheet.set_updated_at();


--
-- Name: project_execution_record trg_project_execution_record_set_updated_at; Type: TRIGGER; Schema: qc_coversheet; Owner: postgres
--

CREATE TRIGGER trg_project_execution_record_set_updated_at BEFORE UPDATE ON qc_coversheet.project_execution_record FOR EACH ROW EXECUTE FUNCTION qc_coversheet.set_updated_at();


--
-- Name: project trg_project_set_updated_at; Type: TRIGGER; Schema: qc_coversheet; Owner: postgres
--

CREATE TRIGGER trg_project_set_updated_at BEFORE UPDATE ON qc_coversheet.project FOR EACH ROW EXECUTE FUNCTION qc_coversheet.set_updated_at();


--
-- Name: qc_coversheet_coversheet trg_qc_coversheet_coversheet_set_updated_at; Type: TRIGGER; Schema: qc_coversheet; Owner: postgres
--

CREATE TRIGGER trg_qc_coversheet_coversheet_set_updated_at BEFORE UPDATE ON qc_coversheet.qc_coversheet_coversheet FOR EACH ROW EXECUTE FUNCTION qc_coversheet.set_updated_at();


--
-- Name: review_request trg_review_request_set_updated_at; Type: TRIGGER; Schema: qc_coversheet; Owner: postgres
--

CREATE TRIGGER trg_review_request_set_updated_at BEFORE UPDATE ON qc_coversheet.review_request FOR EACH ROW EXECUTE FUNCTION qc_coversheet.set_updated_at();


--
-- Name: review_submission trg_review_submission_enforce_match; Type: TRIGGER; Schema: qc_coversheet; Owner: postgres
--

CREATE TRIGGER trg_review_submission_enforce_match BEFORE INSERT ON qc_coversheet.review_submission FOR EACH ROW EXECUTE FUNCTION qc_coversheet.enforce_submission_matches_request();


--
-- Name: form_template_version fk_form_template_version_template; Type: FK CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.form_template_version
    ADD CONSTRAINT fk_form_template_version_template FOREIGN KEY (form_template_id) REFERENCES qc_coversheet.form_template(id) ON DELETE CASCADE;


--
-- Name: ingest_job fk_ingest_job_latest_event; Type: FK CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.ingest_job
    ADD CONSTRAINT fk_ingest_job_latest_event FOREIGN KEY (latest_event_id) REFERENCES qc_coversheet.ingest_event(event_id) ON DELETE RESTRICT;


--
-- Name: qc_coversheet_coversheet fk_qc_coversheet_coversheet_per; Type: FK CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.qc_coversheet_coversheet
    ADD CONSTRAINT fk_qc_coversheet_coversheet_per FOREIGN KEY (project_execution_record_id) REFERENCES qc_coversheet.project_execution_record(id) ON DELETE RESTRICT;


--
-- Name: qc_coversheet_coversheet fk_qc_coversheet_coversheet_project; Type: FK CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.qc_coversheet_coversheet
    ADD CONSTRAINT fk_qc_coversheet_coversheet_project FOREIGN KEY (project_id) REFERENCES qc_coversheet.project(id) ON DELETE RESTRICT;


--
-- Name: review_form_validation_event review_form_validation_event_review_request_id_fkey; Type: FK CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_form_validation_event
    ADD CONSTRAINT review_form_validation_event_review_request_id_fkey FOREIGN KEY (review_request_id) REFERENCES qc_coversheet.review_request(id) ON DELETE CASCADE;


--
-- Name: review_request fk_review_request_coversheet; Type: FK CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_request
    ADD CONSTRAINT fk_review_request_coversheet FOREIGN KEY (qc_coversheet_coversheet_id) REFERENCES qc_coversheet.qc_coversheet_coversheet(id) ON DELETE RESTRICT;


--
-- Name: review_request_event fk_review_request_event_request; Type: FK CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_request_event
    ADD CONSTRAINT fk_review_request_event_request FOREIGN KEY (review_request_id) REFERENCES qc_coversheet.review_request(id) ON DELETE CASCADE;


--
-- Name: review_request fk_review_request_expected_template_version; Type: FK CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_request
    ADD CONSTRAINT fk_review_request_expected_template_version FOREIGN KEY (expected_form_template_id, expected_form_version) REFERENCES qc_coversheet.form_template_version(form_template_id, version) ON DELETE RESTRICT;


--
-- Name: review_request fk_review_request_reviewer; Type: FK CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_request
    ADD CONSTRAINT fk_review_request_reviewer FOREIGN KEY (reviewer_contact_id) REFERENCES qc_coversheet.contact(id) ON DELETE RESTRICT;


--
-- Name: review_submission fk_review_submission_request; Type: FK CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_submission
    ADD CONSTRAINT fk_review_submission_request FOREIGN KEY (review_request_id) REFERENCES qc_coversheet.review_request(id) ON DELETE CASCADE;


--
-- Name: review_submission fk_review_submission_template_version; Type: FK CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_submission
    ADD CONSTRAINT fk_review_submission_template_version FOREIGN KEY (form_template_id, form_version) REFERENCES qc_coversheet.form_template_version(form_template_id, version) ON DELETE RESTRICT;


--
-- Name: review_request_discipline fk_rrd_discipline; Type: FK CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_request_discipline
    ADD CONSTRAINT fk_rrd_discipline FOREIGN KEY (discipline_id) REFERENCES qc_coversheet.discipline(id) ON DELETE RESTRICT;


--
-- Name: review_request_discipline fk_rrd_request; Type: FK CONSTRAINT; Schema: qc_coversheet; Owner: postgres
--

ALTER TABLE ONLY qc_coversheet.review_request_discipline
    ADD CONSTRAINT fk_rrd_request FOREIGN KEY (review_request_id) REFERENCES qc_coversheet.review_request(id) ON DELETE CASCADE;


--
-- Name: SCHEMA qc_coversheet; Type: ACL; Schema: -; Owner: postgres
--

GRANT ALL ON SCHEMA qc_coversheet TO pg_write_all_data;


--
-- PostgreSQL database dump complete
--

\unrestrict THdsmKxq3Wmsb2BjraaXfaHegxBOjtyX7TPieUhEnZTSbCFPmxuqBFf3xEuRAVx

-- Migration: phase 2 seed data for form_template/form_template_version
-- Safe to re-run: uses ON CONFLICT and conditional inserts.

WITH upsert_template AS (
    INSERT INTO qc_coversheet.form_template (
        template_key, display_name, description, created_at, updated_at
    )
    VALUES (
        'qc_subconsultant_review',
        'QC Subconsultant Review Form',
        'Phase 2 default reviewer template',
        now(),
        now()
    )
    ON CONFLICT (template_key) DO UPDATE
    SET display_name = EXCLUDED.display_name,
        description = EXCLUDED.description,
        updated_at = now()
    RETURNING id
),
template_id AS (
    SELECT id FROM upsert_template
    UNION ALL
    SELECT id FROM qc_coversheet.form_template WHERE template_key = 'qc_subconsultant_review'
    LIMIT 1
)
INSERT INTO qc_coversheet.form_template_version (
    form_template_id, version, schema_json, is_active, created_at
)
SELECT
    t.id,
    1,
    '{
      "schema_version": "1.0",
      "template_key": "qc_subconsultant_review",
      "display_name": "QC Subconsultant Review Form",
      "branding": { "org_name": "Gresham Smith", "logo_url": "/static/images/logo.png" },
      "auto_fields": [
        "project_name",
        "project_number",
        "owner_end_user",
        "submittal_name",
        "submittal_date",
        "reviewer_name"
      ],
      "discipline_repeat": {
        "source": "review_request_discipline",
        "label_field": "discipline_name",
        "items": [
          {
            "section_key": "off_team_qualified_review",
            "section_label": "Off-Team Qualified Review",
            "choice": { "type": "single_select", "options": ["complete", "na"], "required": true },
            "signature": {
              "required_when_choice_selected": true,
              "type_name_must_match_reviewer": true,
              "match_mode": "case_insensitive_exact",
              "capture_timestamp": true
            },
            "notes": { "type": "text", "required": false, "max_length": 4000 }
          },
          {
            "section_key": "constructability_review",
            "section_label": "Constructability Review",
            "choice": { "type": "single_select", "options": ["complete", "na"], "required": true },
            "signature": {
              "required_when_choice_selected": true,
              "type_name_must_match_reviewer": true,
              "match_mode": "case_insensitive_exact",
              "capture_timestamp": true
            },
            "notes": { "type": "text", "required": false, "max_length": 4000 }
          }
        ]
      }
    }'::jsonb,
    true,
    now()
FROM template_id t
ON CONFLICT (form_template_id, version) DO UPDATE
SET schema_json = EXCLUDED.schema_json,
    is_active = true;

-- Ensure all other versions are inactive once v1 is seeded/updated.
UPDATE qc_coversheet.form_template_version ftv
SET is_active = false
FROM qc_coversheet.form_template ft
WHERE ftv.form_template_id = ft.id
  AND ft.template_key = 'qc_subconsultant_review'
  AND ftv.version <> 1;

-- Optional local seed for review_request and disciplines:
-- inserts only when enough source data exists.
DO $$
DECLARE
    v_template_id uuid;
    v_coversheet_id uuid;
    v_contact_id uuid;
    v_request_id uuid;
BEGIN
    SELECT id INTO v_template_id
    FROM qc_coversheet.form_template
    WHERE template_key = 'qc_subconsultant_review'
    LIMIT 1;

    SELECT id INTO v_coversheet_id
    FROM qc_coversheet.qc_coversheet_coversheet
    ORDER BY ingested_at DESC
    LIMIT 1;

    SELECT id INTO v_contact_id
    FROM qc_coversheet.contact
    ORDER BY updated_at DESC
    LIMIT 1;

    IF v_template_id IS NULL OR v_coversheet_id IS NULL OR v_contact_id IS NULL THEN
        RAISE NOTICE 'Skipping review_request seed (missing template/coversheet/contact).';
        RETURN;
    END IF;

    INSERT INTO qc_coversheet.review_request (
        qc_coversheet_coversheet_id,
        reviewer_contact_id,
        status,
        reviewer_name_used,
        expected_form_template_id,
        expected_form_version,
        created_at,
        updated_at
    )
    VALUES (
        v_coversheet_id,
        v_contact_id,
        'draft',
        'J Smiley Baltz',
        v_template_id,
        1,
        now(),
        now()
    )
    ON CONFLICT (qc_coversheet_coversheet_id, reviewer_contact_id) DO UPDATE
    SET expected_form_template_id = EXCLUDED.expected_form_template_id,
        expected_form_version = EXCLUDED.expected_form_version,
        reviewer_name_used = EXCLUDED.reviewer_name_used,
        updated_at = now()
    RETURNING id INTO v_request_id;

    IF v_request_id IS NULL THEN
        SELECT id INTO v_request_id
        FROM qc_coversheet.review_request
        WHERE qc_coversheet_coversheet_id = v_coversheet_id
          AND reviewer_contact_id = v_contact_id
        LIMIT 1;
    END IF;

    INSERT INTO qc_coversheet.review_request_discipline (review_request_id, discipline_id, created_at)
    SELECT v_request_id, d.id, now()
    FROM qc_coversheet.discipline d
    WHERE d.active = true
    ORDER BY d.updated_at DESC
    LIMIT 2
    ON CONFLICT (review_request_id, discipline_id) DO NOTHING;
END $$;

-- Migration append: 20260306_authn_authz_tables.sql

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

-- Migration append: 20260306_authn_authz_seed.sql

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
