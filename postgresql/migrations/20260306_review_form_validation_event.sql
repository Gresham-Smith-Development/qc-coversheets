CREATE TABLE qc_coversheet.review_form_validation_event (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    review_request_id uuid NOT NULL,
    errors jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE qc_coversheet.review_form_validation_event
    ADD CONSTRAINT review_form_validation_event_pkey PRIMARY KEY (id);

ALTER TABLE qc_coversheet.review_form_validation_event
    ADD CONSTRAINT review_form_validation_event_review_request_id_fkey
        FOREIGN KEY (review_request_id)
        REFERENCES qc_coversheet.review_request(id)
        ON DELETE CASCADE;

CREATE INDEX review_form_validation_event_review_request_id_idx
    ON qc_coversheet.review_form_validation_event (review_request_id);

CREATE INDEX review_form_validation_event_created_at_idx
    ON qc_coversheet.review_form_validation_event (created_at);
