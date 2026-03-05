-- Migration: add email to contact
-- Date: 2026-03-05

ALTER TABLE qc_coversheet.contact
ADD COLUMN IF NOT EXISTS email text;
