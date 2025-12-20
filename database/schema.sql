-- Schema for Paper Trail API canonical database.
-- This file intentionally rebuilds the core tables and materialized views
-- required by the application and test suite. It can be rerun safely because
-- existing objects are dropped before creation.

DROP MATERIALIZED VIEW IF EXISTS canonical_politician_industry_summary;
DROP TABLE IF EXISTS votes CASCADE;
DROP TABLE IF EXISTS bill_topics CASCADE;
DROP TABLE IF EXISTS rollcalls CASCADE;
DROP TABLE IF EXISTS contributions CASCADE;
DROP TABLE IF EXISTS donors CASCADE;
DROP TABLE IF EXISTS politicians CASCADE;

CREATE TABLE politicians (
    politician_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT NOT NULL,
    full_name TEXT NOT NULL,
    party TEXT NOT NULL,
    state TEXT NOT NULL,
    seat TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    is_placeholder BOOLEAN NOT NULL DEFAULT FALSE,
    placeholder_type TEXT,
    icpsr_id INT,
    bioguide_id TEXT,
    nominate_dim1 NUMERIC,
    nominate_dim2 NUMERIC,
    first_elected_year INT,
    last_elected_year INT
);

CREATE TABLE donors (
    donor_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    donor_type TEXT NOT NULL,
    igcat TEXT,
    employer TEXT,
    occupation TEXT,
    state TEXT,
    total_contributions_count INT NOT NULL DEFAULT 0,
    total_amount NUMERIC(14, 2) NOT NULL DEFAULT 0
);

CREATE TABLE contributions (
    transaction_id TEXT PRIMARY KEY,
    donor_id TEXT NOT NULL REFERENCES donors(donor_id) ON DELETE CASCADE,
    recipient_id TEXT NOT NULL REFERENCES politicians(politician_id) ON DELETE CASCADE,
    amount NUMERIC(14, 2) NOT NULL,
    transaction_date DATE NOT NULL,
    industry TEXT,
    election_cycle INT,
    raw_contributor_name TEXT,
    raw_employer TEXT
);

CREATE TABLE rollcalls (
    rollcall_id SERIAL PRIMARY KEY,
    congress INT NOT NULL,
    chamber TEXT NOT NULL,
    rollnumber INT NOT NULL,
    bill_number TEXT NOT NULL,
    bill_description TEXT NOT NULL,
    vote_date DATE NOT NULL,
    vote_result TEXT,
    has_topics BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE bill_topics (
    topic_id SERIAL PRIMARY KEY,
    rollcall_id INT NOT NULL REFERENCES rollcalls(rollcall_id) ON DELETE CASCADE,
    topic_label TEXT NOT NULL,
    topic_source TEXT NOT NULL,
    topic_weight NUMERIC(6, 2),
    is_primary BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE votes (
    vote_id SERIAL PRIMARY KEY,
    politician_id TEXT NOT NULL REFERENCES politicians(politician_id) ON DELETE CASCADE,
    rollcall_id INT NOT NULL REFERENCES rollcalls(rollcall_id) ON DELETE CASCADE,
    vote_value TEXT NOT NULL CHECK (vote_value IN ('Yea', 'Nay', 'Present', 'Not Voting'))
);

CREATE INDEX IF NOT EXISTS idx_votes_politician_id ON votes(politician_id);
CREATE INDEX IF NOT EXISTS idx_votes_rollcall_id ON votes(rollcall_id);
CREATE INDEX IF NOT EXISTS idx_bill_topics_rollcall_id ON bill_topics(rollcall_id);
CREATE INDEX IF NOT EXISTS idx_contributions_donor_id ON contributions(donor_id);
CREATE INDEX IF NOT EXISTS idx_contributions_recipient_id ON contributions(recipient_id);
CREATE INDEX IF NOT EXISTS idx_contributions_election_cycle ON contributions(election_cycle);

CREATE MATERIALIZED VIEW canonical_politician_industry_summary AS
SELECT
    p.politician_id,
    contributions.industry,
    COUNT(*) AS contribution_count,
    SUM(contributions.amount) AS total_amount,
    AVG(contributions.amount) AS avg_amount
FROM contributions
JOIN politicians p ON p.politician_id = contributions.recipient_id
GROUP BY p.politician_id, contributions.industry;
