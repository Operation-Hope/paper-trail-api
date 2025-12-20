-- Phase 4: Create All Indexes
-- Federal Legislators Database
-- Coverage: 1980-2024
-- Date: 2025-11-15

\timing on

\echo '============================================================================'
\echo 'Starting Index Creation'
\echo '============================================================================'
\echo 'Database: federal_legislators'
\echo 'Tables: politicians, donors, contributions, rollcalls, votes, bill_topics'
\echo 'Expected Duration: 1-2 hours for 398.7M contributions'
\echo '============================================================================'

\echo ''
\echo '============================================================================'
\echo 'Politicians Table Indexes'
\echo '============================================================================'

-- Name search using trigram similarity
\echo 'Creating idx_politicians_name_search (GIN trigram)...'
CREATE INDEX idx_politicians_name_search
  ON politicians
  USING GIN ((first_name || ' ' || last_name) gin_trgm_ops);

-- Sorting index (active first, then alphabetical)
\echo 'Creating idx_politicians_sort...'
CREATE INDEX idx_politicians_sort
  ON politicians (is_active DESC, last_name ASC, first_name ASC);

-- ICPSR lookup for vote integration
\echo 'Creating idx_politicians_icpsr...'
CREATE INDEX idx_politicians_icpsr
  ON politicians (icpsr_id)
  WHERE icpsr_id IS NOT NULL;

\echo ''
\echo '============================================================================'
\echo 'Donors Table Indexes'
\echo '============================================================================'

-- Name search using trigram similarity
\echo 'Creating idx_donors_name_search (GIN trigram)...'
CREATE INDEX idx_donors_name_search
  ON donors
  USING GIN (name gin_trgm_ops);

-- Sorting index (alphabetical by name)
\echo 'Creating idx_donors_name_sort...'
CREATE INDEX idx_donors_name_sort
  ON donors (name ASC);

-- Donor type filtering
\echo 'Creating idx_donors_type...'
CREATE INDEX idx_donors_type
  ON donors (donor_type);

\echo ''
\echo '============================================================================'
\echo 'Contributions Table Indexes (Largest - 398.7M records)'
\echo '============================================================================'

-- Donor lookup with date sorting (Requirement 7)
\echo 'Creating idx_contributions_donor...'
CREATE INDEX idx_contributions_donor
  ON contributions (donor_id, transaction_date DESC);

-- Recipient + industry aggregation (Requirement 9)
\echo 'Creating idx_contributions_recipient_industry...'
CREATE INDEX idx_contributions_recipient_industry
  ON contributions (recipient_id, industry, transaction_date);

-- Recipient lookup with date sorting
\echo 'Creating idx_contributions_recipient_date...'
CREATE INDEX idx_contributions_recipient_date
  ON contributions (recipient_id, transaction_date DESC);

-- Date range filtering
\echo 'Creating idx_contributions_date...'
CREATE INDEX idx_contributions_date
  ON contributions (transaction_date);

-- Election cycle filtering
\echo 'Creating idx_contributions_cycle...'
CREATE INDEX idx_contributions_cycle
  ON contributions (election_cycle);

\echo ''
\echo '============================================================================'
\echo 'Rollcalls Table Indexes'
\echo '============================================================================'

-- Congress filtering
\echo 'Creating idx_rollcalls_congress...'
CREATE INDEX idx_rollcalls_congress
  ON rollcalls (congress);

-- Date sorting for voting history
\echo 'Creating idx_rollcalls_date...'
CREATE INDEX idx_rollcalls_date
  ON rollcalls (vote_date DESC);

-- Bill number lookup
\echo 'Creating idx_rollcalls_number...'
CREATE INDEX idx_rollcalls_number
  ON rollcalls (bill_number)
  WHERE bill_number IS NOT NULL;

-- Procedural votes filtering
\echo 'Creating idx_rollcalls_procedural...'
CREATE INDEX idx_rollcalls_procedural
  ON rollcalls (is_procedural)
  WHERE is_procedural = TRUE;

\echo ''
\echo '============================================================================'
\echo 'Votes Table Indexes (10.3M records)'
\echo '============================================================================'

-- Politician voting history (Requirement 8)
-- Note: Primary key (politician_id, rollcall_id) already creates index
\echo 'Primary key index on (politician_id, rollcall_id) already exists'

-- Rollcall lookup for reverse queries
\echo 'Creating idx_votes_rollcall...'
CREATE INDEX idx_votes_rollcall
  ON votes (rollcall_id);

-- Vote value filtering
\echo 'Creating idx_votes_value...'
CREATE INDEX idx_votes_value
  ON votes (vote_value);

\echo ''
\echo '============================================================================'
\echo 'Bill Topics Table Indexes'
\echo '============================================================================'

-- Topic label filtering (Requirement 10)
\echo 'Creating idx_bill_topics_label...'
CREATE INDEX IF NOT EXISTS idx_bill_topics_label
  ON bill_topics (topic_label);

\echo ''
\echo '============================================================================'
\echo 'Index Creation Complete'
\echo '============================================================================'
\echo 'Next steps:'
\echo '  1. Run ANALYZE on all tables (scripts/analyze_tables.sql)'
\echo '  2. Validate index usage (scripts/validate_indexes.py)'
\echo '  3. Run VACUUM FULL (scripts/vacuum_database.sql)'
\echo '  4. Benchmark queries (scripts/benchmark_queries.py)'
\echo '============================================================================'
