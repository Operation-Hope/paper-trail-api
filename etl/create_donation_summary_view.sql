-- Materialized view for donation summaries with accurate transaction type filtering
--
-- This view filters contributions by transaction type to ensure only actual donations
-- are counted, excluding transfers, refunds, and independent expenditures.
--
-- Individual contribution types (15, 15E, 15C, 11):
-- - 15: Contribution from individual/partnership
-- - 15E: Earmarked contribution from individual
-- - 15C: Contribution from candidate (self-funding)
-- - 11: Native American tribe contribution
--
-- PAC/Committee contribution types (18K, 18U, 10):
-- - 18K: Contribution from registered filer (PACs, party committees)
-- - 18U: Contribution from unregistered committee
-- - 10: Contribution to Super PACs
--
-- This fixes the accuracy problem where we were counting ALL committee transactions
-- instead of just actual contributions (e.g., Bernie Sanders showed $29M when he
-- actually received only $275K in PAC donations).

-- Step 1: Drop existing view (data will be regenerated from contributions table)
DROP MATERIALIZED VIEW IF EXISTS canonical_politician_donation_summary;

-- Step 2: Create the new view with correct filtering
CREATE MATERIALIZED VIEW canonical_politician_donation_summary AS
SELECT
    c.canonical_id,
    d.donor_type,
    CASE
        WHEN d.donor_type = 'C' THEN c.industry
        ELSE NULL
    END AS industry,
    COUNT(*) AS contribution_count,
    SUM(c.amount) AS total_amount,
    AVG(c.amount) AS avg_amount
FROM contributions c
JOIN donors d ON c.donor_id = d.donor_id
WHERE c.canonical_id IS NOT NULL
  AND (
    -- Individual contributions
    (d.donor_type = 'I' AND c.transaction_type IN ('15', '15E', '15C', '11'))
    OR
    -- PAC/Committee contributions (actual donations, not transfers/refunds)
    (d.donor_type = 'C' AND c.transaction_type IN ('18K', '18U', '10'))
  )
GROUP BY c.canonical_id, d.donor_type,
    CASE
        WHEN d.donor_type = 'C' THEN c.industry
        ELSE NULL
    END;

-- Step 3: Create indexes for performance
CREATE INDEX idx_donation_summary_canonical
    ON canonical_politician_donation_summary(canonical_id);

CREATE INDEX idx_donation_summary_donor_type
    ON canonical_politician_donation_summary(canonical_id, donor_type);

CREATE INDEX idx_donation_summary_industry
    ON canonical_politician_donation_summary(canonical_id, industry)
    WHERE industry IS NOT NULL;

-- Step 4: Show statistics
DO $$
DECLARE
    new_count BIGINT;
BEGIN
    -- Count rows in new view
    SELECT COUNT(*) INTO new_count
    FROM canonical_politician_donation_summary;

    RAISE NOTICE 'New view created with % rows', new_count;
END $$;
