"""Donors API endpoints."""

from decimal import Decimal

from fastapi import APIRouter, HTTPException

from api.database import DbDep

router = APIRouter(tags=["donors"])


def decimal_to_float(val: Decimal | None) -> float | None:
    """Convert Decimal to float for JSON serialization."""
    return float(val) if val is not None else None


@router.get("/donors/search")
async def search_donors(db: DbDep, name: str) -> list[dict]:
    """Search donors by name."""
    async with db.cursor() as cur:
        await cur.execute(
            """
            SELECT donor_id, name, donor_type, igcat, employer, occupation, state,
                   total_contributions_count, total_amount
            FROM donors
            WHERE name ILIKE %s
            ORDER BY total_amount DESC
            LIMIT 20
            """,
            (f"%{name}%",),
        )
        rows = await cur.fetchall()
        return [
            {
                "donor_id": row["donor_id"],
                "name": row["name"],
                "donor_type": row["donor_type"],
                "igcat": row["igcat"],
                "employer": row["employer"],
                "occupation": row["occupation"],
                "state": row["state"],
                "total_contributions_count": row["total_contributions_count"],
                "total_amount": decimal_to_float(row["total_amount"]),
            }
            for row in rows
        ]


@router.get("/donor/{donor_id}")
async def get_donor(db: DbDep, donor_id: str) -> dict:
    """Get a single donor by ID."""
    async with db.cursor() as cur:
        await cur.execute(
            """
            SELECT donor_id, name, donor_type, igcat, employer, occupation, state,
                   total_contributions_count, total_amount
            FROM donors
            WHERE donor_id = %s
            """,
            (donor_id,),
        )
        row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Donor not found")

        return {
            "donor_id": row["donor_id"],
            "name": row["name"],
            "donor_type": row["donor_type"],
            "igcat": row["igcat"],
            "employer": row["employer"],
            "occupation": row["occupation"],
            "state": row["state"],
            "total_contributions_count": row["total_contributions_count"],
            "total_amount": decimal_to_float(row["total_amount"]),
        }


@router.get("/donor/{donor_id}/donations")
async def get_donor_donations(db: DbDep, donor_id: str, limit: int = 100) -> list[dict]:
    """Get all donations made by a specific donor."""
    async with db.cursor() as cur:
        await cur.execute(
            """
            SELECT c.transaction_id, c.amount, c.transaction_date, c.industry,
                   c.election_cycle,
                   p.politician_id, p.first_name, p.last_name, p.full_name,
                   p.party, p.state
            FROM contributions c
            JOIN politicians p ON p.politician_id = c.recipient_id
            WHERE c.donor_id = %s
            ORDER BY c.transaction_date DESC, c.amount DESC
            LIMIT %s
            """,
            (donor_id, limit),
        )
        rows = await cur.fetchall()
        return [
            {
                "transaction_id": row["transaction_id"],
                "amount": decimal_to_float(row["amount"]),
                "transaction_date": str(row["transaction_date"])
                if row["transaction_date"]
                else None,
                "industry": row["industry"],
                "election_cycle": row["election_cycle"],
                "canonical_id": row["politician_id"],
                "first_name": row["first_name"],
                "last_name": row["last_name"],
                "full_name": row["full_name"],
                "party": row["party"],
                "state": row["state"],
            }
            for row in rows
        ]
