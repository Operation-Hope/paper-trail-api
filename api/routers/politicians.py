"""Politicians API endpoints."""

from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from api.database import DbDep

router = APIRouter(tags=["politicians"])


def decimal_to_float(val: Decimal | None) -> float | None:
    """Convert Decimal to float for JSON serialization."""
    return float(val) if val is not None else None


@router.get("/politicians/search")
async def search_politicians(db: DbDep, name: str) -> list[dict]:
    """Search politicians by name."""
    async with db.cursor() as cur:
        await cur.execute(
            """
            SELECT politician_id, first_name, last_name, full_name, party, state, seat,
                   is_active, nominate_dim1, nominate_dim2
            FROM politicians
            WHERE full_name ILIKE %s
            ORDER BY full_name
            LIMIT 20
            """,
            (f"%{name}%",),
        )
        rows = await cur.fetchall()
        return [
            {
                "canonical_id": row["politician_id"],
                "first_name": row["first_name"],
                "last_name": row["last_name"],
                "full_name": row["full_name"],
                "party": row["party"],
                "state": row["state"],
                "seat": row["seat"],
                "is_active": row["is_active"],
                "nominate_dim1": decimal_to_float(row["nominate_dim1"]),
                "nominate_dim2": decimal_to_float(row["nominate_dim2"]),
            }
            for row in rows
        ]


@router.get("/politician/{politician_id}")
async def get_politician(db: DbDep, politician_id: str) -> dict:
    """Get a single politician by ID."""
    async with db.cursor() as cur:
        await cur.execute(
            """
            SELECT politician_id, first_name, last_name, full_name, party, state, seat,
                   is_active, is_placeholder, placeholder_type, icpsr_id, bioguide_id,
                   nominate_dim1, nominate_dim2, first_elected_year, last_elected_year
            FROM politicians
            WHERE politician_id = %s
            """,
            (politician_id,),
        )
        row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Politician not found")

        return {
            "canonical_id": row["politician_id"],
            "first_name": row["first_name"],
            "last_name": row["last_name"],
            "full_name": row["full_name"],
            "party": row["party"],
            "state": row["state"],
            "seat": row["seat"],
            "is_active": row["is_active"],
            "is_placeholder": row["is_placeholder"],
            "placeholder_type": row["placeholder_type"],
            "icpsr_id": row["icpsr_id"],
            "bioguide_id": row["bioguide_id"],
            "nominate_dim1": decimal_to_float(row["nominate_dim1"]),
            "nominate_dim2": decimal_to_float(row["nominate_dim2"]),
            "first_elected_year": row["first_elected_year"],
            "last_elected_year": row["last_elected_year"],
        }


@router.get("/politician/{politician_id}/votes")
async def get_politician_votes(
    db: DbDep,
    politician_id: str,
    page: int = 1,
    per_page: int = 20,
    sort: str | None = None,
    type: Annotated[list[str] | None, Query()] = None,
    subject: Annotated[list[str] | None, Query()] = None,
    date_from: str | None = None,
    date_to: str | None = None,
    vote_value: Annotated[list[str] | None, Query()] = None,
    search: str | None = None,
) -> dict:
    """Get paginated voting history for a politician."""
    async with db.cursor() as cur:
        # Build query with filters
        conditions = ["v.politician_id = %s"]
        params: list = [politician_id]

        if date_from:
            conditions.append("r.vote_date >= %s")
            params.append(date_from)
        if date_to:
            conditions.append("r.vote_date <= %s")
            params.append(date_to)
        if vote_value:
            conditions.append("v.vote_value = ANY(%s)")
            params.append(vote_value)
        if search:
            conditions.append("(r.bill_number ILIKE %s OR r.bill_description ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])

        where_clause = " AND ".join(conditions)

        # Get total count
        await cur.execute(
            f"""
            SELECT COUNT(*)
            FROM votes v
            JOIN rollcalls r ON r.rollcall_id = v.rollcall_id
            WHERE {where_clause}
            """,
            params,
        )
        total = (await cur.fetchone())["count"]

        # Calculate pagination
        offset = (page - 1) * per_page
        total_pages = (total + per_page - 1) // per_page if total > 0 else 0

        # Sort handling
        order_by = "r.vote_date DESC, r.rollcall_id DESC"
        if sort == "date_asc":
            order_by = "r.vote_date ASC, r.rollcall_id ASC"
        elif sort == "bill_number":
            order_by = "r.bill_number ASC"

        # Get votes
        query_params = [*params, per_page, offset]
        await cur.execute(
            f"""
            SELECT v.vote_id, v.vote_value, r.rollcall_id, r.congress, r.chamber,
                   r.rollnumber, r.bill_number, r.bill_description, r.vote_date, r.vote_result
            FROM votes v
            JOIN rollcalls r ON r.rollcall_id = v.rollcall_id
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
            """,
            query_params,
        )
        rows = await cur.fetchall()

        votes = [
            {
                "canonical_id": politician_id,
                "vote_id": row["vote_id"],
                "vote_value": row["vote_value"],
                "rollcall_id": row["rollcall_id"],
                "congress": row["congress"],
                "chamber": row["chamber"],
                "rollnumber": row["rollnumber"],
                "bill_number": row["bill_number"],
                "bill_description": row["bill_description"],
                "vote_date": str(row["vote_date"]) if row["vote_date"] else None,
                "vote_result": row["vote_result"],
                "has_topics": False,
                "topics": [],
            }
            for row in rows
        ]

        return {
            "votes": votes,
            "pagination": {
                "currentPage": page,
                "totalPages": total_pages,
                "totalVotes": total,
            },
            "metadata": {
                "topic_coverage": "",
            },
        }


@router.get("/politician/{politician_id}/votes/date-range")
async def get_politician_votes_date_range(db: DbDep, politician_id: str) -> dict:
    """Get date range metadata for a politician's voting history."""
    async with db.cursor() as cur:
        await cur.execute(
            """
            SELECT
                MIN(r.vote_date) as min_date,
                MAX(r.vote_date) as max_date,
                ARRAY_AGG(DISTINCT r.congress ORDER BY r.congress) as congresses
            FROM votes v
            JOIN rollcalls r ON r.rollcall_id = v.rollcall_id
            WHERE v.politician_id = %s
            """,
            (politician_id,),
        )
        row = await cur.fetchone()

        congress_sessions = []
        if row["congresses"]:
            for congress in row["congresses"]:
                if congress:
                    start_year = 1787 + (congress * 2)
                    end_year = start_year + 2
                    congress_sessions.append(
                        {
                            "congress": congress,
                            "start": f"{start_year}-01-03",
                            "end": f"{end_year}-01-03",
                        }
                    )

        return {
            "earliest_vote": str(row["min_date"]) if row["min_date"] else None,
            "latest_vote": str(row["max_date"]) if row["max_date"] else None,
            "congress_sessions": congress_sessions,
        }


@router.get("/politician/{politician_id}/donations/summary")
async def get_donation_summary(db: DbDep, politician_id: str) -> list[dict]:
    """Get donation summary grouped by industry for a politician."""
    async with db.cursor() as cur:
        await cur.execute(
            """
            SELECT industry, contribution_count, total_amount, avg_amount
            FROM canonical_politician_industry_summary
            WHERE politician_id = %s
            ORDER BY total_amount DESC
            """,
            (politician_id,),
        )
        rows = await cur.fetchall()
        return [
            {
                "industry": row["industry"],
                "contribution_count": row["contribution_count"],
                "total_amount": decimal_to_float(row["total_amount"]),
                "avg_amount": decimal_to_float(row["avg_amount"]),
            }
            for row in rows
        ]


@router.get("/politician/{politician_id}/donations/summary/filtered")
async def get_filtered_donation_summary(db: DbDep, politician_id: str, topic: str) -> dict:
    """Get donation summary filtered by bill topic."""
    # Note: This would require bill_topics to be populated with topic data
    # For now, return empty result since we don't have topic data loaded
    return {
        "donations": [],
        "topic": topic,
        "politician_id": politician_id,
    }
