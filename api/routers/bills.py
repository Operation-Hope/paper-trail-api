"""Bills API endpoints."""

from fastapi import APIRouter

from api.database import DbDep

router = APIRouter(tags=["bills"])


@router.get("/bills/subjects")
async def get_bill_subjects(db: DbDep) -> dict:
    """Get all unique bill subjects/topics."""
    async with db.cursor() as cur:
        # Get unique topics from bill_topics table if populated
        await cur.execute(
            """
            SELECT DISTINCT topic_label, COUNT(*) as count
            FROM bill_topics
            GROUP BY topic_label
            ORDER BY count DESC
            """
        )
        rows = await cur.fetchall()

        subjects = [{"label": row["topic_label"], "count": row["count"]} for row in rows]

        return {
            "subjects": subjects,
            "total": len(subjects),
        }
