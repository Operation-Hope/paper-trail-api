"""Health check endpoints."""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check() -> dict[str, str]:
    """Basic health check endpoint."""
    return {"status": "healthy"}
