"""FastAPI application entry point."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import bills, donors, health, politicians

app = FastAPI(
    title="Paper Trail API",
    description="Political campaign finance and voting record transparency API",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(politicians.router, prefix="/api")
app.include_router(donors.router, prefix="/api")
app.include_router(bills.router, prefix="/api")
