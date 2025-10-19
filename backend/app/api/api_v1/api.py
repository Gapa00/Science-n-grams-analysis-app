from fastapi import APIRouter
from app.api.api_v1 import leaderboard, filters
from app.api.api_v1 import vote
from app.api.api_v1 import bursts


api_router = APIRouter()

api_router.include_router(leaderboard.router, prefix="", tags=["leaderboard"])
api_router.include_router(filters.router, prefix="", tags=["filters"])
api_router.include_router(bursts.router, prefix="", tags=["bursts"])
api_router.include_router(vote.router, prefix="", tags=["vote"])
