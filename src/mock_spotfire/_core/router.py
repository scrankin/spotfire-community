"""Router registration for core mock endpoints."""

from fastapi import APIRouter

from .paths import oauth2_token


router = APIRouter()


router.add_api_route(
    path="/spotfire/oauth2/token",
    endpoint=oauth2_token,
    methods=["POST"],
    include_in_schema=True,
)


__all__ = [
    "router",
]
