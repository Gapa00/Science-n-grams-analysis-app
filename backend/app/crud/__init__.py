# backend/app/crud/__init__.py
from .ngram import ngram_crud
from .hierarchy import hierarchy_crud

__all__ = ["ngram_crud", "hierarchy_crud"]