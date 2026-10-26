"""
Plan limits and job validation: the single source of truth for what each plan may run.

Pure Python (no Streamlit/Supabase imports) so it is trivial to test and can be reused by any
entry point that starts a scrape. The authoritative check lives in app.py's start_scrape(), which
validates against a FRESH plan read from Supabase right before the job starts.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List

FREE = "free"
PRO = "pro"


@dataclass(frozen=True)
class PlanLimits:
    max_pages_per_keyword: int
    max_keywords_per_run: int


PLAN_LIMITS: Dict[str, PlanLimits] = {
    FREE: PlanLimits(max_pages_per_keyword=2, max_keywords_per_run=3),
    PRO: PlanLimits(max_pages_per_keyword=10, max_keywords_per_run=20),
}

PLAN_LABELS = {FREE: "Free", PRO: "Pro"}


class PlanLimitError(PermissionError):
    """Raised when a job would exceed the user's plan limits."""


@dataclass
class JobCheck:
    ok: bool
    plan: str
    limits: PlanLimits
    errors: List[str] = field(default_factory=list)


def normalize_plan(plan: Any) -> str:
    """Unknown, missing or malformed plan values are treated as Free (the most restrictive)."""
    return PRO if str(plan or "").strip().lower() == PRO else FREE


def limits_for(plan: Any) -> PlanLimits:
    return PLAN_LIMITS[normalize_plan(plan)]


def describe(plan: Any) -> str:
    limits = limits_for(plan)
    return (f"up to {limits.max_keywords_per_run} keywords and "
            f"{limits.max_pages_per_keyword} pages per keyword per run")


def _as_int(value: Any) -> int:
    """Strict integer parsing: rejects bools, floats with fractions, and non-numeric values."""
    if isinstance(value, bool):
        raise ValueError("not an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    raise ValueError("not an integer")


def validate_job(plan: Any, keywords: Any, max_pages: Any) -> JobCheck:
    """Check a requested job against the plan's limits. Never raises; returns every problem found."""
    plan = normalize_plan(plan)
    limits = PLAN_LIMITS[plan]
    label = PLAN_LABELS[plan]
    errors: List[str] = []

    keyword_list = [k for k in (keywords or []) if isinstance(k, str) and k.strip()]
    if not keyword_list:
        errors.append("Enter at least one keyword.")
    elif len(keyword_list) > limits.max_keywords_per_run:
        errors.append(
            f"The {label} plan allows up to {limits.max_keywords_per_run} keywords per run, "
            f"but you selected {len(keyword_list)}. Remove "
            f"{len(keyword_list) - limits.max_keywords_per_run}"
            + (f" or upgrade to Pro (up to {PLAN_LIMITS[PRO].max_keywords_per_run})." if plan == FREE else "."))

    try:
        pages = _as_int(max_pages)
    except (ValueError, TypeError):
        pages = None
        errors.append("Pages per keyword must be a whole number.")
    if pages is not None:
        if pages < 1:
            errors.append("Pages per keyword must be at least 1.")
        elif pages > limits.max_pages_per_keyword:
            errors.append(
                f"The {label} plan allows up to {limits.max_pages_per_keyword} pages per keyword, "
                f"but you asked for {pages}."
                + (f" Lower it or upgrade to Pro (up to {PLAN_LIMITS[PRO].max_pages_per_keyword})."
                   if plan == FREE else ""))

    return JobCheck(ok=not errors, plan=plan, limits=limits, errors=errors)
