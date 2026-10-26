"""
Supabase helpers for the Streamlit app (auth + reading the user's plan).

No Streamlit imports here, so this module is easy to test. It only talks to Supabase with the
PUBLISHABLE (formerly "anon") key plus the signed-in user's own session. Row Level Security (see
supabase_schema.sql) means the user can read only their own row and can never change their plan.
The secret / service-role key must NEVER be used in the app; it belongs to the webhook service only.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

FREE = "free"
PRO = "pro"


@dataclass
class AuthResult:
    ok: bool
    message: str = ""
    user_id: str = ""
    email: str = ""
    username: str = ""
    needs_confirmation: bool = False  # sign-up succeeded but the user must confirm their email


def make_client(url: str, key: str):
    """Create a Supabase client. Create ONE PER USER SESSION (it holds that user's login token);
    never share one across users with st.cache_resource."""
    from supabase import create_client  # imported lazily so the app can start without the package

    return create_client(url, key)


def friendly_error(exc: BaseException) -> str:
    """Turn Supabase/GoTrue exceptions into short messages that are safe to show to users."""
    text = (getattr(exc, "message", None) or str(exc) or "").strip()
    low = text.lower()
    if "invalid login credentials" in low:
        return "Incorrect email or password."
    if "email not confirmed" in low:
        return "Please confirm your email first (check your inbox), then log in."
    if "already registered" in low or "already been registered" in low:
        return "An account with this email already exists. Try logging in."
    if "password should be" in low or "weak password" in low:
        return text
    if "rate limit" in low or "too many" in low:
        return "Too many attempts. Please wait a minute and try again."
    if "invalid api key" in low or "invalid url" in low:
        return ("Server configuration error: check SUPABASE_URL and the Supabase key. "
                "If you use an sb_publishable_ key, upgrade the 'supabase' package "
                "(pip install -U supabase) or use the legacy anon key.")
    return text or "Something went wrong. Please try again."


def _metadata_username(user: Any) -> str:
    metadata = getattr(user, "user_metadata", None) or {}
    return str(metadata.get("username") or "")


def sign_in(client, email: str, password: str) -> AuthResult:
    try:
        res = client.auth.sign_in_with_password({"email": email.strip(), "password": password})
    except Exception as exc:
        return AuthResult(False, friendly_error(exc))
    user = getattr(res, "user", None)
    session = getattr(res, "session", None)
    if user is None or session is None:
        return AuthResult(False, "Could not sign in. Please try again.")
    return AuthResult(True, user_id=str(user.id), email=user.email or email.strip(),
                      username=_metadata_username(user))


def sign_up(client, email: str, password: str, username: str) -> AuthResult:
    try:
        res = client.auth.sign_up({
            "email": email.strip(),
            "password": password,
            "options": {"data": {"username": username.strip()}},  # read by the DB trigger
        })
    except Exception as exc:
        return AuthResult(False, friendly_error(exc))
    user = getattr(res, "user", None)
    session = getattr(res, "session", None)
    if user is None:
        return AuthResult(False, "Could not create the account. Please try again.")
    if session is None:  # email confirmation is switched on in the Supabase project
        return AuthResult(True, "Account created. Check your email to confirm it, then log in.",
                          needs_confirmation=True)
    return AuthResult(True, user_id=str(user.id), email=user.email or email.strip(),
                      username=username.strip())


def sign_out(client) -> None:
    try:
        client.auth.sign_out()
    except Exception:
        pass  # local state is cleared by the caller regardless


def session_is_valid(client) -> bool:
    """True while the client holds a usable session (supabase-py refreshes expired tokens here)."""
    try:
        return client.auth.get_session() is not None
    except Exception:
        return False


def fetch_profile(client, user_id: str) -> Optional[Dict[str, Any]]:
    """The signed-in user's own row from public.users, or None if it could not be read."""
    try:
        res = (client.table("users")
               .select("username,email,subscription_status,stripe_customer_id")
               .eq("id", user_id)
               .limit(1)
               .execute())
    except Exception:
        return None
    rows = getattr(res, "data", None) or []
    return rows[0] if rows else None


def normalize_plan(status: Optional[str]) -> str:
    """Anything other than an explicit 'pro' is treated as free."""
    return PRO if str(status or "").strip().lower() == PRO else FREE


@dataclass
class VerifiedPlan:
    plan: str            # "free" or "pro"
    verified: bool       # True only if BOTH the login token and the database row were checked just now
    user_id: str = ""
    reason: str = ""     # why verification failed (for logs/UI)


def verify_user_and_plan(client) -> VerifiedPlan:
    """
    Authoritative, uncached plan check used before running a scrape job.

    1. auth.get_user() makes Supabase Auth validate the access token on the server (not just a
       locally stored session), and tells us who the user really is.
    2. The `users` row is read with that user's token, so Row Level Security only ever returns
       their own row.

    Fails CLOSED: if anything cannot be verified the plan is "free", so quotas still apply.
    """
    try:
        res = client.auth.get_user()
        user = getattr(res, "user", None)
    except Exception:
        user = None
    if user is None:
        return VerifiedPlan(FREE, False, reason="login session could not be verified")

    user_id = str(user.id)
    profile = fetch_profile(client, user_id)
    if profile is None:
        return VerifiedPlan(FREE, False, user_id, "subscription status could not be read")
    return VerifiedPlan(normalize_plan(profile.get("subscription_status")), True, user_id)