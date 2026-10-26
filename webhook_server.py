"""
Stripe -> Supabase webhook service  (Phase 2: signature-verified, idempotent plan updates).

Streamlit cannot receive HTTP webhooks, so this is a small separate web service (FastAPI). Stripe
calls it after payments and subscription changes; it flips `users.subscription_status` between
'pro' and 'free' in Supabase. The Streamlit app only READS that column.

SECURITY PROPERTIES
  1. Every request is verified with Stripe's official library (Webhook.construct_event) using
     STRIPE_WEBHOOK_SECRET against the RAW request body. Forged, tampered or replayed-too-late
     requests (older than 5 minutes) get HTTP 400 and touch nothing.
  2. Only after verification does the service use the Supabase SECRET key (bypasses RLS). That key
     exists only in this service's environment, never in the Streamlit app.
  3. Idempotent: each Stripe event id is claimed in the `stripe_events` table before processing, so
     duplicate or retried deliveries are acknowledged but applied once. Handlers also set absolute
     states (pro/free) and re-check the subscription's CURRENT status with Stripe, so out-of-order
     events cannot leave a user on the wrong plan.
  4. Failures return HTTP 500 (Stripe retries with backoff); permanent mismatches (unknown user)
     return 200 and are logged, to avoid retry storms.

EVENTS TO ENABLE (Stripe Dashboard -> Developers -> Webhooks -> Add endpoint)
    checkout.session.completed              -> upgrade to Pro
    checkout.session.async_payment_succeeded-> upgrade to Pro (delayed payment methods)
    customer.subscription.updated           -> Pro while active/trialing, Free once ended
    customer.subscription.deleted           -> Free
    invoice.payment_failed                  -> Free once Stripe has given up retrying

RUN
    pip install -r requirements-webhook.txt
    uvicorn webhook_server:create_app --factory --host 0.0.0.0 --port 8000

ENVIRONMENT (this service only)
    STRIPE_SECRET_KEY        sk_live_... / sk_test_...
    STRIPE_WEBHOOK_SECRET    whsec_...  (shown after you add the endpoint in Stripe)
    SUPABASE_URL             https://<project-ref>.supabase.co
    SUPABASE_SECRET_KEY      sb_secret_...  (legacy: SUPABASE_SERVICE_ROLE_KEY also accepted)
    DOWNGRADE_ON_PAST_DUE    optional; "1" = drop to Free the moment a payment fails (default: keep
                             Pro while Stripe retries the card)
    LOG_LEVEL                optional; INFO by default

LOCAL TEST
    stripe listen --forward-to localhost:8000/stripe/webhook      # prints a whsec_ for local use
    stripe trigger checkout.session.completed
    curl -i -X POST localhost:8000/stripe/webhook -d '{}'          # must be 400 (no signature)
"""

import json
import logging
import os
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Protocol, Tuple

log = logging.getLogger("stripe_webhook")

FREE = "free"
PRO = "pro"
MAX_BODY_BYTES = 1_000_000  # Stripe events are far smaller; anything bigger is not from Stripe

ACTIVE_STATUSES = {"active", "trialing"}
ENDED_STATUSES = {"canceled", "unpaid", "incomplete_expired"}

CHECKOUT_EVENTS = {"checkout.session.completed", "checkout.session.async_payment_succeeded"}
SUBSCRIPTION_EVENTS = {"customer.subscription.updated", "customer.subscription.deleted"}
INVOICE_EVENTS = {"invoice.payment_failed"}
HANDLED_EVENTS = CHECKOUT_EVENTS | SUBSCRIPTION_EVENTS | INVOICE_EVENTS

LatestStatus = Callable[[str], Optional[str]]  # subscription id -> its current Stripe status


@dataclass(frozen=True)
class Policy:
    # False: keep Pro while Stripe retries a failed card (recommended; matches Stripe Smart Retries).
    # True: downgrade as soon as the subscription goes past_due.
    downgrade_on_past_due: bool = False

    @classmethod
    def from_env(cls) -> "Policy":
        return cls(downgrade_on_past_due=os.environ.get("DOWNGRADE_ON_PAST_DUE", "").strip().lower()
                   in ("1", "true", "yes", "on"))


class InvalidSignature(Exception):
    """The Stripe-Signature header does not match the payload (forged, tampered or too old)."""


class InvalidPayload(Exception):
    """The body is not a valid Stripe event."""


# --------------------------------------------------------------------------- #
# Data access (tiny interfaces so the logic is easy to test)
# --------------------------------------------------------------------------- #
class UsersRepo(Protocol):
    def find_by_id(self, user_id: str) -> Optional[Dict[str, Any]]: ...
    def find_by_customer(self, customer_id: str) -> Optional[Dict[str, Any]]: ...
    def find_by_email(self, email: str) -> Optional[Dict[str, Any]]: ...
    def update(self, user_id: str, fields: Dict[str, Any]) -> None: ...


class EventStore(Protocol):
    def claim(self, event_id: str, event_type: str) -> bool: ...   # True = you own this event now
    def mark_processed(self, event_id: str, result: str) -> None: ...
    def release(self, event_id: str) -> None: ...                  # let Stripe's retry reprocess it


class SupabaseUsersRepo:
    """public.users access with the SECRET key (bypasses RLS, so keep this service private)."""

    COLUMNS = "id,email,subscription_status,stripe_customer_id"

    def __init__(self, client) -> None:
        self.db = client

    def _one(self, query) -> Optional[Dict[str, Any]]:
        rows = query.limit(1).execute().data or []
        return rows[0] if rows else None

    def find_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        return self._one(self.db.table("users").select(self.COLUMNS).eq("id", user_id))

    def find_by_customer(self, customer_id: str) -> Optional[Dict[str, Any]]:
        return self._one(self.db.table("users").select(self.COLUMNS).eq("stripe_customer_id", customer_id))

    def find_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        return self._one(self.db.table("users").select(self.COLUMNS).eq("email", email.strip().lower()))

    def update(self, user_id: str, fields: Dict[str, Any]) -> None:
        self.db.table("users").update(fields).eq("id", user_id).execute()


class SupabaseEventStore:
    """
    Idempotency ledger in public.stripe_events (see supabase_schema.sql).
    claim() is ONE atomic SQL call (rpc claim_stripe_event): it inserts the event id, or re-claims a
    'processing' row that has been stuck for STALE_AFTER_SECONDS (a crashed worker). It returns
    False if another delivery already processed or is currently processing the event.
    """

    STALE_AFTER_SECONDS = 300

    def __init__(self, client) -> None:
        self.db = client

    def claim(self, event_id: str, event_type: str) -> bool:
        res = self.db.rpc("claim_stripe_event", {
            "p_id": event_id, "p_type": event_type, "p_stale_after_seconds": self.STALE_AFTER_SECONDS,
        }).execute()
        return bool(res.data)

    def mark_processed(self, event_id: str, result: str) -> None:
        self.db.table("stripe_events").update(
            {"status": "processed", "result": (result or "")[:200]}).eq("id", event_id).execute()

    def release(self, event_id: str) -> None:
        self.db.table("stripe_events").delete().eq("id", event_id).eq("status", "processing").execute()


# --------------------------------------------------------------------------- #
# Business logic
# --------------------------------------------------------------------------- #
def plan_for_subscription_status(status: Optional[str], policy: Policy = Policy()) -> Optional[str]:
    """'pro' / 'free', or None when the status should not change the plan."""
    if status in ACTIVE_STATUSES:
        return PRO
    if status in ENDED_STATUSES:
        return FREE
    if status == "past_due" and policy.downgrade_on_past_due:
        return FREE
    return None  # past_due (Stripe still retrying), incomplete, paused, unknown


def _valid_uuid(value: Optional[str]) -> Optional[str]:
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, AttributeError, TypeError):
        return None


def _apply_plan(user: Dict[str, Any], plan: str, repo: UsersRepo, why: str) -> str:
    if user.get("subscription_status") == plan:
        return f"already {plan}"
    repo.update(user["id"], {"subscription_status": plan})
    log.info("user %s -> %s (%s)", user["id"], plan, why)
    return f"set {plan}"


def _current_status(latest_status: Optional[LatestStatus], subscription_id: Optional[str]) -> Optional[str]:
    """Ask Stripe for the subscription's CURRENT status; None if unavailable (callers then fall back)."""
    if not latest_status or not subscription_id:
        return None
    try:
        return latest_status(subscription_id)
    except Exception:
        log.exception("could not re-fetch subscription %s from Stripe", subscription_id)
        return None


def _find_user_for_checkout(session: Dict[str, Any], repo: UsersRepo) -> Optional[Dict[str, Any]]:
    """Match a paid Checkout Session to one of our users."""
    # 1) Best: the Supabase user id the app put in the Payment Link as client_reference_id.
    user_id = _valid_uuid(session.get("client_reference_id"))
    if user_id:
        user = repo.find_by_id(user_id)
        if user:
            return user
    # 2) Fallback: the email entered/pre-filled at checkout.
    email = (session.get("customer_details") or {}).get("email") or session.get("customer_email")
    if email:
        return repo.find_by_email(email)
    return None


def on_checkout_completed(session: Dict[str, Any], repo: UsersRepo,
                          latest_status: Optional[LatestStatus] = None) -> str:
    if session.get("payment_status") not in ("paid", "no_payment_required"):
        return "ignored: payment not completed yet"  # delayed methods fire async_payment_succeeded later

    user = _find_user_for_checkout(session, repo)
    if not user:
        log.error("checkout %s: no matching user (client_reference_id=%r)",
                  session.get("id"), session.get("client_reference_id"))
        return "unmatched"

    # A late/replayed checkout event must not resurrect a subscription that has already ended.
    if _current_status(latest_status, session.get("subscription")) in ENDED_STATUSES:
        return "ignored: subscription already ended"

    fields: Dict[str, Any] = {"subscription_status": PRO}
    customer = session.get("customer")
    if customer:
        owner = repo.find_by_customer(customer)
        if owner and owner["id"] != user["id"]:
            log.error("checkout %s: Stripe customer %s already belongs to user %s, not %s; needs a manual look",
                      session.get("id"), customer, owner["id"], user["id"])
            return "conflict: customer belongs to another user"
        current = user.get("stripe_customer_id")
        if current and current != customer and user.get("subscription_status") == PRO:
            log.warning("user %s is already Pro with customer %s; ignoring new customer %s",
                        user["id"], current, customer)
        else:
            fields["stripe_customer_id"] = customer

    if all(user.get(k) == v for k, v in fields.items()):
        return "already pro"
    repo.update(user["id"], fields)
    log.info("user %s upgraded to pro (checkout %s)", user["id"], session.get("id"))
    return "upgraded"


def on_subscription_changed(subscription: Dict[str, Any], repo: UsersRepo,
                            latest_status: Optional[LatestStatus] = None,
                            policy: Policy = Policy()) -> str:
    # Events can arrive out of order, so prefer the subscription's CURRENT status from Stripe.
    status = _current_status(latest_status, subscription.get("id")) or subscription.get("status")
    new_plan = plan_for_subscription_status(status, policy)
    if new_plan is None:
        return f"no change (status={status})"
    customer_id = subscription.get("customer")
    user = repo.find_by_customer(customer_id) if customer_id else None
    if not user:
        log.warning("subscription event for unknown customer %r", customer_id)
        return "unmatched"
    return _apply_plan(user, new_plan, repo, f"subscription status={status}")


def _subscription_id_from_invoice(invoice: Dict[str, Any]) -> Optional[str]:
    sub = invoice.get("subscription")  # older Stripe API versions
    if not sub:
        sub = ((invoice.get("parent") or {}).get("subscription_details") or {}).get("subscription")  # newer
    if isinstance(sub, dict):
        sub = sub.get("id")
    return sub if isinstance(sub, str) else None


def on_invoice_payment_failed(invoice: Dict[str, Any], repo: UsersRepo,
                              latest_status: Optional[LatestStatus] = None,
                              policy: Policy = Policy()) -> str:
    """
    A renewal payment failed. This handler can only DOWNGRADE, never upgrade.
      * Stripe will retry (next_payment_attempt is set): keep Pro unless policy says otherwise.
      * Final failure (no next attempt) or the subscription is already ended: downgrade to Free.
    """
    status = _current_status(latest_status, _subscription_id_from_invoice(invoice))
    plan = plan_for_subscription_status(status, policy)
    if plan is None and invoice.get("next_payment_attempt") is None:
        plan = FREE  # Stripe has stopped retrying: stop granting Pro
    if plan != FREE:
        return "no change (payment failed, Stripe will retry)"

    customer_id = invoice.get("customer")
    user = repo.find_by_customer(customer_id) if customer_id else None
    if not user:
        log.warning("payment_failed for unknown customer %r", customer_id)
        return "unmatched"
    return _apply_plan(user, FREE, repo, "payment failed, no more retries")


def handle_event(event: Dict[str, Any], repo: UsersRepo, latest_status: Optional[LatestStatus] = None,
                 policy: Policy = Policy()) -> str:
    """Dispatch one VERIFIED Stripe event (plain dict)."""
    event_type = event.get("type", "")
    obj = (event.get("data") or {}).get("object") or {}
    if event_type in CHECKOUT_EVENTS:
        return on_checkout_completed(obj, repo, latest_status)
    if event_type in SUBSCRIPTION_EVENTS:
        return on_subscription_changed(obj, repo, latest_status, policy)
    if event_type in INVOICE_EVENTS:
        return on_invoice_payment_failed(obj, repo, latest_status, policy)
    return "ignored"


# --------------------------------------------------------------------------- #
# The request pipeline (framework-free, so every branch can be unit-tested)
# --------------------------------------------------------------------------- #
def make_stripe_verifier(stripe_module, endpoint_secret: str) -> Callable[[bytes, str], Dict[str, Any]]:
    """Wrap Stripe's official signature check. Returns the event as a plain dict."""
    signature_error = (getattr(stripe_module, "SignatureVerificationError", None)
                       or stripe_module.error.SignatureVerificationError)

    def verify(payload: bytes, signature: str) -> Dict[str, Any]:
        try:
            # Checks the HMAC-SHA256 of "<timestamp>.<raw body>" with the whsec_ secret and rejects
            # timestamps older than 5 minutes (replay protection).
            stripe_module.Webhook.construct_event(payload, signature, endpoint_secret)
        except signature_error as exc:
            raise InvalidSignature(str(exc)) from exc
        except ValueError as exc:
            raise InvalidPayload(str(exc)) from exc
        return json.loads(payload)  # verified above; use a plain dict from here on

    return verify


def process_webhook(payload: bytes, signature: Optional[str], *,
                    verify: Callable[[bytes, str], Dict[str, Any]],
                    repo: UsersRepo, events: EventStore,
                    latest_status: Optional[LatestStatus] = None,
                    policy: Policy = Policy()) -> Tuple[int, Dict[str, Any]]:
    """Verify -> dedupe -> handle -> record. Returns (http_status, json_body)."""
    if len(payload) > MAX_BODY_BYTES:
        return 413, {"error": "payload too large"}
    if not signature:
        return 400, {"error": "missing signature"}

    # 1) Authenticity: nothing below runs for a request that Stripe did not sign.
    try:
        event = verify(payload, signature)
    except InvalidSignature:
        log.warning("rejected webhook: invalid signature")
        return 400, {"error": "invalid signature"}
    except InvalidPayload:
        return 400, {"error": "invalid payload"}

    event_id, event_type = event.get("id"), event.get("type")
    if event_type not in HANDLED_EVENTS:
        return 200, {"received": True, "result": "ignored"}  # acknowledge events we don't use
    if not event_id:
        return 400, {"error": "event has no id"}

    # 2) Idempotency: only one delivery of an event id may process it.
    try:
        if not events.claim(event_id, event_type):
            log.info("duplicate or in-flight event %s (%s); acknowledged", event_id, event_type)
            return 200, {"received": True, "result": "duplicate"}
    except Exception:
        log.exception("could not claim event %s", event_id)
        return 500, {"error": "temporary failure"}  # Stripe retries

    # 3) Apply it. On failure give the claim back so Stripe's retry can run it again.
    try:
        result = handle_event(event, repo, latest_status, policy)
    except Exception:
        log.exception("failed handling %s (%s)", event_id, event_type)
        try:
            events.release(event_id)
        except Exception:
            log.exception("could not release event %s (it becomes claimable again when stale)", event_id)
        return 500, {"error": "handler error"}

    try:
        events.mark_processed(event_id, result)
    except Exception:
        log.exception("applied %s but could not mark it processed (safe: handlers are idempotent)", event_id)
    log.info("event %s (%s): %s", event_id, event_type, result)
    return 200, {"received": True, "result": result}


# --------------------------------------------------------------------------- #
# FastAPI app
# --------------------------------------------------------------------------- #
def _require_env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    raise RuntimeError(f"Missing required environment variable: {' or '.join(names)}")


def create_app():
    import stripe
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse
    from starlette.concurrency import run_in_threadpool
    from supabase import create_client

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    stripe.api_key = _require_env("STRIPE_SECRET_KEY")
    stripe.max_network_retries = 2
    endpoint_secret = _require_env("STRIPE_WEBHOOK_SECRET")
    if not endpoint_secret.startswith("whsec_"):
        log.warning("STRIPE_WEBHOOK_SECRET does not look like a webhook signing secret (whsec_...)")

    db = create_client(_require_env("SUPABASE_URL"),
                       _require_env("SUPABASE_SECRET_KEY", "SUPABASE_SERVICE_ROLE_KEY"))
    repo, events = SupabaseUsersRepo(db), SupabaseEventStore(db)
    verify = make_stripe_verifier(stripe, endpoint_secret)
    policy = Policy.from_env()

    def latest_status(subscription_id: str) -> Optional[str]:
        return stripe.Subscription.retrieve(subscription_id)["status"]

    # No interactive docs / OpenAPI schema on a public payment endpoint.
    app = FastAPI(title="Stripe webhook", docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/health")
    def health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.post("/stripe/webhook")
    async def stripe_webhook(request: Request):
        declared = request.headers.get("content-length", "")
        if declared.isdigit() and int(declared) > MAX_BODY_BYTES:
            return JSONResponse({"error": "payload too large"}, status_code=413)
        payload = await request.body()  # RAW bytes: the signature covers the exact body Stripe sent
        status_code, body = await run_in_threadpool(
            process_webhook, payload, request.headers.get("stripe-signature"),
            verify=verify, repo=repo, events=events, latest_status=latest_status, policy=policy)
        return JSONResponse(body, status_code=status_code)

    return app
