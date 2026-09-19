"""
OmniWatch — Entry-Point / Identity Layer
Component: Pydantic models (request/response contracts)
Phase: entry-point (Wave 1)
Purpose: Data contracts for register/login/refresh/me
Inputs: Raw JSON request bodies
Outputs: Validated request objects / response schemas
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator

# Password policy: min 10 chars (plan spec), max 128 chars so bcrypt's
# 72-byte input limit can never surface as a 500 (oversize -> 422 instead).
PASSWORD_MIN_LENGTH = 10
PASSWORD_MAX_LENGTH = 128
EMAIL_MAX_LENGTH = 320


class RegisterRequest(BaseModel):
    """POST /auth/register body. Email is stored literally (any non-empty
    string, parameterized queries only) — never interpolated into SQL."""

    email: str = Field(min_length=1, max_length=EMAIL_MAX_LENGTH)
    password: str = Field(
        min_length=PASSWORD_MIN_LENGTH, max_length=PASSWORD_MAX_LENGTH)


class LoginRequest(BaseModel):
    """POST /auth/login body."""

    email: str = Field(min_length=1, max_length=EMAIL_MAX_LENGTH)
    # Login accepts any non-empty password (policy is enforced at register);
    # wrong passwords -> 401, never 422, to avoid user enumeration via codes.
    password: str = Field(min_length=1, max_length=PASSWORD_MAX_LENGTH)


class RefreshRequest(BaseModel):
    """POST /auth/refresh and POST /auth/logout body."""

    refresh_token: str = Field(min_length=1)


class TokenResponse(BaseModel):
    """Access + refresh pair issued by login/refresh."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


class RegisterResponse(BaseModel):
    """201 response for a new user."""

    user_id: str
    email: str
    created_at: str


class MeResponse(BaseModel):
    """GET /auth/me response for the JWT holder."""

    user_id: str
    email: str
    created_at: str


# ---------------------------------------------------------------------------
# Onboarding wizard (entry-point Wave 2, todo 3)
# ---------------------------------------------------------------------------

# Wizard enums (plan todo 3 + FUP capacity question set — closed vocabularies;
# anything outside these -> 422 via pydantic Literal validation).
APP_TYPES = ("api", "worker", "ml", "iot", "custom")
CLOUD_PROVIDERS = ("aws", "azure", "gcp", "onprem", "other")
VOLUME_BANDS = ("<100", "100-1k", "1k-10k", ">10k")

AppType = Literal["api", "worker", "ml", "iot", "custom"]
CloudProvider = Literal["aws", "azure", "gcp", "onprem", "other"]
VolumeBand = Literal["<100", "100-1k", "1k-10k", ">10k"]

# Free-form exec markers rejected in every wizard text field (plan: "reject
# script content"). Case-insensitive substring match -> ValueError -> 422.
# Covers <script> payloads, shell chains (; rm / pipes / &&), template
# injections (${{ }}), command substitutions ($() / backticks), control
# characters, and SQL-exec fragments. Legit app names, emails, and endpoint
# URLs never contain these substrings.
EXEC_MARKERS = (
    "<script",
    "</script",
    "<iframe",
    "javascript:",
    "${{",
    "$(",
    "`",
    ";rm",
    "; rm",
    "rm -rf",
    "|sh",
    "| sh",
    "|bash",
    "| bash",
    "&&",
    "||",
    "\r",
    "\n",
    "\x00",
    "exec(",
    "system(",
    "drop table",
    "delete from",
)

APP_NAME_MAX_LENGTH = 200
ALERT_CONTACT_MAX_LENGTH = 320
ENDPOINT_MAX_LENGTH = 500
ENDPOINTS_MAX_ITEMS = 32
RETENTION_DAYS_MIN = 1
RETENTION_DAYS_MAX = 3650


def reject_exec_markers(value: str, field_name: str) -> str:
    """Raise ValueError when a wizard text value carries exec markers.

    Args:
        value: Raw field value to inspect.
        field_name: Field name used in the 422 error detail.

    Raises:
        ValueError: Any EXEC_MARKERS substring present (case-insensitive).
    """
    lowered = value.lower()
    for marker in EXEC_MARKERS:
        if marker in lowered:
            raise ValueError(
                f"{field_name} contains disallowed content ({marker.strip()!r})"
            )
    return value


class OnboardingSubmit(BaseModel):
    """POST /workspaces/{id}/onboarding body (FUP capacity question set).

    Upsert semantics: resubmission overwrites (never duplicates). Enums and
    ranges are closed — violations surface as 422 before any persistence.
    """

    app_name: str = Field(min_length=1, max_length=APP_NAME_MAX_LENGTH)
    app_type: AppType
    cloud_provider: CloudProvider
    service_endpoints: list[str] = Field(
        default_factory=list, max_length=ENDPOINTS_MAX_ITEMS)
    expected_eps: VolumeBand
    log_volume: VolumeBand
    retention_days: int = Field(ge=RETENTION_DAYS_MIN, le=RETENTION_DAYS_MAX)
    alert_contact: str = Field(min_length=1, max_length=ALERT_CONTACT_MAX_LENGTH)

    @field_validator("app_name", "alert_contact")
    @classmethod
    def _no_exec_markers(cls, value: str, info) -> str:
        return reject_exec_markers(value, info.field_name)

    @field_validator("service_endpoints")
    @classmethod
    def _endpoints_clean(cls, values: list[str]) -> list[str]:
        cleaned: list[str] = []
        for endpoint in values:
            if not endpoint or not endpoint.strip():
                raise ValueError("service_endpoints entries must not be empty")
            if len(endpoint) > ENDPOINT_MAX_LENGTH:
                raise ValueError(
                    "service_endpoints entries must be at most "
                    f"{ENDPOINT_MAX_LENGTH} chars"
                )
            cleaned.append(reject_exec_markers(endpoint, "service_endpoints"))
        return cleaned


class SuggestedConfig(BaseModel):
    """Deterministic capacity suggestions (advisory ONLY — never applied)."""

    queue_depth: int
    batch_size: int
    poll_interval_s: int
    scrape_interval_s: int


class OnboardingResponse(BaseModel):
    """POST + GET /workspaces/{id}/onboarding payload (round-trip identical)."""

    workspace_id: str
    slug: str
    answers: OnboardingSubmit
    suggested_config: SuggestedConfig
    config_path: str
    updated_at: str
