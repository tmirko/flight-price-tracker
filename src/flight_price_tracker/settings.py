"""Configuration and environment settings.

Loads non-secret configuration from YAML and secrets (API keys) from `.env`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RouteConfig(BaseModel):
    """Route definition.

    Attributes:
        origin: IATA airport code of the departure airport.
        destination: IATA airport code of the arrival airport.
    """

    model_config = ConfigDict(extra="forbid")

    origin: str = Field(min_length=3, max_length=10)
    destination: str = Field(min_length=3, max_length=10)


class WindowConfig(BaseModel):
    """Rolling outbound-date window configuration.

    Attributes:
        start_offset_days: Days from today to start querying.
        window_days: Number of consecutive outbound dates to query.
    """

    model_config = ConfigDict(extra="forbid")

    start_offset_days: int = Field(default=1, ge=0, le=365)
    window_days: int = Field(default=30, ge=1, le=365)


class SerpApiConfig(BaseModel):
    """SerpApi request configuration.

    Attributes:
        hl: Language code for the SerpApi response.
        gl: Country code for localization (e.g. "at").
        currency: Currency code for prices.
        adults: Number of adult passengers.
        travel_class: Travel class (SerpApi numeric enum; 1=economy).
        deep_search: Whether to enable deep search.
        include_airlines: Optional list of airline codes to include.
        exclude_airlines: Optional list of airline codes to exclude.
        top_n_offers: Number of offers to store per outbound date.
        rate_limit_seconds: Sleep between API calls.
    """

    model_config = ConfigDict(extra="forbid")

    hl: str = "en"
    gl: str = "us"
    currency: str = "USD"
    adults: int = Field(default=1, ge=1, le=9)
    travel_class: int = Field(default=1, ge=1, le=4)
    deep_search: bool = False
    include_airlines: list[str] | None = None
    exclude_airlines: list[str] | None = None
    top_n_offers: int = Field(default=5, ge=1, le=50)
    rate_limit_seconds: float = Field(default=1.0, ge=0.0, le=60.0)


class ReportingConfig(BaseModel):
    """Report output configuration.

    Attributes:
        write_dated_report: Whether to write a dated report alongside latest.md.
        top_k_deals: Number of cheapest dates to list in the report.
    """

    model_config = ConfigDict(extra="forbid")

    write_dated_report: bool = True
    top_k_deals: int = Field(default=5, ge=1, le=50)


class AppConfig(BaseModel):
    """Top-level YAML configuration model."""

    model_config = ConfigDict(extra="forbid")

    route: RouteConfig
    window: WindowConfig = WindowConfig()
    serpapi: SerpApiConfig = SerpApiConfig()
    reporting: ReportingConfig = ReportingConfig()


class EnvSettings(BaseSettings):
    """Secrets loaded from environment variables and `.env`."""

    model_config = SettingsConfigDict(env_prefix="", env_file=".env", extra="ignore")

    serpapi_api_key: str = Field(validation_alias="SERPAPI_API_KEY")


def load_app_config(path: Path) -> AppConfig:
    """Load and validate app config from a YAML file.

    Args:
        path: Path to the YAML config file.

    Returns:
        Validated configuration.
    """
    raw = _read_yaml(path)
    return AppConfig.model_validate(raw)


def _read_yaml(path: Path) -> dict[str, Any]:
    """Read a YAML mapping from disk.

    Args:
        path: Path to a YAML file.

    Returns:
        The parsed YAML data as a dict (empty if the file is empty).

    Raises:
        ValueError: If the YAML root is not a mapping.
    """
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"Config at {path} must be a YAML mapping/object")
    return data
