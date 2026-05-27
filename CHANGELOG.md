# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `src/breakerd` package layout — standalone Python package, no Django required
- `pyproject.toml` with hatchling build backend and optional extras: `redis`, `metrics`, `dev`
- Thread-safe singleton `get_instance()` with double-checked locking
- `maybe_recover()` explicit state transition method (replaces side-effectful property getter)
- `HALF_OPEN` state added to `BreakerStates` enum (full 3-state machine in Stage 2)
- Structured logging with `extra={}` context
- Exception-safe daemon thread ticker (`utils.py`)
- `circuit_breaker` as primary decorator name; `circuit` kept as alias
- `redis_client` injection parameter for testability
- Removed Django as a hard dependency; moved to `contrib/django/`
- `contrib/django/settings.py` reads all values from environment variables
- `.env.example` documenting all required environment variables

### Fixed
- Recursive `record_failure` / `record_success` replaced with explicit `_initialize_if_absent` guard
- `__init__` double-initialization guard in `CircuitStoreSingleton`
- Mutable default argument `extra={}` in `log()` replaced with `extra=None`
- `_strategy = None` in base class now set to `"base"` so logs are not misleading
- `int(val.decode())` Redis mget crash replaced with `int(v) if v is not None else 0`
- Config dict lookup now correctly keyed by breaker name
- `_should_close` guard: `total_events == 0` now correctly allows circuit to close

### Removed
- Django, DRF, `six`, `async-timeout` from core dependencies
- Empty Django scaffolding files (`urls.py`, `models.py`, `views.py`, `admin.py`, `migrations/`)
- `debug` parameter from `ticker()` (was unused)

## [0.0.1] - 2022-01-01

Initial prototype — Django-coupled distributed circuit breaker with Redis sync.
