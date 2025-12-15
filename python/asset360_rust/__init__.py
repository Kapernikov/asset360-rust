"""Python package for asset360-rust bindings."""

# Import the native module built by Rust; we alias it to `_native` so
# shims stay compatible with the dependency's layout.
from . import _native2 as _native  # type: ignore
from ._native2 import *  # noqa: F401,F403
from ._resolver import resolve_schemas
from .schemaview import (
    ClassDefinition,
    ClassView,
    EnumDefinition,
    EnumView,
    SchemaDefinition,
    SchemaView,
    SlotDefinition,
    SlotView,
    sum_as_string,
)
from .debug_utils import pretty_linkml_value

# Build an explicit export list so type checkers don't flag the backing module.
_native_exports = [name for name in dir(_native) if not name.startswith("_")]

__all__ = _native_exports + [
    "resolve_schemas",
    "SchemaView",
    "ClassView",
    "SlotView",
    "EnumView",
    "SchemaDefinition",
    "ClassDefinition",
    "SlotDefinition",
    "EnumDefinition",
    "sum_as_string",
    "pretty_linkml_value",
]
