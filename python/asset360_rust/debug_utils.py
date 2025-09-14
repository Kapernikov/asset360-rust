"""Utilities for pretty-printing LinkMLInstance trees.

This module provides a pure Python helper to render a :class:`LinkMLInstance`
as a human readable tree. It is intended for debugging and diagnostic use.
"""

from __future__ import annotations

from typing import Any

try:  # pragma: no cover - runtime optional during type checking
    from . import LinkMLInstance as LinkMLValue
except Exception:  # pragma: no cover - fallback when extension missing
    LinkMLValue = Any  # type: ignore[misc]

__all__ = ["pretty_linkml_value"]


def pretty_linkml_value(value: "LinkMLValue", prefix: str = '', nofirstline: bool = False) -> str:
    """Return a tree-style string representation of ``value``.

    Parameters
    ----------
    value: LinkMLInstance
        The value to render.
    """
    prefix
    
    if value.kind == "map":
        txt = f"{prefix}* [{value.class_name}]\n" if not nofirstline else f"[{value.class_name}]\n"
        for key in value.keys():
            rval = value[key]
            if rval.kind == "scalar":
                txt += f"{prefix}  | {key}={rval.as_python()}\n"
            elif rval.kind == "list":
                txt += f"{prefix}  | {key}:\n{pretty_linkml_value(rval, prefix + '  | ')}"
            else:
                pfx =  f"{prefix}  | " + ' ' * len(key)
                txt += f"{prefix}  | {key}: {pretty_linkml_value(rval, pfx, nofirstline=True)}"
                txt += f"{prefix}  |\n"
    elif value.kind == "list":
        for idx in range(len(value)):
            rval = value[idx]
            if rval.kind == "scalar":
                txt = f"{prefix} - {rval.as_python()}\n"
            else:
                txt = f"{prefix} - {pretty_linkml_value(rval, prefix + ' ', nofirstline=True)}"
    elif value.kind == "scalar":
        txt = f"{prefix}{value.as_python()}\n"
    else:
        txt = f"[UNKNOWN KIND={value.kind}]\n"
        
    return txt
