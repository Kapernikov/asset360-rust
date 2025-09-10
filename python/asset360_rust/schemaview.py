# shim that re-exports the native submodule
from . import _native as _n
_mod = _n.linkml_schemaview

# Re-export public names
__all__ = getattr(_mod, "__all__", [])
for name in dir(_mod):
    if not name.startswith("_") or name in __all__:
        globals()[name] = getattr(_mod, name)


class _LazyClassGetter:
    def __init__(self, sv: "SchemaView") -> None:
        self._sv = sv

    def __iter__(self):
        return iter(self._sv.get_class_ids())

    def items(self):
        return ((cid, self._sv.get_class_view(cid)) for cid in self._sv.get_class_ids())

    def keys(self):
        return self._sv.get_class_ids()

    def __getitem__(self, cid: str):
        cv = self._sv.get_class_view(cid)
        if cv is None:
            raise KeyError(cid)
        return cv


class _LazySlotGetter:
    def __init__(self, sv: "SchemaView") -> None:
        self._sv = sv

    def __iter__(self):
        return iter(self._sv.get_slot_ids())

    def items(self):
        return ((sid, self._sv.get_slot_view(sid)) for sid in self._sv.get_slot_ids())

    def keys(self):
        return self._sv.get_slot_ids()

    def __getitem__(self, sid: str):
        sv = self._sv.get_slot_view(sid)
        if sv is None:
            raise KeyError(sid)
        return sv


SchemaView.classes = property(lambda self: _LazyClassGetter(self))
SchemaView.slots = property(lambda self: _LazySlotGetter(self))
