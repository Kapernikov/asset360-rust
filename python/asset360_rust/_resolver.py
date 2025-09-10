"""Schema import resolution implemented in pure Python."""

from pathlib import Path
from urllib.request import urlopen
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - for type checking only
    from .schemaview import SchemaView


_KNOWN_IMPORTS = {
    "https://w3id.org/linkml/mappings": "https://raw.githubusercontent.com/linkml/linkml-model/refs/heads/main/linkml_model/model/schema/mappings.yaml",
    "https://w3id.org/linkml/types": "https://raw.githubusercontent.com/linkml/linkml-model/refs/heads/main/linkml_model/model/schema/types.yaml",
    "https://w3id.org/linkml/extensions": "https://raw.githubusercontent.com/linkml/linkml-model/refs/heads/main/linkml_model/model/schema/extensions.yaml",
    "https://w3id.org/linkml/annotations": "https://raw.githubusercontent.com/linkml/linkml-model/refs/heads/main/linkml_model/model/schema/annotations.yaml",
    "https://w3id.org/linkml/units": "https://raw.githubusercontent.com/linkml/linkml-model/refs/heads/main/linkml_model/model/schema/units.yaml",
}


def resolve_schemas(sv: "SchemaView") -> None:
    """Resolve any imported schemas using Python's ``urllib``.

    Mirrors the Rust implementation using stdlib (no reqwest).
    Local paths are resolved relative to the schema containing the import.
    """
    nb_resolved = 1

    while nb_resolved > 0:
        nb_resolved = 0
        for schema_id, uri in sv.get_unresolved_schema_refs():
            target = _KNOWN_IMPORTS.get(uri, uri)

            path = Path(target)
            if not path.exists():
                if not path.is_absolute():
                    schema_source_uri = sv.get_resolution_uri_of_schema(schema_id)
                    if schema_source_uri:
                        schema_source_uri = Path(schema_source_uri)
                    if schema_source_uri:
                        imported_from_dir = schema_source_uri.parent
                        if imported_from_dir:
                            path = imported_from_dir / path
                    if not path.exists() and path.with_suffix(".yaml").exists():
                        path = path.with_suffix(".yaml")
                    if not path.exists() and path.with_suffix(".yml").exists():
                        path = path.with_suffix(".yml")

            if path.exists():
                text = path.read_text()
                path = str(path)
            else:
                path = str(target)
                with urlopen(target) as resp:  # nosec: B310 - controlled URLs
                    text = resp.read().decode("utf-8")
            if sv.add_schema_str_with_import_ref(text, schema_id, uri):
                nb_resolved += 1

