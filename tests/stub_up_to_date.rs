//! Guard the committed Python type stubs against silent drift.
//!
//! `python/asset360_rust/_native2.pyi` is generated from the PyO3 bindings and
//! shipped inside the wheel, but nothing regenerated it: it fell three commits
//! behind the v0.8.4 release it shipped with (missing `load_json_batch` and
//! `PlanOp.retrieval`) because `stub_gen --check` existed and was never run.
//! Running it from the test suite makes `cargo test` — and therefore CI — fail
//! on any binding change that is not accompanied by a regenerated stub.
//!
//! When this fails, run:
//!
//! ```text
//! cargo run --bin stub_gen --features stubgen
//! ```

#![cfg(feature = "stubgen")]

use std::process::Command;

#[test]
fn committed_python_stubs_match_the_bindings() {
    let output = Command::new(env!("CARGO_BIN_EXE_stub_gen"))
        .arg("--check")
        .output()
        .expect("failed to run the stub_gen binary");

    if !output.status.success() {
        panic!(
            "The committed Python stubs do not match the PyO3 bindings.\n\
             Regenerate them with `cargo run --bin stub_gen --features stubgen` \
             and commit the result.\n\n\
             --- stub_gen stdout ---\n{}\n--- stub_gen stderr ---\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

/// A stub that no Python parser can read types nothing at all.
///
/// One Rust raw identifier (`def r#type`) leaked in by the generator is a
/// `SyntaxError`, and mypy answers a `SyntaxError` in a stub by degrading the
/// whole package to `Any` — so a consumer's clean type check means nothing.
/// Parse the committed file with the interpreter's own `ast` module.
#[test]
fn committed_python_stubs_are_parseable_python() {
    let stub = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/python/asset360_rust/_native2.pyi"
    );

    let output = Command::new("python3")
        .arg("-c")
        .arg("import ast, sys; ast.parse(open(sys.argv[1]).read(), sys.argv[1])")
        .arg(stub)
        .output()
        .expect("failed to run python3 to parse the stub");

    assert!(
        output.status.success(),
        "{stub} is not parseable Python:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
}

/// PEP 561: without this marker a type checker ignores the inline types.
#[test]
fn the_python_package_ships_a_pep561_marker() {
    let marker = concat!(env!("CARGO_MANIFEST_DIR"), "/python/asset360_rust/py.typed");
    assert!(
        std::path::Path::new(marker).is_file(),
        "missing PEP 561 marker at {marker}; without it every type checker \
         ignores the stubs we ship"
    );
}

/// A `gen_stub_*` attribute must sit *above* the PyO3 attributes it describes.
///
/// PyO3's own macro expands first and consumes the `#[pyo3(...)]` attributes,
/// so a `gen_stub_*` attribute placed after them sees neither the renamed
/// Python name nor the signature defaults. This is silent: the stub still
/// generates, it just describes a different API. It had bitten seven
/// declarations — `plan_query_refined`, `naive_plan_text` and
/// `refined_plan_text` were stubbed under their Rust names
/// (`py_plan_query_refined`, ...) which do not exist at runtime, and
/// `sparql_execute`, `sparql_scope`, `sparql_reads_only_the_schema_graph` and
/// `plan_query_refined` lost every default value, so the stub demanded
/// arguments the function does not require.
///
/// Enforced as a source lint because the failure mode is a stub that parses
/// and lies, which no parse check can catch.
#[test]
fn gen_stub_attributes_precede_the_pyo3_attributes() {
    let source = include_str!("../src/lib.rs");
    let lines: Vec<&str> = source.lines().collect();
    let mut offenders = Vec::new();

    for (idx, line) in lines.iter().enumerate() {
        if !line.contains("gen_stub_py") {
            continue;
        }
        // Walk back over the contiguous attribute block above this line.
        for earlier in lines[..idx].iter().rev() {
            let trimmed = earlier.trim_start();
            if !trimmed.starts_with("#[") {
                break;
            }
            let is_pyo3_attr = ["#[pyfunction", "#[pyclass", "#[pymethods", "#[pyo3("]
                .iter()
                .any(|prefix| trimmed.starts_with(prefix));
            if is_pyo3_attr {
                offenders.push(format!(
                    "src/lib.rs:{}: `{}` comes after `{}`",
                    idx + 1,
                    line.trim(),
                    trimmed
                ));
                break;
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "A `gen_stub_*` attribute must be written above the `#[pyfunction]` / \
         `#[pyclass]` / `#[pyo3(...)]` attributes, or it silently generates a \
         stub for the wrong name and signature:\n{}",
        offenders.join("\n"),
    );
}

/// Every `@x.setter` must directly follow the `@property def x` it completes.
///
/// `pyo3-stub-gen` renders all of a class's getters and then all of its
/// setters, which Python's property protocol does not accept: a detached
/// `@x.setter` re-binds the name instead of completing the property, and mypy
/// reports every settable PyO3 attribute as **read-only** — it rejected
/// `obj.attr = value` in consumer code that runs fine. It also produced 3719 of
/// the 3742 errors mypy found inside the stub (one `no-redef` /
/// `untyped-decorator` / `attr-defined` triple per setter).
///
/// `stub_gen` reorders the blocks; this asserts the result, using Python's own
/// `ast` rather than the same logic that did the reordering. The failure is
/// invisible to a plain parse — the file stays valid Python, it just describes
/// an API that is read-only and is not.
#[test]
fn committed_python_stubs_attach_every_setter_to_its_property() {
    let stub = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/python/asset360_rust/_native2.pyi"
    );

    let audit = r#"
import ast, sys

detached = []
total = 0

def audit(body, where):
    global total
    previous = None
    for statement in body:
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
            decorators = [ast.unparse(d) for d in statement.decorator_list]
            setters = [d for d in decorators if d.endswith(".setter")]
            if setters:
                total += 1
                owner = setters[0][: -len(".setter")]
                if previous != (owner, True):
                    detached.append(f"{where}.{statement.name} (line {statement.lineno})")
            previous = (statement.name, "property" in decorators)
        elif isinstance(statement, ast.ClassDef):
            audit(statement.body, statement.name)
            previous = None
        else:
            previous = None

tree = ast.parse(open(sys.argv[1]).read(), sys.argv[1])
for statement in tree.body:
    if isinstance(statement, ast.ClassDef):
        audit(statement.body, statement.name)

if detached:
    print(f"{len(detached)} of {total} setters are detached from their property:")
    for entry in detached[:20]:
        print("  -", entry)
    sys.exit(1)
print(f"all {total} setters directly follow their @property")
"#;

    let output = Command::new("python3")
        .arg("-c")
        .arg(audit)
        .arg(stub)
        .output()
        .expect("failed to run python3 to audit the stub's properties");

    assert!(
        output.status.success(),
        "{stub} describes settable attributes as read-only.\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
