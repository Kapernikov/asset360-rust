//! Generate (or verify) the Python type stubs for the `asset360_rust` extension.
//!
//! Run without arguments to write the stubs; run with `--check` to fail when
//! the committed stubs differ from what a fresh generation would produce. The
//! `--check` mode is what `tests/stub_up_to_date.rs` exercises, so a stale
//! `python/asset360_rust/_native2.pyi` fails `cargo test`.
//!
//! # Why this binary post-processes the generator's output
//!
//! `pyo3-stub-gen` 0.13.1 takes a member's Python name from
//! `syn::Ident::to_string()` (see `gen_stub/member.rs::new_getter` and
//! `gen_stub/method.rs`). For a Rust *raw* identifier such as `r#type` that
//! yields the string `"r#type"`, whereas PyO3 itself un-raws the identifier
//! and exposes the attribute as `type`. The stub therefore named a getter
//! `r#type`, which is not valid Python, and a single such line makes the whole
//! `.pyi` unparseable — every type checker then degrades the entire package to
//! `Any`.
//!
//! The offending declaration is not ours: it is `#[getter] fn r#type` on
//! `PyValidationResult` in the `linkml_runtime_python` crate of
//! `rust-linkml-core`. We cannot annotate it from here, and the underlying
//! defect is the generator's (PyO3 un-raws, its stub generator does not), so
//! this binary un-raws identifiers in the generated model before rendering it.
//! [`assert_no_raw_identifiers`] is the backstop: if a raw identifier survives
//! in a field this pass does not cover, generation fails loudly rather than
//! writing an unparseable stub.
//!
//! # The second post-processing pass: detached property setters
//!
//! `pyo3-stub-gen`'s `generate/class.rs` renders *all* of a class's getters in
//! one loop and then *all* of its setters in a second, so `@x.setter` never
//! immediately follows the `@property def x` it decorates. Python's
//! property protocol requires adjacency, and mypy answers the detached form by
//! treating every settable PyO3 attribute as **read-only** — `obj.attr = value`
//! is rejected in consumer code that is perfectly valid at runtime. It also
//! accounts for the bulk of the errors mypy reports inside the stub
//! (`no-redef`, `untyped-decorator`, `attr-defined`), one triple per setter.
//!
//! [`interleave_property_setters`] moves each setter block up to its getter.
//! It does not re-render anything: `GetterDisplay` and `SetterDisplay` are
//! public, so the exact block text the generator emits is reproduced by the
//! generator's own code and only *reordered*. The getters-then-setters run is
//! located by exact string match, and a run that cannot be found is a hard
//! error rather than a silent no-op — if upstream changes its layout we want to
//! hear about it. [`assert_setters_follow_their_property`] then re-checks the
//! adjacency invariant on the finished text.

#[cfg(feature = "stubgen")]
use asset360_rust::stub_info;
#[cfg(feature = "stubgen")]
use pyo3_stub_gen::{
    Result, StubInfo,
    generate::{
        Arg, ClassDef, EnumDef, FunctionDef, GetterDisplay, MemberDef, MethodDef, Module,
        SetterDisplay, VariableDef,
    },
};
#[cfg(feature = "stubgen")]
use std::env;
#[cfg(feature = "stubgen")]
use std::fs;
#[cfg(feature = "stubgen")]
use std::io::ErrorKind;
#[cfg(feature = "stubgen")]
use std::path::PathBuf;

#[cfg(feature = "stubgen")]
fn main() -> Result<()> {
    let check_only = env::args().skip(1).any(|arg| arg == "--check");
    let mut stub = stub_info()?;
    sanitize(&mut stub);

    if check_only {
        check_stubs(&stub)
    } else {
        write_stubs(&stub)
    }
}

/// Strip the `r#` prefix a Rust raw identifier carries into the generator.
#[cfg(feature = "stubgen")]
fn unraw(name: &'static str) -> &'static str {
    name.strip_prefix("r#").unwrap_or(name)
}

#[cfg(feature = "stubgen")]
fn unraw_owned(name: String) -> String {
    match name.strip_prefix("r#") {
        Some(rest) => rest.to_owned(),
        None => name,
    }
}

#[cfg(feature = "stubgen")]
fn sanitize(stub: &mut StubInfo) {
    for module in stub.modules.values_mut() {
        sanitize_module(module);
    }
}

#[cfg(feature = "stubgen")]
fn sanitize_module(module: &mut Module) {
    for class in module.class.values_mut() {
        sanitize_class(class);
    }
    for enum_ in module.enum_.values_mut() {
        sanitize_enum(enum_);
    }
    module.function = std::mem::take(&mut module.function)
        .into_iter()
        .map(|(name, mut defs)| {
            for def in &mut defs {
                sanitize_function(def);
            }
            (unraw(name), defs)
        })
        .collect();
    module.variables = std::mem::take(&mut module.variables)
        .into_iter()
        .map(|(name, mut def)| {
            sanitize_variable(&mut def);
            (unraw(name), def)
        })
        .collect();
}

#[cfg(feature = "stubgen")]
fn sanitize_class(class: &mut ClassDef) {
    class.name = unraw(class.name);
    for member in class
        .attrs
        .iter_mut()
        .chain(class.getters.iter_mut())
        .chain(class.setters.iter_mut())
    {
        sanitize_member(member);
    }
    class.methods = std::mem::take(&mut class.methods)
        .into_iter()
        .map(|(name, mut defs)| {
            for def in &mut defs {
                sanitize_method(def);
            }
            (unraw_owned(name), defs)
        })
        .collect();
    if let Some(match_args) = class.match_args.as_mut() {
        for arg in match_args.iter_mut() {
            *arg = unraw_owned(std::mem::take(arg));
        }
    }
    for nested in &mut class.classes {
        sanitize_class(nested);
    }
}

#[cfg(feature = "stubgen")]
fn sanitize_enum(enum_: &mut EnumDef) {
    enum_.name = unraw(enum_.name);
    if enum_
        .variants
        .iter()
        .any(|(name, _)| name.starts_with("r#"))
    {
        let variants: Vec<(&'static str, &'static str)> = enum_
            .variants
            .iter()
            .map(|(name, doc)| (unraw(name), *doc))
            .collect();
        enum_.variants = Box::leak(variants.into_boxed_slice());
    }
    for member in enum_
        .attrs
        .iter_mut()
        .chain(enum_.getters.iter_mut())
        .chain(enum_.setters.iter_mut())
    {
        sanitize_member(member);
    }
    for method in &mut enum_.methods {
        sanitize_method(method);
    }
}

#[cfg(feature = "stubgen")]
fn sanitize_member(member: &mut MemberDef) {
    member.name = unraw(member.name);
}

#[cfg(feature = "stubgen")]
fn sanitize_method(method: &mut MethodDef) {
    method.name = unraw(method.name);
    for arg in &mut method.args {
        sanitize_arg(arg);
    }
}

#[cfg(feature = "stubgen")]
fn sanitize_function(function: &mut FunctionDef) {
    function.name = unraw(function.name);
    for arg in &mut function.args {
        sanitize_arg(arg);
    }
}

#[cfg(feature = "stubgen")]
fn sanitize_variable(variable: &mut VariableDef) {
    variable.name = unraw(variable.name);
}

#[cfg(feature = "stubgen")]
fn sanitize_arg(arg: &mut Arg) {
    arg.name = unraw(arg.name);
}

/// Where each module's stub file belongs, mirroring `StubInfo::generate`.
#[cfg(feature = "stubgen")]
fn stub_path(stub: &StubInfo, name: &str, module: &Module) -> PathBuf {
    let path = name.replace('-', "_").replace('.', "/");
    if module.submodules.is_empty() {
        stub.python_root.join(format!("{path}.pyi"))
    } else {
        stub.python_root.join(&path).join("__init__.pyi")
    }
}

/// Fail rather than emit a stub that no Python parser can read.
///
/// A raw identifier is only ever a leak from the Rust side, so a survivor here
/// means [`sanitize`] misses a field the generator has grown. That must be a
/// hard error: a stub with one `r#name` in it silently turns the whole package
/// into `Any` for every consumer.
///
/// Only declaration-shaped lines are inspected, so `r#` occurring as prose
/// inside a rendered docstring is not mistaken for a leak. That is why
/// `tests/stub_up_to_date.rs` also parses the committed file with Python's own
/// `ast`: this check is the fast signal, the parse is the authority.
#[cfg(feature = "stubgen")]
fn assert_no_raw_identifiers(rendered: &str, module_name: &str) -> Result<()> {
    let offenders: Vec<&str> = rendered
        .lines()
        .filter(|line| {
            if !line.contains("r#") {
                return false;
            }
            let trimmed = line.trim_start();
            trimmed.starts_with("def ")
                || trimmed.starts_with("async def ")
                || trimmed.starts_with("class ")
                || trimmed.starts_with('@')
                || trimmed.starts_with("r#")
        })
        .collect();
    if offenders.is_empty() {
        return Ok(());
    }
    let mut msg = format!(
        "module `{module_name}`: Rust raw identifiers leaked into the generated stub, \
         which would make it unparseable Python. Extend `sanitize` in \
         src/bin/stub_gen.rs to cover them:\n"
    );
    for line in offenders {
        msg.push_str("  - ");
        msg.push_str(line.trim());
        msg.push('\n');
    }
    Err(std::io::Error::other(msg).into())
}

/// One class's or enum's getter and setter blocks, in the order upstream emits
/// them and in the order Python needs them.
///
/// Both strings are built with the generator's own `GetterDisplay` /
/// `SetterDisplay`, so `emitted` is byte-identical to the run that appears in
/// the rendered module and `wanted` differs from it only in block order.
#[cfg(feature = "stubgen")]
struct PropertyRun {
    owner: String,
    emitted: String,
    wanted: String,
}

/// A nested class is embedded by re-indenting its rendering line by line, so
/// its blocks appear in the module text one level deeper than they render.
#[cfg(feature = "stubgen")]
fn reindent(text: &str, depth: usize) -> String {
    if depth == 0 {
        return text.to_owned();
    }
    let prefix = "    ".repeat(depth);
    text.lines()
        .map(|line| {
            if line.is_empty() {
                String::from("\n")
            } else {
                format!("{prefix}{line}\n")
            }
        })
        .collect()
}

#[cfg(feature = "stubgen")]
fn property_run(
    owner: &str,
    depth: usize,
    getters: &[MemberDef],
    setters: &[MemberDef],
) -> Option<PropertyRun> {
    if setters.is_empty() {
        return None;
    }

    let mut emitted = String::new();
    for getter in getters {
        emitted.push_str(&GetterDisplay(getter).to_string());
    }
    for setter in setters {
        emitted.push_str(&SetterDisplay(setter).to_string());
    }

    let mut wanted = String::new();
    for getter in getters {
        wanted.push_str(&GetterDisplay(getter).to_string());
        for setter in setters.iter().filter(|s| s.name == getter.name) {
            wanted.push_str(&SetterDisplay(setter).to_string());
        }
    }
    // A setter with no matching getter is not a property at all; leave those
    // where upstream put them rather than inventing a position for them.
    for setter in setters
        .iter()
        .filter(|s| !getters.iter().any(|g| g.name == s.name))
    {
        wanted.push_str(&SetterDisplay(setter).to_string());
    }

    if emitted == wanted {
        return None;
    }
    Some(PropertyRun {
        owner: owner.to_owned(),
        emitted: reindent(&emitted, depth),
        wanted: reindent(&wanted, depth),
    })
}

#[cfg(feature = "stubgen")]
fn collect_property_runs(module: &Module, runs: &mut Vec<PropertyRun>) {
    fn visit_class(class: &ClassDef, depth: usize, runs: &mut Vec<PropertyRun>) {
        if let Some(run) = property_run(class.name, depth, &class.getters, &class.setters) {
            runs.push(run);
        }
        for nested in &class.classes {
            visit_class(nested, depth + 1, runs);
        }
    }

    for class in module.class.values() {
        visit_class(class, 0, runs);
    }
    for enum_ in module.enum_.values() {
        if let Some(run) = property_run(enum_.name, 0, &enum_.getters, &enum_.setters) {
            runs.push(run);
        }
    }
}

/// Move each `@X.setter` block up to sit directly after its `@property def X`.
///
/// Upstream renders all getters, then all setters, which Python's property
/// protocol does not accept: a detached `@x.setter` re-binds the name instead
/// of completing the property, and mypy then reports the attribute as
/// read-only. Nothing is re-rendered here — only the generator's own block text
/// is relocated.
#[cfg(feature = "stubgen")]
fn interleave_property_setters(
    rendered: String,
    module: &Module,
    module_name: &str,
) -> Result<String> {
    let mut runs = Vec::new();
    collect_property_runs(module, &mut runs);

    let mut out = rendered;
    let mut unmatched = Vec::new();
    for run in runs {
        match out.find(&run.emitted) {
            Some(at) => out.replace_range(at..at + run.emitted.len(), &run.wanted),
            None => unmatched.push(run.owner),
        }
    }

    if !unmatched.is_empty() {
        // The run is built from the generator's own Display impls, so failing to
        // find it means upstream changed how a class body is laid out. Refuse
        // rather than write a stub whose setters are silently still detached.
        return Err(std::io::Error::other(format!(
            "module `{module_name}`: could not locate the getter/setter run for {unmatched:?} \
             in the rendered stub. `pyo3-stub-gen`'s class layout has changed; revisit \
             `interleave_property_setters` in src/bin/stub_gen.rs."
        ))
        .into());
    }

    assert_setters_follow_their_property(&out, module_name)?;
    Ok(out)
}

/// Every `@X.setter` must be preceded by the `@property def X` it completes.
///
/// Checked on the finished text rather than trusted from the transform above,
/// because the failure is invisible to a parse: the file is still valid Python,
/// it just describes a read-only attribute.
#[cfg(feature = "stubgen")]
fn assert_setters_follow_their_property(rendered: &str, module_name: &str) -> Result<()> {
    /// The name a `def <name>(` line declares.
    fn declared_name(trimmed: &str) -> Option<&str> {
        trimmed
            .strip_prefix("def ")
            .and_then(|rest| rest.split('(').next())
    }

    let indent_of = |line: &str| line.len() - line.trim_start().len();

    let mut offenders = Vec::new();
    // The property whose getter was declared last, per indentation level: a
    // docstring body is indented deeper and so cannot clear it, while the next
    // declaration at the same level does.
    let mut last_property: Option<(usize, String)> = None;
    let mut pending_property_at: Option<usize> = None;

    for (idx, line) in rendered.lines().enumerate() {
        let trimmed = line.trim_start();
        let indent = indent_of(line);

        if trimmed == "@property" {
            pending_property_at = Some(indent);
            continue;
        }
        if let Some(name) = trimmed
            .strip_prefix('@')
            .and_then(|rest| rest.strip_suffix(".setter"))
        {
            match &last_property {
                Some((property_indent, property))
                    if *property_indent == indent && property == name => {}
                _ => offenders.push(format!(
                    "line {}: `@{name}.setter` does not directly follow `@property def {name}`",
                    idx + 1
                )),
            }
            // The setter's own `def` line must not be read as a new property.
            pending_property_at = None;
            continue;
        }
        if let Some(name) = declared_name(trimmed) {
            if pending_property_at == Some(indent) {
                last_property = Some((indent, name.to_owned()));
            } else if last_property
                .as_ref()
                .is_some_and(|(i, n)| *i == indent && n != name)
            {
                last_property = None;
            }
            pending_property_at = None;
            continue;
        }
        if trimmed.starts_with("class ") {
            last_property = None;
            pending_property_at = None;
        }
    }

    if offenders.is_empty() {
        return Ok(());
    }
    let mut msg = format!(
        "module `{module_name}`: property setters are detached from their getters. \
         Python needs `@x.setter` to directly follow `@property def x`; detached, \
         mypy reports every settable attribute as read-only. Check \
         `interleave_property_setters` in src/bin/stub_gen.rs:\n"
    );
    for offender in &offenders {
        msg.push_str("  - ");
        msg.push_str(offender);
        msg.push('\n');
    }
    Err(std::io::Error::other(msg).into())
}

#[cfg(feature = "stubgen")]
fn render(module: &Module, module_name: &str) -> Result<String> {
    let rendered = module.to_string();
    assert_no_raw_identifiers(&rendered, module_name)?;
    interleave_property_setters(rendered, module, module_name)
}

#[cfg(feature = "stubgen")]
fn write_stubs(stub: &StubInfo) -> Result<()> {
    for (name, module) in &stub.modules {
        let rendered = render(module, name)?;
        let dest = stub_path(stub, name, module);
        if let Some(dir) = dest.parent() {
            fs::create_dir_all(dir)?;
        }
        fs::write(&dest, rendered)?;
        eprintln!("wrote {}", dest.display());
    }
    Ok(())
}

#[cfg(feature = "stubgen")]
fn check_stubs(stub: &StubInfo) -> Result<()> {
    let mut issues = Vec::new();

    for (name, module) in &stub.modules {
        let dest = stub_path(stub, name, module);
        let expected = render(module, name)?;
        match fs::read_to_string(&dest) {
            Ok(actual) => {
                if actual != expected {
                    issues.push(format!(
                        "updated content differs for `{}` (module `{name}`)",
                        dest.display()
                    ));
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                issues.push(format!(
                    "missing stub file `{}` for module `{name}`",
                    dest.display()
                ));
            }
            Err(err) => return Err(err.into()),
        }
    }

    if issues.is_empty() {
        Ok(())
    } else {
        let mut msg = String::from("Stub files are out of date:\n");
        for issue in &issues {
            msg.push_str("  - ");
            msg.push_str(issue);
            msg.push('\n');
        }
        msg.push_str("Run `cargo run --bin stub_gen --features stubgen` to regenerate.");
        Err(std::io::Error::other(msg).into())
    }
}

#[cfg(not(feature = "stubgen"))]
fn main() {
    eprintln!("Enable the `stubgen` feature to run this generator.");
}
