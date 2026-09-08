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

#[cfg(feature = "stubgen")]
use asset360_rust::stub_info;
#[cfg(feature = "stubgen")]
use pyo3_stub_gen::{
    Result, StubInfo,
    generate::{Arg, ClassDef, EnumDef, FunctionDef, MemberDef, MethodDef, Module, VariableDef},
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

#[cfg(feature = "stubgen")]
fn render(module: &Module, module_name: &str) -> Result<String> {
    let rendered = module.to_string();
    assert_no_raw_identifiers(&rendered, module_name)?;
    Ok(rendered)
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
