use asset360_rust::stub_info;
use pyo3_stub_gen::Result;
use std::env;
use std::fs;
use std::io::ErrorKind;

fn main() -> Result<()> {
    let check_only = env::args().skip(1).any(|arg| arg == "--check");
    let stub = stub_info()?;

    if check_only {
        check_stubs(&stub)
    } else {
        stub.generate()
    }
}

fn check_stubs(stub: &pyo3_stub_gen::StubInfo) -> Result<()> {
    let mut issues = Vec::new();

    for (name, module) in &stub.modules {
        let normalized_name = name.replace('-', "_");
        let path = normalized_name.replace('.', "/");
        let dest = if module.submodules.is_empty() {
            stub.python_root.join(format!("{path}.pyi"))
        } else {
            stub.python_root.join(&path).join("__init__.pyi")
        };

        let expected = module.to_string();
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
        msg.push_str("Run `cargo run --bin stub_gen` to regenerate.");
        Err(std::io::Error::new(ErrorKind::Other, msg).into())
    }
}
