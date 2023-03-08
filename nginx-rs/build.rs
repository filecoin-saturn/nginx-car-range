use std::env;
use std::path::PathBuf;

fn main() {
    // Path to the nginx repo in the local file system
    let nginx_dir = env::var("NGINX_DIR").unwrap_or(String::from("../../nginx"));

    let clang_args = [
        format!("-I{}/objs", nginx_dir),
        format!("-I{}/src/core", nginx_dir),
        format!("-I{}/src/event", nginx_dir),
        format!("-I{}/src/event/modules", nginx_dir),
        format!("-I{}/src/os/unix", nginx_dir),
        format!("-I{}/src/http", nginx_dir),
        format!("-I{}/src/http/modules", nginx_dir),
    ];

    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .layout_tests(false)
        .allowlist_type("ngx_.*")
        .allowlist_function("ngx_.*")
        .allowlist_var("NGX_.*|ngx_.*|nginx_.*")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .clang_args(clang_args)
        .generate()
        .expect("unable to generate nginx bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("unable to write nginx bindings.");
}
