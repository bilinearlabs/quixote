// build.rs - Sets up runtime library path (rpath) for dynamic duckdb loading

fn main() {
    // Only set rpath during linking, not during cargo check or other metadata phases

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();

    match target_os.as_str() {
        "linux" => {
            // $ORIGIN refers to the directory containing the executable
            // This tells the linker to look for libduckdb.so in the same directory as the binary
            println!("cargo::rustc-link-arg=-Wl,-rpath,$ORIGIN");
            // Also check a relative "lib" subdirectory if you prefer that layout
            println!("cargo::rustc-link-arg=-Wl,-rpath,$ORIGIN/../lib");
        }
        "macos" => {
            // @executable_path refers to the directory containing the executable on macOS
            println!("cargo::rustc-link-arg=-Wl,-rpath,@executable_path");
            // Also check a relative "lib" subdirectory
            println!("cargo::rustc-link-arg=-Wl,-rpath,@executable_path/../lib");
        }
        _ => {
            // Windows uses PATH and doesn't need rpath
            // Other platforms: let the user handle it
        }
    }
}
