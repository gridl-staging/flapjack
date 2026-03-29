fn main() {
    let path = flapjack_http::openapi_export::default_docs2_output_path();
    flapjack_http::openapi_export::write_openapi_json(&path).unwrap();
    println!("Regenerated {}", path.display());
}
