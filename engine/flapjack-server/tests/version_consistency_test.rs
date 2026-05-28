#[test]
fn cargo_package_version_is_1_0_1() {
    assert_eq!(env!("CARGO_PKG_VERSION"), "1.0.1");
}
