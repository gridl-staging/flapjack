#[test]
fn cargo_package_version_is_1_0_3() {
    assert_eq!(env!("CARGO_PKG_VERSION"), "1.0.3");
}
