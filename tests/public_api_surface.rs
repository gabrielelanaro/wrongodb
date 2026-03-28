#[test]
fn public_api_surface_matches_contract() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/public_api_pass.rs");
    cases.compile_fail("tests/ui/internal_storage_fails.rs");
    cases.compile_fail("tests/ui/internal_server_api_fails.rs");
}
