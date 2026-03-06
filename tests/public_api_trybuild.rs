#[test]
fn public_api_ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass/public_api_smoke.rs");
    t.compile_fail("tests/ui/fail/*.rs");
}
