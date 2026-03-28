use wrongodb::server::commands;

fn main() {
    let _ = std::any::type_name::<commands::CommandRegistry>();
}
