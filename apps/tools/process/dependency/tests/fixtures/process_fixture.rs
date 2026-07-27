use std::io;

fn main() {
    eprintln!("fixture-stderr");
    println!(
        "openai_present={}",
        std::env::var_os("OPENAI_API_KEY").is_some()
    );
    let mut input = String::new();
    io::stdin().read_line(&mut input).expect("read input");
    println!("fixture-stdout:{}", input.trim_end());
}
