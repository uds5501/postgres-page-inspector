mod core;
mod db;

use crate::core::handle_command_call;

fn main() {
    println!("Hello, world!");
    handle_command_call();
}