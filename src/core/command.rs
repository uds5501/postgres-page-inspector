use std::cell::RefCell;
use getopts::Options;
use std::env;
use std::sync::Arc;
use crate::db;

pub fn handle_command_call() {
    let args: Vec<String> = env::args().collect();
    let mut opts = Options::new();
    opts.optflag("h", "help", "print this help menu");
    opts.optopt("i", "host", "the host to connect to", "HOST");
    opts.optopt("p", "port", "the port to connect to", "PORT");
    opts.optopt("u", "user", "the user to connect as", "USER");
    opts.optopt("d", "db", "the database name", "DB");
    opts.optopt("w", "password", "the password to use", "PASSWORD");
    opts.optopt("x", "index", "the index to use", "INDEX");
    opts.optopt("a", "path", "the file path to use", "PATH");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!("{}", f.to_string()),
    };

    if matches.opt_present("h") {
        println!("{}", opts.usage(&format!("Usage: {} [options]", args[0])));
        return;
    }

    // TODO: Use these args
    // let host = matches.opt_str("i").unwrap_or_else(|| "".to_string());
    // let port = matches.opt_str("p").unwrap_or_else(|| "".to_string());
    // let user = matches.opt_str("u").unwrap_or_else(|| "".to_string());
    // let db = matches.opt_str("d").unwrap_or_else(|| "".to_string());
    // let password = matches.opt_str("w").unwrap_or_else(|| "".to_string());
    // let index = matches.opt_str("x").unwrap_or_else(|| "".to_string());
    // let file_path = matches.opt_str("a").unwrap_or_else(|| "".to_string());

    // Connect to the database
    let client = db::init_client("localhost".to_string(), "5432".to_string(), "postgres".to_string());

    for row in db::get(Arc::new(RefCell::new(client)), "SELECT COUNT(*) FROM subcriptions".to_string()) {
        let count: i64 = row.get(0);
        println!("count: {}", count);
    }
}