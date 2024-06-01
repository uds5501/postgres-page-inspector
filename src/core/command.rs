use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use crate::core::btree::generate_btree;
use crate::core::render;
use crate::db;
use clap::Parser;
use log::error;

/// Postgres CLI args
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Postgres host
    #[arg(long, default_value = "localhost")]
    host: String,

    /// Postgres port
    #[arg(short, long, default_value = "5432")]
    port: String,

    /// Postgres user
    #[arg(short, long, default_value = "postgres")]
    user: String,

    /// Postgres database
    #[arg(short, long, default_value = "postgres")]
    db: String,

    /// Postgres password
    #[arg(short = 'x', long, default_value = "")]
    password: String,

    /// Postgres index
    #[arg(short, long)]
    index: String,

    /// Output file path
    #[arg(short, long, default_value = "output.html")]
    output: String,
}

pub fn handle_command_call() {
    let args = Args::parse();

    // Connect to the database
    let client_ref = Arc::new(RefCell::new(db::init_client(args.host, args.port, args.db,
                                                           args.user, args.password)));
    let index_information = db::get_index_info(Arc::clone(&client_ref), args.index.clone());
    if index_information.index_type == "btree" {
        let output_path = Path::new(args.output.as_str());
        let tree = generate_btree(Arc::clone(&client_ref), args.index, Rc::new(index_information));
        render(tree, output_path);
    } else {
        error!("Index type is not btree");
    }
}