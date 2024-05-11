use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use postgres::Client;
use crate::db::{get_metadata_page, get_page, IndexInfo};
pub use crate::core::structs::Tree;

pub fn generate_btree(client: Arc<RefCell<Client>>, index_name: String, index_info: Rc<IndexInfo>) -> Tree {
    let metadata_page = get_metadata_page(Arc::clone(&client), index_name.clone());
    println!("Metadata page: {:?}", metadata_page);
    let root = get_page(client.clone(), metadata_page.root, index_name.clone(), index_info.clone());
    Tree::new(metadata_page, root, index_name, index_info)
}
