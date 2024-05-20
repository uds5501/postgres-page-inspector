use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use postgres;
use postgres::{Client, Row};
use crate::core::structs::{Item, MetadataPage, RowData, Tid};
use crate::core::Page;

pub fn init_client(host: String, port: String, db: String, user: String, pass: String) -> Client {
    let mut connection_string = format!("host={} port={} dbname={}", host, port, db);
    if user != "" {
        connection_string.push_str(&format!(" user={}", user));
    }
    if pass != "" {
        connection_string.push_str(&format!(" password={}", pass));
    }

    Client::connect(connection_string.as_str(), postgres::NoTls).unwrap()
}

pub fn get(client: Arc<RefCell<Client>>, query: String) -> Vec<Row> {
    client.borrow_mut().query(&query, &[]).unwrap()
}

#[derive(PartialEq, Debug)]
pub struct IndexInfo {
    pub index_type: String,
    pub columns: Vec<String>,
    pub table_name: String,
    pub table_oid: postgres::types::Oid,
    pub primary_indexed_attributes: Vec<String>,
}

pub fn get_index_info(client: Arc<RefCell<Client>>, index: String) -> IndexInfo {
    let mut index_info = IndexInfo {
        index_type: "".to_string(),
        columns: vec![],
        table_name: "".to_string(),
        table_oid: 0,
        primary_indexed_attributes: vec![],
    };

    let index_type_query = r#"
        SELECT
            t.relname as table_name,
            i.relname as index_name,
            am.amname,
            t.oid as table_oid,
            array_to_string(array_agg(a.attname), ', ') as column_names
        FROM pg_index ix
        JOIN pg_class t ON (t.oid = ix.indrelid AND t.relkind = 'r')
        JOIN pg_class i ON (i.oid = ix.indexrelid)
        JOIN pg_am am ON (am.oid = i.relam)
        JOIN pg_attribute a ON (a.attrelid = t.oid AND a.attnum = ANY(ix.indkey))
        WHERE i.relname = $1
        GROUP BY t.relname, i.relname, am.amname, t.oid;
    "#;
    let result = client.borrow_mut().query(index_type_query, &[&index]).unwrap();

    let (table_name, index_type, columns, table_oid) = match result.get(0) {
        Some(row) => {
            let table_name: String = row.get(0);
            let table_oid: postgres::types::Oid = row.get(3);
            let index_type: String = row.get(2);
            let column_str: String = row.get(4);
            let columns = column_str.split(",")
                .map(|s| {
                    s.to_string().trim().to_string()
                })
                .collect();
            (table_name, index_type, columns, table_oid)
        }
        None => ("".to_string(), "".to_string(), vec![], 0),
    };
    index_info.table_oid = table_oid;
    index_info.index_type = index_type;
    index_info.columns = columns;
    index_info.table_name = table_name.clone();
    println!("t: {:?} {:?}", table_name, table_oid);
    let table_indexed_attributes_query = r#"
        SELECT COALESCE(array_agg(cast(a.attname as TEXT)), '{}')
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = $1
        AND    i.indisprimary;
    "#;
    let result_indexed_attributes = client.borrow_mut().query(table_indexed_attributes_query, &[&table_oid]).unwrap();
    println!("{:?}", result_indexed_attributes);
    let indexed_attributes = match result_indexed_attributes.get(0) {
        Some(indexed_attributes) => {
            let indexed_columns: Vec<String> = indexed_attributes.get(0);
            indexed_columns
        }
        None => vec![],
    };
    index_info.primary_indexed_attributes = indexed_attributes;
    index_info
}

pub fn get_metadata_page(client: Arc<RefCell<Client>>, index_name: String) -> MetadataPage {
    let btree_metadata_query = r#"
        SELECT
            version,
            root,
            level,
            fastroot,
            fastlevel
        FROM bt_metap($1);
    "#;
    println!("Index name: {}", index_name);
    let result_metadata = client.borrow_mut().query(btree_metadata_query, &[&index_name]).unwrap();
    let metadata_page = match result_metadata.get(0) {
        Some(row) => {
            let version: i32 = row.get(0);
            let root: i64 = row.get(1);
            let level: i64 = row.get(2);
            let fast_root: i64 = row.get(3);
            let fast_level: i64 = row.get(4);
            MetadataPage::new(version, root, level, fast_root, fast_level)
        }
        None => MetadataPage::new(0, 0, 0, 0, 0),
    };
    metadata_page
}

pub fn get_page(client: Arc<RefCell<Client>>, page_id: i64, index_name: String, index_info: Rc<IndexInfo>) -> Page {
    let page_query = r#"
        SELECT
        blkno,
        type::text,
        live_items,
        dead_items,
        avg_item_size,
        page_size,
        free_size,
        btpo_prev,
        btpo_next,
        btpo_level,
        btpo_flags
        FROM bt_page_stats($1, $2)
    "#.to_string();
    let result_page = client.borrow_mut().query(&page_query, &[&index_name, &page_id]).unwrap();
    let mut page = match result_page.get(0) {
        Some(row) => {
            let block_number: i64 = row.get(0);
            let rtype: String = row.get(1);
            let level: i64 = row.get(9);
            let next_page_id: i64 = row.get(8);
            let prev_page_id: i64 = row.get(7);
            Page::new(block_number, level, rtype == "l", rtype == "r", next_page_id, prev_page_id)
        }
        None => Page::new(page_id, 0, false, false, 0, 0),
    };
    let (items, prev_item, next_item) = get_items(client.clone(), Rc::new(page.clone()), index_name.clone(), index_info.clone());

    page.items = items;
    if next_item.is_some() {
        page.high_key = Some(next_item.unwrap().value)
    }
    page.prev_item = match prev_item {
        None => { None }
        Some(item) => { Some(Box::new(item)) }
    };
    page
}

pub fn get_row(client: Arc<RefCell<Client>>, ct_ids: Vec<Tid>, index_info: Rc<IndexInfo>) -> HashMap<Tid, RowData> {
    let primary_key_columns = index_info.primary_indexed_attributes.iter().map(|pk| format!("{}::text", pk)).collect::<Vec<String>>().join(", ");
    let columns = index_info.columns.iter().map(|pk| format!("{}::text", pk)).collect::<Vec<String>>().join(", ");


    let ct_ids_array = ct_ids.iter()
        .map(|tid| format!("({},{})", tid.block_number, tid.offset_number))
        .collect::<Vec<String>>()
        .join(", ");

    let mut row_query = "".to_string();
    if primary_key_columns == "" {
        row_query = format!(r#"
        SELECT ctid, {}
        FROM {}
        WHERE ctid IN (SELECT ('('|| block_num || ',' || offset_num || ')')::tid FROM unnest(ARRAY[{}]) AS t(block_num integer , offset_num integer))
    "#, columns, index_info.table_name, ct_ids_array);
    } else {
        row_query = format!(r#"
        SELECT ctid, {}, {}
        FROM {}
        WHERE ctid IN (SELECT ('('|| block_num || ',' || offset_num || ')')::tid FROM unnest(ARRAY[{}]) AS t(block_num integer , offset_num integer))
    "#, primary_key_columns, columns, index_info.table_name, ct_ids_array);
    }
    let rows = client.borrow_mut().query(&row_query, &[]).unwrap();


    // Rows data in page should Map<ct_id, RowData>
    // Row Data -> primary key data, standard_index_data
    let mut row_data: HashMap<Tid, RowData> = HashMap::new();
    for row in rows.iter() {
        let mut i = 0;
        let ct_id: Tid = row.get(i);
        i += 1;

        let mut pks_left = index_info.primary_indexed_attributes.len();
        let mut pk_values: Vec<String> = vec![];
        while pks_left > 0 {
            let pk: String = row.get(i);
            pks_left -= 1;
            i += 1;
            &pk_values.push(pk);
        }

        let mut cols_left = index_info.columns.len();
        let mut col_vals: Vec<String> = vec![];
        while cols_left > 0 {
            let col: String = row.get(i);
            cols_left -= 1;
            i += 1;
            &col_vals.push(col);
        }
        row_data.insert(ct_id, RowData::new(pk_values, col_vals));
    }

    row_data
}

pub fn get_items(client: Arc<RefCell<Client>>, page: Rc<Page>, index_name: String, index_info: Rc<IndexInfo>) -> (Vec<Item>, Option<Item>, Option<Item>) {
    let btree_item_query = r#"
        SELECT *
        FROM bt_page_items($1, $2);
    "#;
    let result_items = client.borrow_mut().query(btree_item_query, &[&index_name, &page.id]).unwrap();
    let mut items: Vec<Item> = vec![];

    if page.is_leaf {
        let ct_ids: Vec<Tid> = result_items.iter().map(|item| {
            let ct_id: Tid = item.get(1);
            ct_id
        }).collect();
        let rows = get_row(client.clone(), ct_ids, index_info.clone());
        for item in result_items.iter() {
            let row_id: Tid = item.get(1);
            let row_id_value = rows.get(&row_id);
            let value: String = match row_id_value {
                Some(row_data) => row_data.byte_values.clone().unwrap_or_else(|| item.get(5)),
                None => item.get(5),
            };
            items.push(Item::new(value, None, Some(row_id.block_number as i64), Some(row_id)));
        }
    } else {
        for item in result_items.iter() {
            let next_page_tid: Tid = item.get(1);
            let next_page_pointer: i64 = next_page_tid.block_number as i64;
            let child_page = get_page(client.clone(), next_page_pointer, index_name.clone(), index_info.clone());

            let value: String = match item.get(5) {
                Some(value) => value,
                None => "".to_string(),
            };
            items.push(Item::new(value, Some(Box::new(child_page)), Some(next_page_pointer), None));
        }
    }

    let mut prev_item: Option<Item> = None;
    let next_item: Option<Item> = match page.next_page_id {
        None => None,
        Some(_) => { Some(items.remove(0)) }
    };
    if page.prev_page_id.is_some() || (page.prev_page_id.is_none() && !page.is_root && !page.is_leaf) {
        prev_item = Some(items.remove(0));
    }

    (items, prev_item, next_item)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use postgres::Client;
    use crate::core::{Tid};
    use crate::db::client::{get_index_info, get_metadata_page, get_row, IndexInfo};
    use crate::db::get_page;
    use crate::core::btree::generate_btree;

    fn setup_test_data(client: Arc<RefCell<Client>>) {
        tear_down_test_data(Arc::clone(&client));
        client.borrow_mut().batch_execute(
            "CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )").unwrap();

        client.borrow_mut().batch_execute(
            "CREATE INDEX IF NOT EXISTS idx_users_name_email ON test_table (name, email)"
        ).unwrap();
    }

    fn insert_data(client: Arc<RefCell<Client>>) {
        client.borrow_mut().batch_execute(
            "INSERT INTO test_table(id, name, email) VALUES \
            (1, 'foo', 'foo@gmail.com'),\
            (2, 'bar', 'bar@gmail.com'),\
            (3, 'alice', 'alice@gmail.com'),\
            (4, 'foo2', 'foo2@gmail.com'),\
            (5, 'bob2', 'bob2@gmail.com'),\
            (6, 'alice2', 'alice2@gmail.com'),\
            (7, 'foo3', 'foo3@gmail.com'),\
            (8, 'bob3', 'bob3@gmail.com')"
        ).unwrap()
    }

    fn tear_down_test_data(client: Arc<RefCell<Client>>) {
        client.borrow_mut().batch_execute(
            "DROP TABLE IF EXISTS test_table"
        ).unwrap();
    }

    fn assert_index_info(expected_index_info: &IndexInfo, actual_index_info: &IndexInfo) {
        assert_eq!(expected_index_info.index_type, actual_index_info.index_type);
        assert_eq!(expected_index_info.columns, actual_index_info.columns);
        assert_eq!(expected_index_info.table_name, actual_index_info.table_name);
        assert_eq!(expected_index_info.primary_indexed_attributes, actual_index_info.primary_indexed_attributes);
    }

    #[test]
    pub fn test_index_info() {
        let client_ref = Arc::new(RefCell::new(super::init_client(
            "localhost".to_string(), "5432".to_string(), "postgres".to_string(), "postgres".to_string(), "".to_string(),
        )));
        setup_test_data(Arc::clone(&client_ref));
        let actual_index_info = get_index_info(Arc::clone(&client_ref), "idx_users_name_email".to_string());
        let expected_index_info = IndexInfo {
            index_type: "btree".to_string(),
            columns: vec!["name".to_string(), "email".to_string()],
            table_name: "test_table".to_string(),
            primary_indexed_attributes: vec!["id".to_string()],
            table_oid: 0,
        };
        assert_index_info(&expected_index_info, &actual_index_info);
        assert_ne!(0, actual_index_info.table_oid);
        tear_down_test_data(Arc::clone(&client_ref));
    }

    #[test]
    pub fn test_metadata_page_information() {
        let client_ref = Arc::new(RefCell::new(super::init_client(
            "localhost".to_string(), "5432".to_string(), "postgres".to_string(), "postgres".to_string(), "".to_string(),
        )));
        setup_test_data(Arc::clone(&client_ref));
        let actual_metadata_page = get_metadata_page(Arc::clone(&client_ref), "idx_users_name_email".to_string());
        let expected_metadata_page = super::MetadataPage::new(1, 1, 1, 0, 0);
        assert_ne!(expected_metadata_page, actual_metadata_page);
        tear_down_test_data(Arc::clone(&client_ref));
    }

    #[test]
    pub fn test_get_row() {
        // Todo: update this test with predictable data
        let client_ref = Arc::new(RefCell::new(super::init_client(
            "localhost".to_string(), "5432".to_string(), "postgres".to_string(), "postgres".to_string(), "".to_string(),
        )));
        setup_test_data(Arc::clone(&client_ref));
        insert_data(Arc::clone(&client_ref));
        let actual_index_info = get_index_info(Arc::clone(&client_ref), "idx_users_name_email".to_string());
        let row_data = get_row(Arc::clone(&client_ref), vec![Tid { block_number: 0, offset_number: 1 }, Tid { block_number: 0, offset_number: 2 }], Rc::new(actual_index_info));
        for (k, v) in row_data.iter() {
            println!("Key: {}, Value: {:?}", k, v);
        }
        tear_down_test_data(Arc::clone(&client_ref));
    }

    #[test]
    pub fn test_get_page() {
        // Todo: update this test with predictable data
        let client_ref = Arc::new(RefCell::new(super::init_client(
            "localhost".to_string(), "5432".to_string(), "postgres".to_string(), "postgres".to_string(), "".to_string(),
        )));
        setup_test_data(Arc::clone(&client_ref));
        insert_data(Arc::clone(&client_ref));
        let index_name = "idx_users_name_email".to_string();
        let actual_index_info = get_index_info(Arc::clone(&client_ref), index_name.clone());
        let metadata_page = get_metadata_page(Arc::clone(&client_ref), index_name.clone());
        let page = get_page(Arc::clone(&client_ref), metadata_page.root, index_name, Rc::new(actual_index_info));
        println!("{:?}", page);
        tear_down_test_data(Arc::clone(&client_ref));
    }

    #[test]
    pub fn test_get_tree() {
        let client_ref = Arc::new(RefCell::new(super::init_client(
            "localhost".to_string(), "5432".to_string(), "postgres".to_string(), "".to_string(), "".to_string(),
        )));
        setup_test_data(Arc::clone(&client_ref));
        insert_data(Arc::clone(&client_ref));
        let index_name = "idx_users_name_email".to_string();
        let actual_index_info = get_index_info(Arc::clone(&client_ref), index_name.clone());
        let tree = generate_btree(Arc::clone(&client_ref), index_name.clone(), Rc::new(actual_index_info));
        tear_down_test_data(Arc::clone(&client_ref));
    }
}