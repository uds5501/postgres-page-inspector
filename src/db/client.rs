use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::format;
use std::iter::Map;
use std::rc::Rc;
use std::sync::Arc;
use postgres;
use postgres::{Client, Row};
use crate::core::btree::{Item, MetadataPage, RowData, Tid};
use crate::core::Page;

pub fn init_client(host: String, port: String, db: String) -> Client {
    Client::connect(&format!("host={} port={} dbname={}", host, port, db), postgres::NoTls).unwrap()
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

    let table_indexed_attributes_query = r#"
        SELECT array_agg(cast(a.attname as TEXT))
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = $1
        AND    i.indisprimary;
    "#;
    let result_indexed_attributes = client.borrow_mut().query(table_indexed_attributes_query, &[&table_oid]).unwrap();
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

// pub fn get_items(client: Arc<RefCell<Client>>, page: Rc<Page>, index_name: String, index_info: Rc<IndexInfo>) -> (Item, Item, Item) {
//     let btree_item_query = r#"
//         SELECT *
//         FROM bt_page_items($1, $2);
//     "#;
//     let result_items = client.borrow_mut().query(btree_item_query, &[&index_name, &page.id]).unwrap();
//     let items = vec![];
//     // for item  in result_items.iter() {
//     //     items.push()
//     // }
//     (Item::new(vec![], None, None, None), Item::new(vec![], None, None, None), Item::new(vec![], None, None, None))
// }

pub fn get_row(client: Arc<RefCell<Client>>, ct_ids: Vec<Tid>, index_info: Rc<IndexInfo>) -> HashMap<String, RowData> {
    let in_query: String = ct_ids.iter().map(|ct_id| format!("{}::tid", ct_id))
        .collect::<Vec<String>>().join(", ");
    println!("IN: {}", in_query);
    let primary_key_columns = index_info.primary_indexed_attributes.iter().map(|pk| format!("{}::text", pk)).collect::<Vec<String>>().join(", ");
    let columns = index_info.columns.iter().map(|pk| format!("{}::text", pk)).collect::<Vec<String>>().join(", ");


    let ct_ids_array = ct_ids.iter()
        .map(|tid| format!("({},{})", tid.block_number, tid.offset_number))
        .collect::<Vec<String>>()
        .join(", ");

    let row_query = format!(r#"
        SELECT ctid, {}, {}
        FROM {}
        WHERE ctid IN (SELECT ('('|| block_num || ',' || offset_num || ')')::tid FROM unnest(ARRAY[{}]) AS t(block_num integer , offset_num integer))
    "#, primary_key_columns, columns, index_info.table_name, ct_ids_array);

    println!("ROW QUERY: {}", row_query);
    let rows = client.borrow_mut().query(&row_query, &[]).unwrap();


    // Rows data in page should Map<ct_id, RowData>
    // Row Data -> primary key data, standard_index_data
    let mut row_data: HashMap<String, RowData> = HashMap::new();
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
        row_data.insert(format!("{}", ct_id), RowData {
            primary_key_data: pk_values,
            column_data: col_vals,
        });
    }

    row_data
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use postgres::Client;
    use crate::core::Tid;
    use crate::db::client::{get_index_info, get_metadata_page, get_row, IndexInfo};

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
            (1, 'foo', 'foo1@gmail.com'),\
            (2, 'bar', 'bar2@gmail.com'),\
            (3, 'alice', 'alice3@gmail.com'),\
            (4, 'bob', 'bob4@gmail.com')"
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
            "localhost".to_string(), "5432".to_string(), "postgres".to_string(),
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
            "localhost".to_string(), "5432".to_string(), "postgres".to_string(),
        )));
        setup_test_data(Arc::clone(&client_ref));
        let actual_metadata_page = get_metadata_page(Arc::clone(&client_ref), "idx_users_name_email".to_string());
        let expected_metadata_page = super::MetadataPage::new(1, 1, 1, 0, 0);
        assert_ne!(expected_metadata_page, actual_metadata_page);
        tear_down_test_data(Arc::clone(&client_ref));
    }

    #[test]
    pub fn test_get_row() {
        let client_ref = Arc::new(RefCell::new(super::init_client(
            "localhost".to_string(), "5432".to_string(), "postgres".to_string(),
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
}