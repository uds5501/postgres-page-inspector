use std::cell::RefCell;
use std::sync::Arc;
use postgres;
use postgres::{Client, Row};

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
    pub table_indexed_attributes: Vec<String>,
}

pub fn get_index_info(client: Arc<RefCell<Client>>, index: String) -> IndexInfo {
    let mut index_info = IndexInfo {
        index_type: "".to_string(),
        columns: vec![],
        table_name: "".to_string(),
        table_oid: 0,
        table_indexed_attributes: vec![],
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
    index_info.table_indexed_attributes = indexed_attributes;
    index_info
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::Arc;
    use postgres::Client;
    use crate::db::client::{get_index_info, IndexInfo};

    fn setup_test_data(client: Arc<RefCell<Client>>) {
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

    fn tear_down_test_data(client: Arc<RefCell<Client>>) {
        client.borrow_mut().batch_execute(
            "DROP TABLE IF EXISTS test_table"
        ).unwrap();
    }

    fn assert_index_info(expected_index_info: &IndexInfo, actual_index_info: &IndexInfo) {
        assert_eq!(expected_index_info.index_type, actual_index_info.index_type);
        assert_eq!(expected_index_info.columns, actual_index_info.columns);
        assert_eq!(expected_index_info.table_name, actual_index_info.table_name);
        assert_eq!(expected_index_info.table_indexed_attributes, actual_index_info.table_indexed_attributes);
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
            table_indexed_attributes: vec!["id".to_string()],
            table_oid: 0,
        };
        assert_index_info(&expected_index_info, &actual_index_info);
        assert_ne!(0, actual_index_info.table_oid);
        tear_down_test_data(Arc::clone(&client_ref));
    }
}