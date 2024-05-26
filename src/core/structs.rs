use std::error::Error;
use std::fmt::{Display, Formatter};
use std::rc::Rc;
use bytes::{Buf, BufMut, BytesMut};
use postgres_types::{FromSql, IsNull, to_sql_checked, ToSql, Type};
use crate::db::IndexInfo;
use serde::{Serialize, Deserialize};


#[derive(PartialEq, Debug, Serialize, Deserialize)]
pub struct MetadataPage {
    pub version: i32,
    pub root: i64,
    pub level: i64,
    pub fast_root: i64,
    pub fast_level: i64,
}

impl MetadataPage {
    pub fn new(version: i32, root: i64, level: i64, fast_root: i64, fast_level: i64) -> MetadataPage {
        MetadataPage {
            version,
            root,
            level,
            fast_root,
            fast_level,
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Page {
    pub id: i64,
    pub level: i64,
    pub is_leaf: bool,
    pub is_root: bool,
    pub items: Vec<Item>,
    pub prev_page_id: Option<i64>,
    pub next_page_id: Option<i64>,
    pub high_key: Option<String>,
    pub prev_item: Option<Box<Item>>,
    pub nb_items: Option<i32>,
}

impl Page {
    pub fn new(block_number: i64, level: i64, is_leaf: bool, is_root: bool, next_page_id: i64, prev_page_id: i64) -> Self {
        Self {
            id: block_number,
            level,
            is_leaf,
            is_root,
            items: vec![],
            prev_page_id: Some(prev_page_id),
            next_page_id: Some(next_page_id),
            high_key: None,
            prev_item: None,
            nb_items: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Item {
    pub value: String,
    pub child: Option<Box<Page>>,
    pub pointer: Option<i64>,
    pub obj_id: Option<Tid>,
}

impl Item {
    pub fn new(value: String, child: Option<Box<Page>>, pointer: Option<i64>, obj_id: Option<Tid>) -> Self {
        Self {
            value,
            child,
            pointer,
            obj_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Tree {
    metadata_page: Option<MetadataPage>,
    pub root: Page,
    index_name: String,
    table_name: String,
    columns: Vec<String>,
    pub index_type: Option<String>,
}

impl Tree {
    pub fn new(metadata_page: MetadataPage, root: Page, index_name: String, index_info: Rc<IndexInfo>) -> Self {
        Self {
            metadata_page: Some(metadata_page),
            root,
            index_name,
            table_name: index_info.table_name.clone(),
            columns: index_info.columns.clone(),
            index_type: Some("btree".to_string()),
        }
    }
}

#[derive(Debug)]
pub struct RowData {
    pub primary_key_data: Option<Vec<String>>,
    pub column_data: Option<Vec<String>>,
    pub byte_values: Option<String>,
}

impl RowData {
    pub fn new(primary_key_data: Vec<String>, column_data: Vec<String>) -> Self {
        Self {
            primary_key_data: Some(primary_key_data),
            column_data: Some(column_data),
            byte_values: None,
        }
    }
    pub fn new_bytes(byte_values: String) -> Self {
        Self {
            primary_key_data: None,
            column_data: None,
            byte_values: Some(byte_values),
        }
    }
}


#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Tid {
    pub block_number: u32,
    pub offset_number: u16,
}

impl Display for Tid {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({},{})", self.block_number, self.offset_number)
    }
}


impl ToSql for Tid {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> where Self: Sized {
        let mut tid_str = format!("({},{})", self.block_number, self.offset_number);

        w.reserve(tid_str.len());
        w.put_slice(tid_str.as_bytes());

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool where Self: Sized {
        matches!(*ty,Type::TID)
    }

    to_sql_checked!();
}

fn tid_from_sql(mut buf: &[u8]) -> Result<Tid, Box<dyn Error + Sync + Send>> {
    Ok(Tid {
        block_number: buf.get_u32(),
        offset_number: buf.get_u16(),
    })
}

impl<'a> FromSql<'a> for Tid {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        tid_from_sql(raw)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty,Type::TID)
    }
}