use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use postgres_types::{FromSql, IsNull, to_sql_checked, ToSql, Type};
use postgres_types::private::BytesMut;
use bytes::{Buf, BufMut};

#[derive(PartialEq, Debug)]
pub struct MetadataPage {
    version: i32,
    root: i64,
    level: i64,
    fast_root: i64,
    fast_level: i64,
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


pub struct Page {
    pub id: u32,
    pub level: u32,
    pub is_leaf: bool,
    pub is_root: bool,
    pub items: Vec<Item>,
    pub prev_page_id: Option<u32>,
    pub next_page_id: Option<u32>,
    pub high_key: Option<Vec<u8>>,
    pub prev_item: Option<Box<Item>>,
    pub nb_items: Option<u32>,
}

pub struct Item {
    pub value: Vec<u8>,
    pub child: Option<Box<Page>>,
    pub pointer: Option<u32>,
    pub obj_id: Option<u32>,
}

pub struct Tree {
    metadata_page: Option<MetadataPage>,
    root: Page,
    index_name: String,
    table_name: String,
    columns: Vec<String>,
    index_type: Option<String>,
}

#[derive(Debug)]
pub struct RowData {
    pub primary_key_data: Vec<String>,
    pub column_data: Vec<String>,
}

#[derive(Debug)]
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