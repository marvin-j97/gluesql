use fjall::Slice;

/* const TEMP_DATA: &str = "temp_data/";
const TEMP_SCHEMA: &str = "temp_schema/";
const TEMP_INDEX: &str = "temp_index/"; */

/* pub fn data_prefix(table_name: &str) -> String {
    format!("data/{table_name}/")
}

pub fn data(table_name: &str, key: Vec<u8>) -> IVec {
    let key = data_prefix(table_name).into_bytes().into_iter().chain(key);

    IVec::from_iter(key)
} */

/* macro_rules! prefix {
    ($txid: ident, $prefix: ident) => {
        $prefix
            .to_owned()
            .into_bytes()
            .into_iter()
            .chain($txid.to_be_bytes().iter().copied())
    };
} */

/* pub fn temp_data_prefix(txid: u64) -> Slice {
    Slice::from_iter(prefix!(txid, TEMP_DATA))
}

pub fn temp_schema_prefix(txid: u64) -> Slice {
    Slice::from_iter(prefix!(txid, TEMP_SCHEMA))
}

pub fn temp_index_prefix(txid: u64) -> Slice {
    Slice::from_iter(prefix!(txid, TEMP_INDEX))
}

pub fn temp_data(txid: u64, data_key: &Slice) -> Slice {
    Slice::from_iter(prefix!(txid, TEMP_DATA).chain(data_key.iter().copied()))
}

pub fn temp_data_str(txid: u64, data_key: &str) -> Slice {
    Slice::from_iter(prefix!(txid, TEMP_DATA).chain(data_key.as_bytes().iter().copied()))
}

pub fn temp_schema(txid: u64, table_name: &str) -> Slice {
    Slice::from_iter(prefix!(txid, TEMP_SCHEMA).chain(table_name.as_bytes().iter().copied()))
}

pub fn temp_index(txid: u64, index_key: &[u8]) -> Slice {
    Slice::from_iter(prefix!(txid, TEMP_INDEX).chain(index_key.iter().copied()))
} */
