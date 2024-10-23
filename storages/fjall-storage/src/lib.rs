#![deny(clippy::str_to_string)]

// mod alter_table;
mod error;
// mod gc;
// mod index;
// mod index_mut;
mod index_sync;
mod key;
mod lock;
mod snapshot;
mod store;
// mod store_mut;
// mod transaction;

// re-export
pub use fjall;

use fjall::{TxKeyspace, TxPartition};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use {
    self::snapshot::Snapshot,
    error::err_into,
    gluesql_core::{
        data::Schema,
        error::{Error, Result},
        store::Metadata,
    },
};

/// default transaction timeout : 1 hour
const DEFAULT_TX_TIMEOUT: u128 = 3600 * 1000;

#[derive(Debug, Clone)]
pub enum State {
    Idle,
    Transaction {
        txid: u64,
        created_at: u128,
        autocommit: bool,
    },
}

#[derive(Clone)]
pub struct FjallStorage {
    pub keyspace: TxKeyspace,

    tables: Arc<RwLock<HashMap<String, TxPartition>>>,

    tx_partition: TxPartition,
    schema_partition: TxPartition,

    pub id_offset: u64,

    // id_partition: TxPartition,

    //  pub tree: Db,
    pub state: State,
    /// transaction timeout in milliseconds
    pub tx_timeout: Option<u128>,
}

// type ExportData<T> = (u64, Vec<(Vec<u8>, Vec<u8>, T)>);

impl FjallStorage {
    pub fn new<P: AsRef<std::path::Path>>(folder: P) -> Result<Self> {
        let keyspace = fjall::Config::new(folder)
            .open_transactional()
            .map_err(err_into)?;

        let tx_partition = keyspace
            .open_partition("tx", Default::default())
            .map_err(err_into)?;

        let schema_partition = keyspace
            .open_partition("schema", Default::default())
            .map_err(err_into)?;

        // let tree = sled::open(filename).map_err(err_into)?;
        // let id_offset = get_id_offset(&tree)?;
        let state = State::Idle;
        let tx_timeout = Some(DEFAULT_TX_TIMEOUT);

        Ok(Self {
            keyspace,

            tables: Default::default(), // TODO: need to recover tables from schema

            tx_partition,
            schema_partition,

            id_offset: 0, // TODO:

            state,
            tx_timeout,
        })
    }

    pub fn set_transaction_timeout(&mut self, tx_timeout: Option<u128>) {
        self.tx_timeout = tx_timeout;
    }

    /* pub fn export(&self) -> Result<ExportData<impl Iterator<Item = Vec<Vec<u8>>>>> {
        unimplemented!()

        /* let id_offset = self.id_offset + self.tree.generate_id().map_err(err_into)?;
        let data = self.tree.export();

        Ok((id_offset, data)) */
    } */

    /* pub fn import(&mut self, export: ExportData<impl Iterator<Item = Vec<Vec<u8>>>>) -> Result<()> {
        unimplemented!()

        /* let (new_id_offset, data) = export;
        let old_id_offset = get_id_offset(&self.tree)?;

        self.tree.import(data);

        if new_id_offset > old_id_offset {
            self.tree
                .insert("id_offset", &new_id_offset.to_be_bytes())
                .map_err(err_into)?;

            self.id_offset = new_id_offset;
        }

        Ok(()) */
    } */
}

/* impl TryFrom<Config> for FjallStorage {
    type Error = Error;

    fn try_from(config: Config) -> Result<Self> {
        let tree = config.open().map_err(err_into)?;
        let id_offset = get_id_offset(&tree)?;
        let state = State::Idle;
        let tx_timeout = Some(DEFAULT_TX_TIMEOUT);

        Ok(Self {
            tree,
            id_offset,
            state,
            tx_timeout,
        })
    }
} */

/* fn get_id_offset(tree: &Db) -> Result<u64> {
    tree.get("id_offset")
        .map_err(err_into)?
        .map(|id| {
            id.as_ref()
                .try_into()
                .map_err(err_into)
                .map(u64::from_be_bytes)
        })
        .unwrap_or(Ok(0))
} */

fn fetch_schema(tree: &TxPartition, table_name: &str) -> Result<Option<Snapshot<Schema>>, Error> {
    let value = tree.get(table_name).map_err(err_into)?;

    let schema_snapshot = value
        .map(|v| bincode::deserialize(&v))
        .transpose()
        .map_err(err_into)?;

    Ok(schema_snapshot)
}

impl Metadata for FjallStorage {}
impl gluesql_core::store::CustomFunction for FjallStorage {}
impl gluesql_core::store::CustomFunctionMut for FjallStorage {}
