use {
    super::{err_into, lock, FjallStorage, Snapshot, State},
    async_trait::async_trait,
    fjall::TxPartition,
    futures::stream::iter,
    gluesql_core::{
        data::{Key, Schema},
        error::{Error, Result},
        store::{DataRow, RowIter, Store},
    },
    std::str,
};

#[async_trait(?Send)]
impl Store for FjallStorage {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let (txid, created_at) = match self.state {
            State::Transaction {
                txid, created_at, ..
            } => (txid, created_at),
            State::Idle => lock::register(&self.tx_partition, self.id_offset)?,
        };
        let lock_txid = lock::fetch(&self.tx_partition, txid, created_at, self.tx_timeout)?;

        self.schema_partition
            .inner()
            .iter()
            .map(move |item| {
                let (_, value) = item.map_err(err_into)?;
                let snapshot: Snapshot<Schema> = bincode::deserialize(&value).map_err(err_into)?;
                let schema = snapshot.extract(txid, lock_txid);

                Ok(schema)
            })
            .filter_map(|result| result.transpose())
            .collect::<Result<Vec<_>>>()
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let (txid, created_at, temp) = match self.state {
            State::Transaction {
                txid, created_at, ..
            } => (txid, created_at, false),
            State::Idle => lock::register(&self.tx_partition, self.id_offset)
                .map(|(txid, created_at)| (txid, created_at, true))?,
        };
        let lock_txid = lock::fetch(&self.tx_partition, txid, created_at, self.tx_timeout)?;

        let schema: Option<Schema> = self
            .schema_partition
            .get(table_name)
            .map_err(err_into)?
            .map(|v| bincode::deserialize(&v))
            .transpose()
            .map_err(err_into)?
            .and_then(|snapshot: Snapshot<Schema>| snapshot.extract(txid, lock_txid));

        if temp {
            lock::unregister(&self.tx_partition, txid)?;
        }

        Ok(schema)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        let (txid, created_at) = match self.state {
            State::Transaction {
                txid, created_at, ..
            } => (txid, created_at),
            State::Idle => {
                return Err(Error::StorageMsg(
                    "conflict - fetch_data failed, lock does not exist".to_owned(),
                ));
            }
        };
        let lock_txid = lock::fetch(&self.tx_partition, txid, created_at, self.tx_timeout)?;

        let key = key.to_cmp_be_bytes()?;

        let table_lock = self.tables.read().expect("lock is poisoned");
        let table = table_lock.get(table_name).expect("table should exist");

        let row = table
            .get(key)
            .map_err(err_into)?
            .map(|v| bincode::deserialize(&v))
            .transpose()
            .map_err(err_into)?
            .and_then(|snapshot: Snapshot<DataRow>| snapshot.extract(txid, lock_txid));

        Ok(row)
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let (txid, created_at) = match self.state {
            State::Transaction {
                txid, created_at, ..
            } => (txid, created_at),
            State::Idle => {
                return Err(Error::StorageMsg(
                    "conflict - scan_data failed, lock does not exist".to_owned(),
                ));
            }
        };
        let lock_txid = lock::fetch(&self.tx_partition, txid, created_at, self.tx_timeout)?;

        let table_lock = self.tables.read().expect("lock is poisoned");
        let table = table_lock
            .get(table_name)
            .cloned()
            .expect("table should exist");

        let reader = Reader::new(table, |table| {
            Box::new(
                table
                    .inner()
                    .iter()
                    .map(move |item| {
                        let (key, value) = item.map_err(err_into)?;

                        let snapshot: Snapshot<DataRow> =
                            bincode::deserialize(&value).map_err(err_into)?;

                        let row = snapshot.extract(txid, lock_txid);
                        let item = row.map(|row| (Key::Bytea(key.into()), row));

                        Ok::<_, Error>(item)
                    })
                    .filter_map(|item| item.transpose()),
            )
        });

        Ok(Box::pin(iter(reader)))
    }
}

type BoxedMerge<'a> = Box<dyn Iterator<Item = Result<(Key, DataRow)>> + 'a>;

use self_cell::self_cell;

self_cell!(
    pub struct Reader<'a> {
        owner: TxPartition,

        #[covariant]
        dependent: BoxedMerge,
    }
);

impl<'a> Iterator for Reader<'a> {
    type Item = Result<(Key, DataRow)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next())
    }
}
