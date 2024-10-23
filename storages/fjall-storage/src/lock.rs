use {
    super::{err_into, State},
    crate::error::{StorageError, TransactionError},
    fjall::TxPartition,
    gluesql_core::error::{Error, Result},
    serde::{Deserialize, Serialize},
    std::time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct TxData {
    pub txid: u64,
    pub alive: bool,
    pub created_at: u128,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Lock {
    pub lock_txid: Option<u64>,
    pub lock_created_at: u128,
    pub gc_txid: Option<u64>,
    // TODO: support serializable transaction isolation level
    // - prev_done_at: u128,
}

pub fn get_txdata_key(txid: u64) -> Vec<u8> {
    "txd/"
        .to_owned()
        .into_bytes()
        .into_iter()
        .chain(txid.to_be_bytes().iter().copied())
        .collect::<Vec<_>>()
}

pub fn register(partition: &TxPartition, txid: u64) -> Result<(u64, u128)> {
    let key = get_txdata_key(txid);

    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(err_into)?
        .as_millis();

    let tx_data = TxData {
        txid,
        alive: true,
        created_at,
    };

    bincode::serialize(&tx_data)
        .map_err(err_into)
        .map(|tx_data| partition.inner().insert(key, tx_data))?
        .map_err(err_into)?;

    Ok((txid, created_at))
}

const LOCK_KEY: &str = "lock/";

pub fn fetch(
    partition: &TxPartition,
    txid: u64,
    created_at: u128,
    tx_timeout: Option<u128>,
) -> Result<Option<u64>> {
    let Lock {
        lock_txid, gc_txid, ..
    } = partition
        .get(LOCK_KEY)
        .map_err(err_into)?
        .map(|bytes| bincode::deserialize(&bytes))
        .transpose()
        .map_err(err_into)?
        .unwrap_or_default();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(err_into)?
        .as_millis();

    if tx_timeout.map(|tx_timeout| now >= tx_timeout + created_at) == Some(true) {
        return Err(Error::StorageMsg(
            "fetch failed - expired transaction has used (timeout)".to_owned(),
        ));
    } else if gc_txid.is_some() && Some(txid) <= gc_txid {
        return Err(Error::StorageMsg(
            "fetch failed - expired transaction has used (txid)".to_owned(),
        ));
    }

    Ok(lock_txid)
}

pub enum LockAcquired {
    Success { txid: u64, autocommit: bool },
    RollbackAndRetry { lock_txid: u64 },
}

pub fn acquire(
    tree: &TxPartition,
    state: &State,
    tx_timeout: Option<u128>,
) -> Result<LockAcquired, Error> {
    // TODO: update_fetch or transaction

    let Lock {
        lock_txid,
        lock_created_at,
        gc_txid,
    } = tree
        .get(LOCK_KEY)
        .map_err(err_into)?
        .map(|bytes| bincode::deserialize(&bytes))
        .transpose()
        .map_err(err_into)?
        .unwrap_or_default();

    let (txid, created_at, autocommit) = match state {
        State::Transaction {
            txid,
            created_at,
            autocommit,
        } => (*txid, *created_at, *autocommit),
        State::Idle => {
            return Err(StorageError::TransactionError(TransactionError::Abort(
                "conflict - cannot acquire lock from idle state",
            ))
            .into());
        }
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(err_into)?
        .as_millis();

    if tx_timeout.map(|tx_timeout| now >= tx_timeout + created_at) == Some(true) {
        return Err(StorageError::TransactionError(TransactionError::Abort(
            "acquire failed - expired transaction has used (timeout)",
        ))
        .into());
    } else if gc_txid.is_some() && Some(txid) <= gc_txid {
        return Err(StorageError::TransactionError(TransactionError::Abort(
            "acquire failed - expired transaction has used (txid)",
        ))
        .into());
    }

    let txid = match lock_txid {
        Some(lock_txid) => {
            if tx_timeout.map(|tx_timeout| now >= tx_timeout + lock_created_at) == Some(true) {
                return Ok(LockAcquired::RollbackAndRetry { lock_txid });
            } else if txid != lock_txid {
                return Err(StorageError::TransactionError(TransactionError::Abort(
                    "database is locked",
                ))
                .into());
            }

            txid
        }
        None => {
            let lock = Lock {
                lock_txid: Some(txid),
                lock_created_at: created_at,
                gc_txid,
            };

            let lock = bincode::serialize(&lock).map_err(err_into)?;
            tree.insert(LOCK_KEY, lock).map_err(err_into)?;

            txid
        }
    };

    Ok(LockAcquired::Success { txid, autocommit })
}

pub fn unregister(tree: &TxPartition, txid: u64) -> Result<()> {
    let key = get_txdata_key(txid);

    let mut tx_data: TxData = tree
        .get(&key)
        .map_err(err_into)?
        .ok_or_else(|| Error::StorageMsg("conflict - lock does not exist".to_owned()))
        .map(|tx_data| bincode::deserialize(&tx_data))?
        .map_err(err_into)?;

    tx_data.alive = false;

    let tx_data = bincode::serialize(&tx_data).map_err(err_into)?;
    tree.insert(key, tx_data).map_err(err_into)?;

    Ok(())
}

pub fn release(tree: &TxPartition, txid: u64) -> Result<(), Error> {
    // TODO: update_fetch
    let Lock {
        gc_txid, lock_txid, ..
    } = tree
        .get(LOCK_KEY)
        .map_err(err_into)?
        .map(|l| bincode::deserialize(&l))
        .transpose()
        .map_err(err_into)?
        .unwrap_or_default();

    if Some(txid) == lock_txid {
        let lock = Lock {
            lock_txid: None,
            lock_created_at: 0,
            gc_txid,
        };

        let lock = bincode::serialize(&lock).map_err(err_into)?;
        tree.insert(LOCK_KEY, lock).map_err(err_into)?;
    }

    let key = get_txdata_key(txid);
    let tx_data: Option<TxData> = tree
        .get(&key)
        .map_err(err_into)?
        .map(|tx_data| bincode::deserialize(&tx_data))
        .transpose()
        .map_err(err_into)?;

    let mut tx_data = match tx_data {
        Some(tx_data) => tx_data,
        None => {
            return Ok(());
        }
    };

    tx_data.alive = false;

    let tx_data = bincode::serialize(&tx_data).map_err(err_into)?;
    tree.insert(key, tx_data).map_err(err_into)?;

    Ok(())
}
