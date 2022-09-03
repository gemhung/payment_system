use super::engine::ClientID;
use super::engine::TransactionID;

pub type AssetBook = std::collections::HashMap<ClientID, Asset>;

#[derive(Debug, Clone, Default)]
pub struct Asset {
    pub total: f64,
    pub available: f64,
    pub hold: f64,
    pub is_locked: Option<TransactionID>,
}

impl Asset {
    pub fn new() -> Self {
        Self {
            total: 0.0,
            available: 0.0,
            hold: 0.0,
            is_locked: None,
        }
    }
}
