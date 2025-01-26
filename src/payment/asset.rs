use super::engine::ClientID;
use super::engine::TransactionID;

#[derive(Clone, Debug)]
pub struct AssetBook(std::collections::HashMap<ClientID, Asset>);

impl std::ops::Deref for AssetBook {
    type Target = std::collections::HashMap<ClientID, Asset>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for AssetBook {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AssetBook {
    pub fn new() -> Self {
        AssetBook(std::collections::HashMap::<ClientID, Asset>::new())
    }
    pub fn iter(&self) -> std::collections::hash_map::Iter<ClientID, Asset> {
        self.0.iter()
    }
    pub fn print_to_stdout(&self) {
        self.iter().for_each(|(client_id, asset)| {
            // As intructions required, we output a precision of up to four places past the decimal
            let total = format!("{:.4}", asset.total);
            let available = format!("{:.4}", asset.available);
            let hold = format!("{:.4}", asset.hold);
            let locked = asset.is_locked.is_some();
            println!("{},{},{},{},{}", client_id, available, hold, total, locked);
        })
    }
}

#[derive(Debug, Clone)]
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
