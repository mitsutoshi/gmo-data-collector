use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct MyExecutions {
    pub execution_id: i64,
    pub order_id: i64,
    pub symbol: String,
    pub side: String,
    pub settle_type: String,
    pub size: String,
    pub price: String,
    pub loss_gain: String,
    pub fee: String,
    pub timestamp: String,
}

#[derive(Serialize, Debug)]
pub struct Assets {
    pub timestamp: String,
    pub symbol: String,
    pub amount: f64,
    pub available: f64,
}
