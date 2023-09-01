use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct MyExecutions {
    pub execution_id: i64,
    pub order_id: i64,
    pub symbol: String,
    pub side: String,
    pub settle_type: String,
    pub size: f64,
    pub price: f64,
    pub loss_gain: f64,
    pub fee: f64,
    pub timestamp: String,
}

#[derive(Serialize, Debug)]
pub struct Assets {
    pub timestamp: String,
    pub symbol: String,
    pub amount: f64,
    pub available: f64,
}

#[derive(Serialize, Debug)]
pub struct Positions {
    pub timestamp: String,
    pub execution_id: i64,
    pub average_price: f64,
    pub size: f64,
}

#[derive(Serialize, Debug)]
pub struct Ticker {
    pub timestamp: String,
    pub symbol: String,
    pub last: f64,
}
