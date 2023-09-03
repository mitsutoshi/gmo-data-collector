use {
    chrono::Utc,
    hex::encode,
    reqwest::{
        header::{HeaderMap, HeaderValue},
        Client,
    },
    ring::hmac::{sign, Key, HMAC_SHA256},
    serde::{Deserialize, Serialize},
    serde_json::Value,
};

const PUBLIC_API_URL: &str = "https://api.coin.z.com/public";
const PRIVATE_API_URL: &str = "https://api.coin.z.com/private";

#[derive(Debug)]
pub struct GmoClient {
    api_key: String,
    api_secret: String,
    client: Client,
}

#[derive(Debug, Deserialize)]
pub struct StatusResponse {
    pub status: u8,
    pub data: Option<StatusData>,
    pub responsetime: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct StatusData {
    pub status: Status,
}

#[derive(Debug, Deserialize)]
pub enum Status {
    MAINTENANCE,
    REOPEN,
    OPEN,
}

#[derive(Debug, Deserialize)]
pub struct GmoResponse {
    pub status: u8,
    pub responsetime: String,
    pub data: Option<Value>,
    pub messages: Option<ErrorMessages>,
}

#[derive(Debug, Deserialize)]
pub struct ErrorMessage {
    pub message_code: String,
    pub message_string: String,
}

type ErrorMessages = Vec<ErrorMessage>;

#[derive(Debug, Deserialize)]
pub struct AssetesResponse {
    pub status: u8,
    pub responsetime: String,
    pub data: Option<Vec<Asset>>,
    pub messages: Option<ErrorMessages>,
}

#[derive(Debug, Deserialize)]
pub struct Asset {
    pub amount: String,
    pub available: String,
    #[serde(rename(deserialize = "conversionRate"))]
    pub conversion_rate: String,
    pub symbol: String,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionsResponse {
    pub status: u8,
    pub responsetime: String,
    pub data: Option<ExecutionData>,
    pub messages: Option<ErrorMessages>,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionData {
    pub list: Option<Vec<Execution>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Execution {
    #[serde(rename(deserialize = "executionId"))]
    pub execution_id: i64,
    #[serde(rename(deserialize = "orderId"))]
    pub order_id: i64,
    pub symbol: String,
    pub side: String,
    #[serde(rename(deserialize = "settleType"))]
    pub settle_type: String,
    pub size: String,
    pub price: String,
    #[serde(rename(deserialize = "lossGain"))]
    pub loss_gain: String,
    pub fee: String,
    pub timestamp: String,
}

#[derive(Debug, Deserialize)]
pub struct LatestExecutionsResponse {
    pub status: u8,
    pub responsetime: Option<String>,
    pub pagination: Option<Pagination>,
    pub data: Option<ExecutionData>,
    pub messages: Option<ErrorMessages>,
}

#[derive(Debug, Deserialize)]
pub struct Pagination {
    #[serde(rename(deserialize = "currentPage"))]
    pub current_page: i64,
    pub count: i64,
}

#[derive(Debug, Deserialize)]
pub struct TickerResponse {
    pub status: u8,
    pub data: Vec<TickerData>,
    pub responsetime: String,
}

#[derive(Debug, Deserialize)]
pub struct TickerData {
    pub ask: String,
    pub bid: String,
    pub high: String,
    pub low: String,
    pub last: String,
    pub symbol: String,
    pub timestamp: String,
    pub volume: String,
}

impl GmoClient {
    pub fn new(api_key: String, api_secret: String) -> Self {
        GmoClient {
            api_key: api_key,
            api_secret: api_secret,
            client: Client::builder().build().unwrap(),
        }
    }

    #[allow(dead_code)]
    pub async fn status(&self) -> Result<StatusResponse, reqwest::Error> {
        let path = "/v1/status";
        let res = self
            .client
            .get(format!("{}{}", PUBLIC_API_URL, path))
            .send()
            .await?
            .json::<StatusResponse>()
            .await
            .unwrap();
        Ok(res)
    }

    #[allow(dead_code)]
    pub async fn get_ticker(
        &self,
        symbol: Option<String>,
    ) -> Result<TickerResponse, reqwest::Error> {
        let path = "/v1/ticker";
        let mut query = vec![];
        if let Some(v) = symbol {
            query.push(("symbol", v));
        }
        let res = self
            .client
            .get(format!("{}{}", PUBLIC_API_URL, path))
            .query(&query)
            .send()
            .await?
            .json::<TickerResponse>()
            .await
            .unwrap();
        Ok(res)
    }

    // private api: /v1/account/assets
    #[allow(dead_code)]
    pub async fn get_assets(&self) -> Result<AssetesResponse, reqwest::Error> {
        let path = "/v1/account/assets";
        let res = self
            .client
            .get(format!("{}{}", PRIVATE_API_URL, path))
            .headers(self.create_auth_headers("GET", path, Some("")))
            .send()
            .await?
            .json::<AssetesResponse>()
            .await
            .unwrap();
        Ok(res)
    }

    // private api: /v1/executions
    #[allow(dead_code)]
    pub async fn get_executions(
        &self,
        order_id: Option<String>,
        execution_id: Option<String>,
    ) -> Result<ExecutionsResponse, reqwest::Error> {
        let mut query = vec![];
        if let Some(v) = order_id {
            query.push(("orderId", v));
        }
        if let Some(v) = execution_id {
            query.push(("executionId", v));
        }
        let path = "/v1/executions";
        let res = self
            .client
            .get(format!("{}{}", PRIVATE_API_URL, path))
            .query(&query)
            .headers(self.create_auth_headers("GET", path, Some("")))
            .send()
            .await?
            .json::<ExecutionsResponse>()
            .await
            .unwrap();
        Ok(res)
    }

    pub async fn get_latest_executions(
        &self,
        symbol: String,
        page: Option<i64>,
        count: Option<i64>,
    ) -> Result<LatestExecutionsResponse, reqwest::Error> {
        // create query string
        let mut query = vec![];
        query.push(("symbol", symbol));
        if let Some(v) = page {
            query.push(("page", v.to_string()));
        }
        if let Some(v) = count {
            query.push(("count", v.to_string()));
        }

        let path = "/v1/latestExecutions";
        let res = self
            .client
            .get(format!("{}{}", PRIVATE_API_URL, path))
            .query(&query)
            .headers(self.create_auth_headers("GET", path, Some("")))
            .send()
            .await?;
        println!("Response: {:?}", res);
        let res = res.json::<LatestExecutionsResponse>().await.unwrap();
        Ok(res)
    }

    fn create_auth_headers(&self, method: &str, path: &str, data: Option<&str>) -> HeaderMap {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let text = format!("{}{}{}{}", timestamp, method, path, data.unwrap());
        let key = Key::new(HMAC_SHA256, self.api_secret.as_bytes());
        let sign = &encode(sign(&key, text.as_bytes()).as_ref());
        let mut headers = HeaderMap::new();
        headers.insert("API-KEY", HeaderValue::from_str(&self.api_key).unwrap());
        headers.insert("API-TIMESTAMP", HeaderValue::from_str(&timestamp).unwrap());
        headers.insert("API-SIGN", HeaderValue::from_str(sign).unwrap());
        headers
    }
}
