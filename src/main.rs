mod gmo;

use chrono::DateTime;
use dotenv::dotenv;
use gcp_bigquery_client::{
    model::{
        query_request::QueryRequest, table_data_insert_all_request::TableDataInsertAllRequest,
    },
    Client,
};
use gmo::{GmoClient, LatestExecutionsResponse};
use serde::Serialize;
use std::env;
use yup_oauth2::ServiceAccountKey;

const DATASET_ID: &str = "gmo";
const TABLE_ID: &str = "my_executions";

#[tokio::main]
async fn main() {
    dotenv().ok();

    // get execution from GMO
    let api_key = env::var("API_KEY").unwrap();
    let api_secret = env::var("API_SECRET").unwrap();
    let client = GmoClient::new(api_key, api_secret);
    let executions: LatestExecutionsResponse = client
        .get_latesst_executions(String::from("BTC"), Option::None, Option::None)
        .await
        .unwrap();

    // create BigQuery client
    let service_account_key = env::var("SERVICE_ACCOUNT_KEY").unwrap();
    let key: ServiceAccountKey = serde_json::from_str(&service_account_key).unwrap();
    let bq_client: Client = Client::from_service_account_key(key, false).await.unwrap();

    // select latest execution_id from BigQuery
    let project_id = &env::var("BQ_PROJECT_ID").unwrap();
    let query = format!(
        "select max(execution_id) as execution_id from {}.{}.{}",
        project_id, DATASET_ID, TABLE_ID
    );

    let mut rs = bq_client
        .job()
        .query(project_id, QueryRequest::new(query))
        .await
        .unwrap();

    let mut latest_execution_id: i64 = 0;
    if rs.next_row() {
        let execution_id = rs.get_i64_by_name("execution_id").unwrap();
        if let Some(v) = execution_id {
            latest_execution_id = v;
        }
    } else {
        println!("There are no past records.");
    }
    println!("Latest execution_id: {:?}", latest_execution_id);

    let mut ins_req: TableDataInsertAllRequest = TableDataInsertAllRequest::new();
    if let Some(data) = executions.data {
        for e in data.list {
            if e.execution_id > latest_execution_id {
                // convert date time format
                let d = DateTime::parse_from_rfc3339(&e.timestamp).unwrap();
                let timestamp = d.format("%Y-%m-%d %H:%M:%S").to_string();
                println!(
                    "Found new excution: id={}, timestamp={}",
                    e.execution_id, timestamp
                );

                // insert
                ins_req
                    .add_row(
                        None,
                        MyExecutions {
                            execution_id: e.execution_id,
                            order_id: e.order_id,
                            symbol: e.symbol,
                            side: e.side,
                            settle_type: e.settle_type,
                            size: e.size,
                            price: e.price,
                            loss_gain: e.loss_gain,
                            fee: e.fee,
                            timestamp: timestamp,
                        },
                    )
                    .unwrap()
            }
        }
    }

    // add new executions to table
    if ins_req.len() > 0 {
        let res = bq_client
            .tabledata()
            .insert_all(project_id, DATASET_ID, TABLE_ID, ins_req)
            .await;
        match res {
            Ok(r) => {
                println!("Suceeded to register new records => {:?}", r);
            }
            Err(e) => {
                println!("Failed to add new records => {:?}", e);
            }
        }
    } else {
        println!("There is no new record.");
    }
}

#[derive(Serialize)]
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
