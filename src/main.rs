// Get own executions for the most recent day.
mod gmo;

use chrono::DateTime;
use dotenv::dotenv;
use gcp_bigquery_client::{
    model::{
        query_request::QueryRequest, table_data_insert_all_request::TableDataInsertAllRequest,
    },
    Client,
};
use gmo::{Execution, GmoClient, LatestExecutionsResponse};
use serde::Serialize;
use std::env;
use yup_oauth2::ServiceAccountKey;

const DATASET_ID: &str = "gmo";
const TABLE_ID: &str = "my_executions";

#[tokio::main]
async fn main() {
    dotenv().ok();

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

    // get execution from GMO
    let api_key = env::var("API_KEY").unwrap();
    let api_secret = env::var("API_SECRET").unwrap();
    let client = GmoClient::new(api_key, api_secret);

    let executions = &client
        .get_latest_executions(String::from("BTC"), None, None)
        .await;

    match executions {
        Ok(exec) => {
            if let Some(data) = &exec.data {
                if let Some(list) = &data.list {
                    for e in list {
                        if e.execution_id > latest_execution_id {
                            println!(
                                "Found new excution: id={}, timestamp={}",
                                e.execution_id, e.timestamp
                            );
                            ins_req.add_row(None, convert_my_executions(&e)).unwrap()
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("{:?}", e)
        }
    }

    // add new executions to table
    let row_num = ins_req.len();
    if row_num > 0 {
        let res = bq_client
            .tabledata()
            .insert_all(project_id, DATASET_ID, TABLE_ID, ins_req)
            .await;
        match res {
            Ok(_) => {
                println!("Suceeded to add new {} records.", row_num);
            }
            Err(e) => {
                println!("Failed to add new records => {:?}", e);
            }
        }
    } else {
        println!("There is no new record.");
    }
}

fn convert_my_executions(e: &Execution) -> MyExecutions {
    // convert date time format
    let d = DateTime::parse_from_rfc3339(&e.timestamp).unwrap();
    let timestamp = d.format("%Y-%m-%d %H:%M:%S").to_string();

    // return the execution as a MyExecutions
    MyExecutions {
        execution_id: e.execution_id,
        order_id: e.order_id,
        symbol: e.symbol.clone(),
        side: e.side.clone(),
        settle_type: e.settle_type.clone(),
        size: e.size.clone(),
        price: e.price.clone(),
        loss_gain: e.loss_gain.clone(),
        fee: e.fee.clone(),
        timestamp: timestamp,
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
