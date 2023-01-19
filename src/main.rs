// Get own executions for the most recent day.
mod gmo;

use chrono::{DateTime, Utc};
use clap::Command;
use dotenv::dotenv;
use gcp_bigquery_client::{
    model::{
        query_request::QueryRequest, table_data_insert_all_request::TableDataInsertAllRequest,
    },
    Client,
};
use gmo::{Execution, GmoClient};
use serde::Serialize;
use std::env;
use yup_oauth2::ServiceAccountKey;

const DATASET_ID: &str = "gmo";

#[tokio::main]
async fn main() {
    // read .env
    dotenv().ok();

    // create BigQuery client
    let service_account_key = env::var("SERVICE_ACCOUNT_KEY").unwrap();
    let key: ServiceAccountKey = serde_json::from_str(&service_account_key).unwrap();
    let bq_client: Client = Client::from_service_account_key(key, false).await.unwrap();

    // create GMO API client
    let api_key = env::var("API_KEY").unwrap();
    let api_secret = env::var("API_SECRET").unwrap();
    let client = GmoClient::new(api_key, api_secret);

    // create sub commands
    let sub_myexec = Command::new("my_executions");
    let sub_balance = Command::new("assets");
    let app = Command::new("gmo")
        .subcommand(sub_myexec)
        .subcommand(sub_balance);

    match app.get_matches().subcommand() {
        Some(("my_executions", _)) => {
            get_my_executions(&client, &bq_client).await;
        }
        Some(("assets", _)) => {
            get_assets(&client, &bq_client).await;
        }
        _ => {
            println!("None");
        }
    }
}

async fn get_my_executions(gmo: &GmoClient, bq_client: &Client) {
    // select latest execution_id from BigQuery
    let project_id = &env::var("BQ_PROJECT_ID").unwrap();
    let table_id = "my_executions";
    let query = format!(
        "select max(execution_id) as execution_id from {}.{}.{}",
        project_id, DATASET_ID, table_id
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

    let executions = &gmo
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

    insert_bq(&bq_client, ins_req, project_id, table_id).await;
}

async fn get_assets(gmo: &GmoClient, bq_client: &Client) {
    let mut ins_req: TableDataInsertAllRequest = TableDataInsertAllRequest::new();

    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

    // get baalnce from GMO
    let assets = gmo.get_assets().await.unwrap();
    match assets.data {
        Some(data) => {
            for d in data {
                let a = Assets {
                    timestamp: timestamp.clone(),
                    amount: d.amount.parse::<f64>().unwrap(),
                    available: d.available.parse::<f64>().unwrap(),
                    symbol: d.symbol,
                };
                println!("Assets: {} {}", a.amount, a.symbol);
                ins_req.add_row(None, a).unwrap()
            }
        }
        _ => {}
    }

    let project_id = &env::var("BQ_PROJECT_ID").unwrap();
    insert_bq(&bq_client, ins_req, project_id, "assets").await;
}

async fn insert_bq(
    bq_client: &Client,
    ins_req: TableDataInsertAllRequest,
    project_id: &str,
    table_id: &str,
) {
    // add new executions to table
    let row_num = ins_req.len();
    if row_num > 0 {
        let res = bq_client
            .tabledata()
            .insert_all(project_id, DATASET_ID, table_id, ins_req)
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
