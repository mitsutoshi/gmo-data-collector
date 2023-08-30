use crate::{
    bq::insert_bq,
    gmo::{Execution, GmoClient},
    models::{Assets, MyExecutions, Positions},
};
use chrono::{DateTime, Utc};
use gcp_bigquery_client::{
    model::{
        query_request::QueryRequest, table_data_insert_all_request::TableDataInsertAllRequest,
    },
    Client,
};
use std::env;

const DATASET_ID: &str = "gmo";

pub async fn get_assets() {
    // create GMO API client
    let gmo = GmoClient::new(
        env::var("API_KEY").unwrap(),
        env::var("API_SECRET").unwrap(),
    );
    // get balance from GMO
    let assets = gmo.get_assets().await.unwrap();

    let mut ins_req = TableDataInsertAllRequest::new();
    match assets.data {
        Some(data) => {
            let ts = Utc::now().format("%Y-%m-%d %H:%M:%S");
            for d in data {
                match &*d.symbol {
                    "JPY" | "BTC" => {
                        let a = Assets {
                            timestamp: ts.to_string(),
                            amount: d.amount.parse::<f64>().unwrap(),
                            available: d.available.parse::<f64>().unwrap(),
                            symbol: d.symbol,
                        };
                        println!("Assets: {} {}", a.amount, a.symbol);
                        ins_req.add_row(None, a).unwrap()
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }

    insert_bq(ins_req, DATASET_ID, "assets").await;
}

// Get latest executions within 24 hours and save them into BigQuery.
pub async fn get_my_executions(bq_client: &Client) {
    // select latest execution_id from BigQuery
    let project_id = &env::var("BQ_PROJECT_ID").unwrap();
    let table_id = "my_executions";
    let query = format!(
        "select max(execution_id) as execution_id from {}.{}.{}",
        project_id, DATASET_ID, table_id
    );

    // Search latest execution from BigQuery
    let mut rs = bq_client
        .job()
        .query(project_id, QueryRequest::new(query))
        .await
        .unwrap();

    // get the latest execution_id stored in BigQuery
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

    // create GMO API client
    let gmo = GmoClient::new(
        env::var("API_KEY").unwrap(),
        env::var("API_SECRET").unwrap(),
    );

    // get latest executions by GMO api (within 24 hours)
    let executions = &gmo
        .get_latest_executions(String::from("BTC"), None, None)
        .await;

    match executions {
        Ok(exec) => {
            if let Some(data) = &exec.data {
                if let Some(list) = &data.list {
                    // initial values
                    let mut pos = 0.35641193;
                    let mut pos_price = 1044768.46741743;
                    let mut avg_buy_price = 2931351.0;

                    for e in list {
                        if e.execution_id > latest_execution_id {
                            let size = e.size.parse::<f64>().unwrap();
                            let price = e.price.parse::<f64>().unwrap();
                            if e.side == "BUY" {
                                pos += size;
                                pos_price += size * price;
                                avg_buy_price = pos_price / pos
                            } else {
                                pos -= e.size.parse::<f64>().unwrap();
                                pos_price -= avg_buy_price * size;
                            };

                            println!(
                            "Found new excution: id={}, timestamp={}, side={}, price={}, size={}, avg_buy_price={}",
                            e.execution_id, e.timestamp, e.side, e.price, e.size, avg_buy_price
                        );
                            //ins_req.add_row(None, convert_my_executions(&e)).unwrap()
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("{:?}", e)
        }
    }

    //insert_bq(ins_req, DATASET_ID, table_id).await;
}

pub async fn get_executions_by_order(order_id_csv_file_path: String) {
    // create GMO API client
    let gmo = GmoClient::new(
        env::var("API_KEY").unwrap(),
        env::var("API_SECRET").unwrap(),
    );

    let mut ins_req: TableDataInsertAllRequest = TableDataInsertAllRequest::new();
    let mut reader = csv::Reader::from_path(order_id_csv_file_path).unwrap();
    for rec in reader.records() {
        if let Ok(r) = rec {
            let order_id = r.get(0).unwrap().to_string();
            println!("order_id: {:?}", order_id);

            //  get execution by order_id from GMO
            let executions = gmo.get_executions(Some(order_id), None).await.unwrap();

            if let Some(data) = executions.data {
                if let Some(list) = data.list {
                    for e in list {
                        println!("  execution: {:?}", e.execution_id);
                        ins_req.add_row(None, convert_my_executions(&e)).unwrap();
                    }
                }
            }
            std::thread::sleep_ms(1000);
        }
    }

    insert_bq(ins_req, DATASET_ID, "my_executions").await;
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
        size: e.size.parse::<f64>().unwrap(),
        price: e.price.parse::<f64>().unwrap(),
        loss_gain: e.loss_gain.parse::<f64>().unwrap(),
        fee: e.fee.parse::<f64>().unwrap(),
        timestamp: timestamp,
    }
}
