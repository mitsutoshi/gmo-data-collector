// Get own executions for the most recent day.
use chrono::{DateTime, Utc};
use clap::{Arg, Command};
use dotenv::dotenv;
use gcp_bigquery_client::{
    model::{
        query_request::QueryRequest, table_data_insert_all_request::TableDataInsertAllRequest,
    },
    Client,
};
use std::env;
use yup_oauth2::ServiceAccountKey;

mod gmo;
mod models;
use gmo::{Execution, GmoClient};
use models::{Assets, MyExecutions};

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
    let sub_get_exec_by_order =
        Command::new("get_executions_by_order").arg(Arg::new("path").required(true));

    let app = Command::new("gmo")
        .subcommand(sub_myexec)
        .subcommand(sub_balance)
        .subcommand(sub_get_exec_by_order);

    match app.get_matches().subcommand() {
        Some(("my_executions", _)) => {
            get_my_executions(&client, &bq_client).await;
        }
        Some(("assets", _)) => {
            get_assets(&client, &bq_client).await;
        }
        Some(("get_executions_by_order", args)) => {
            let path = args.get_one::<String>("path").unwrap();
            get_executions_by_order(&client, &bq_client, path.to_string()).await;
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
                    let mut pos = 0.35641193;
                    let mut pos_price = 1044768.46741743;
                    let mut avg_buy_price = 2931351.0;
                    for e in list {
                        //if e.execution_id > latest_execution_id {
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
                        ins_req.add_row(None, convert_my_executions(&e)).unwrap()
                    }
                }
            }
        }
        Err(e) => {
            println!("{:?}", e)
        }
    }

    insert_bq(&bq_client, ins_req, table_id).await;
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

    insert_bq(&bq_client, ins_req, "assets").await;
}

async fn insert_bq(bq_client: &Client, ins_req: TableDataInsertAllRequest, table_id: &str) {
    // add new executions to table
    let row_num = ins_req.len();
    if row_num > 0 {
        let project_id = &env::var("BQ_PROJECT_ID").unwrap();
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

async fn get_executions_by_order(
    gmo: &GmoClient,
    bq_client: &Client,
    order_id_csv_file_path: String,
) {
    let mut ins_req: TableDataInsertAllRequest = TableDataInsertAllRequest::new();

    let mut reader = csv::Reader::from_path(order_id_csv_file_path).unwrap();

    for rec in reader.records() {
        if let Ok(r) = rec {
            let order_id = r.get(0).unwrap().to_string();
            println!("order_id: {:?}", order_id);
            let s = Some(order_id);

            //  get execution by order_id from GMO
            let executions = gmo.get_executions(s, None).await.unwrap();

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

    insert_bq(bq_client, ins_req, "my_executions").await;
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
