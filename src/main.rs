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

mod bq;
mod gmo;
mod models;
mod subcommands;
use gmo::{Execution, GmoClient};
use models::{MyExecutions, Positions};

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
    let client = GmoClient::new(
        env::var("API_KEY").unwrap(),
        env::var("API_SECRET").unwrap(),
    );

    // create sub commands
    let sub_myexec = Command::new("my_executions");
    let sub_balance = Command::new("assets");
    let sub_get_exec_by_order =
        Command::new("get_executions_by_order").arg(Arg::new("path").required(true));
    let sub_avg_price = Command::new("average_price");

    let app = Command::new("gmo")
        .subcommand(sub_myexec)
        .subcommand(sub_balance)
        .subcommand(sub_get_exec_by_order)
        .subcommand(sub_avg_price);

    match app.get_matches().subcommand() {
        Some(("my_executions", _)) => {
            subcommands::get_my_executions(&bq_client).await;
        }
        Some(("assets", _)) => {
            subcommands::get_assets().await;
        }
        Some(("get_executions_by_order", args)) => {
            let path = args.get_one::<String>("path").unwrap();
            subcommands::get_executions_by_order(path.to_string()).await;
        }
        Some(("average_price", _)) => {
            get_avg_price(&client, &bq_client).await;
        }
        _ => {
            println!("None");
        }
    }
}

//async fn get_my_executions(gmo: &GmoClient, bq_client: &Client) {
//    // select latest execution_id from BigQuery
//    let project_id = &env::var("BQ_PROJECT_ID").unwrap();
//    let table_id = "my_executions";
//    let query = format!(
//        "select max(execution_id) as execution_id from {}.{}.{}",
//        project_id, DATASET_ID, table_id
//    );
//
//    let mut rs = bq_client
//        .job()
//        .query(project_id, QueryRequest::new(query))
//        .await
//        .unwrap();
//
//    let mut latest_execution_id: i64 = 0;
//    if rs.next_row() {
//        let execution_id = rs.get_i64_by_name("execution_id").unwrap();
//        if let Some(v) = execution_id {
//            latest_execution_id = v;
//        }
//    } else {
//        println!("There are no past records.");
//    }
//    println!("Latest execution_id: {:?}", latest_execution_id);
//
//    let mut ins_req: TableDataInsertAllRequest = TableDataInsertAllRequest::new();
//
//    let executions = &gmo
//        .get_latest_executions(String::from("BTC"), None, None)
//        .await;
//
//    match executions {
//        Ok(exec) => {
//            if let Some(data) = &exec.data {
//                if let Some(list) = &data.list {
//                    let mut pos = 0.35641193;
//                    let mut pos_price = 1044768.46741743;
//                    let mut avg_buy_price = 2931351.0;
//                    for e in list {
//                        if e.execution_id > latest_execution_id {
//                            let size = e.size.parse::<f64>().unwrap();
//                            let price = e.price.parse::<f64>().unwrap();
//                            if e.side == "BUY" {
//                                pos += size;
//                                pos_price += size * price;
//                                avg_buy_price = pos_price / pos
//                            } else {
//                                pos -= e.size.parse::<f64>().unwrap();
//                                pos_price -= avg_buy_price * size;
//                            };
//
//                            println!(
//                            "Found new excution: id={}, timestamp={}, side={}, price={}, size={}, avg_buy_price={}",
//                            e.execution_id, e.timestamp, e.side, e.price, e.size, avg_buy_price
//                        );
//                            ins_req.add_row(None, convert_my_executions(&e)).unwrap()
//                        }
//                    }
//                }
//            }
//        }
//        Err(e) => {
//            println!("{:?}", e)
//        }
//    }
//
//    insert_bq(&bq_client, ins_req, table_id).await;
//}

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

async fn get_avg_price(gmo: &GmoClient, bq_client: &Client) {
    let project_id = &env::var("BQ_PROJECT_ID").unwrap();

    // get the average price saved last time
    let query = format!(
        "
        select
          execution_id,
          size,
          average_price
        from
        (
          select *
          from {}.{}.positions
          order by
            execution_id desc
        )
        limit 1",
        project_id, DATASET_ID
    );
    let mut rs = bq_client
        .job()
        .query(project_id, QueryRequest::new(query))
        .await
        .unwrap();

    let mut latest_pos_exec_id: i64 = 0;
    let mut cum_size: f64 = 0.0;
    let mut avg_price: f64 = 0.0;

    if rs.next_row() {
        latest_pos_exec_id = rs.get_i64_by_name("execution_id").unwrap().unwrap();
        cum_size = rs.get_f64_by_name("size").unwrap().unwrap();
        avg_price = rs.get_f64_by_name("average_price").unwrap().unwrap();
    }
    println!("latest execution_id in positions: {}", latest_pos_exec_id);

    // get executions records which the average price has not yet been calculated.
    let query = format!(
        "select * from {}.{}.my_executions where execution_id > {} order by timestamp",
        project_id, DATASET_ID, latest_pos_exec_id
    );
    let mut rs = bq_client
        .job()
        .query(project_id, QueryRequest::new(query))
        .await
        .unwrap();

    let mut ins_req: TableDataInsertAllRequest = TableDataInsertAllRequest::new();

    while rs.next_row() {
        let ts = rs.get_string_by_name("timestamp").unwrap().unwrap();
        let exec_id = rs.get_i64_by_name("execution_id").unwrap().unwrap();
        let size = rs.get_f64_by_name("size").unwrap().unwrap();
        let side = rs.get_string_by_name("side").unwrap().unwrap();

        if side == "BUY".to_string() {
            let price = rs.get_f64_by_name("price").unwrap().unwrap();
            avg_price = (price * size + avg_price * cum_size) / (size + cum_size);
            cum_size += size;
        } else {
            cum_size -= size
        }

        let s = format!("{:.8}", cum_size).parse::<f64>().unwrap();
        println!("{}, {}({}), {:.8}, {:.0}", ts, exec_id, side, s, avg_price);
        ins_req
            .add_row(
                None,
                Positions {
                    timestamp: ts,
                    execution_id: exec_id,
                    average_price: avg_price,
                    size: s,
                },
            )
            .unwrap()
    }
    insert_bq(bq_client, ins_req, "positions").await;
}
