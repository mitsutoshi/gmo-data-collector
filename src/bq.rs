use gcp_bigquery_client::{
    model::table_data_insert_all_request::TableDataInsertAllRequest, Client,
};
use std::env;

// create BigQuery client
pub async fn create_bq_client() -> Client {
    let key_str = env::var("SERVICE_ACCOUNT_KEY").unwrap();
    let key = serde_json::from_str(&key_str).unwrap();
    Client::from_service_account_key(key, false).await.unwrap()
}

pub async fn insert_bq(ins_req: TableDataInsertAllRequest, dataset_id: &str, table_id: &str) {
    // create BigQuery client
    let bq_client = create_bq_client().await;

    // add new executions to table
    let row_num = ins_req.len();
    if row_num > 0 {
        let project_id = &env::var("BQ_PROJECT_ID").unwrap();
        let res = bq_client
            .tabledata()
            .insert_all(project_id, dataset_id, table_id, ins_req)
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
