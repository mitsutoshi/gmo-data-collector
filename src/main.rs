mod bq;
mod gmo;
mod models;
mod subcommands;

use clap::{Arg, Command};
use dotenv::dotenv;

const COMMAND_STATUS: &str = "status";

#[tokio::main]
async fn main() {
    dotenv().ok();

    let app = Command::new("gmo")
        .subcommand(Command::new("my_executions"))
        .subcommand(Command::new("assets"))
        .subcommand(Command::new("get_executions_by_order").arg(Arg::new("path").required(true)))
        .subcommand(Command::new("average_price"))
        .subcommand(Command::new("ticker"))
        .subcommand(Command::new(COMMAND_STATUS));

    match app.get_matches().subcommand() {
        // Get latest executions and save them to the BigQuery.
        Some(("my_executions", _)) => {
            subcommands::get_my_executions().await;
        }
        // Get current assets from GMO-Coin and save them to the BigQuery.
        Some(("assets", _)) => {
            subcommands::get_assets().await;
        }
        // Get execution information of specifeid order IDs in csv file and save them to the
        // BigQuery.
        Some(("get_executions_by_order", args)) => {
            let path = args.get_one::<String>("path").unwrap();
            subcommands::get_executions_by_order(path.to_string()).await;
        }
        // Calculate average buy price of own position.
        Some(("average_price", _)) => {
            subcommands::get_avg_price().await;
        }
        Some(("ticker", _)) => {
            subcommands::get_ticker().await;
        }
        Some((COMMAND_STATUS, _)) => {
            subcommands::status().await;
        }
        _ => {
            println!("None");
        }
    }
}
