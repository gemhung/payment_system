use csv::StringRecord;
use std::borrow::Cow;
use std::time::Duration;
use tokio::time;
use tracing::error;
use tracing::info;

const API_URL: &str =
    "https://bhxrfzjyp5hetd4zj3zspdfszq.appsync-api.ap-northeast-1.amazonaws.com/graphql"; // poc for front-end
const API_KEY: &str = "da2-ccbkc76hrra2fkp7u7k4xtvid4"; // poc for front-end

mod schema {
    cynic::use_schema!("./schema.graphql");
}

#[allow(unused)]
#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(schema_path = "./schema.graphql", query_module = "schema")]
struct PocOhlc {
    code: String,
    dod: Option<f64>,
    high: Option<f64>,
    close: Option<f64>,
    low: Option<f64>,
    market: Option<String>,
    open: Option<f64>,
    step: Option<f64>,
    symbol: Option<String>,
    tick: Option<i32>,
    time: Option<String>,
    volume: Option<i32>,
}

#[derive(cynic::FragmentArguments, Clone, Default)]
struct Argument(UpdatePocOhlcInput);

#[derive(cynic::InputObject, Clone, Default)]
#[cynic(schema_path = "./schema.graphql", query_module = "schema")]
struct UpdatePocOhlcInput {
    symbol: Option<String>,
    time: Option<String>,
    step: Option<f64>,
    open: Option<f64>,
    volume: Option<i32>,
    market: Option<String>,
	last: Option<f64>,
    low: Option<f64>,
    code: String,
    close: Option<f64>,
    dod: Option<f64>,
    tick: Option<i32>,
    high: Option<f64>,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(
    schema_path = "./schema.graphql",
    schema_module = "schema",
    graphql_type = "Mutation",
    argument_struct = "Argument"
)]
struct UpdateOhlc {
    #[arguments(event=&args.0)]
    update_ohlc: Option<PocOhlc>,
}

fn to_vec(s: StringRecord) -> Vec<String> {
    s.into_iter().map(ToString::to_string).collect::<Vec<_>>()
}

async fn run_query<R: std::io::Read>(csv: csv::Reader<R>) -> Result<(), anyhow::Error> {
    use cynic::http::ReqwestExt;

    let mut interval = time::interval(Duration::from_millis(1000));
    let mut records = csv.into_records();
    loop {
        interval.tick().await;
        match records.next() {
            Some(Ok(row)) => {
                let v = to_vec(row);
                let query = build_query(v);
                let res = reqwest::Client::new()
                    .post(API_URL)
                    .header("X-Api-Key", API_KEY)
                    .run_graphql(query)
                    .await
                    .unwrap();

                match res.data {
                    Some(UpdateOhlc {
                        update_ohlc: Some(inner),
                    }) => {
                        info!("inner = {:?}", inner);
                    }
                    err => {
                        error!(?err);
                    }
                }
            }

            err => {
                error!(?err);
            }
        }
    }
}

fn to_market(m: &str) -> Cow<str> {
    match m {
        "1" => "Tokyo",
        "3" => "Nagoya",
        "6" => "Fukuoka",
        "8" => "Sapporo",
        _ => "unknown",
    }
    .into()
}

fn build_query(v: Vec<String>) -> cynic::Operation<'static, UpdateOhlc> {
    use cynic::MutationBuilder;

    let arr: [String; 10] = v.try_into().unwrap();
    match arr {
        [timestamp, symbol, market, open, high, low, close, volume, dod, _dod_percent] => {
            UpdateOhlc::build(Argument(UpdatePocOhlcInput {
                code: symbol.clone() + "." + &to_market(&market)[0..1],
                time: Some(timestamp),
                symbol: Some(symbol),
                market: Some(to_market(&market).into_owned()),
                last: close.parse().ok(), // same as close
                open: open.parse().ok(),
                high: high.parse().ok(),
                low: low.parse().ok(),
                close: close.parse().ok(),
                volume: volume.parse().ok(),
                dod: dod.parse().ok(),
                step: Some(1.0),
                tick: Some(1),
            }))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let file = std::fs::File::open("./mkt_data.csv")?;
    let rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(file);

    run_query(rdr).await?;

    Ok(())
}
