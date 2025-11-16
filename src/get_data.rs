use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use reqwest::blocking;
use serde_json::Value;
use std::error::Error;

pub fn get_data(game_list_input: &[u64]) -> Result<Vec<Value>, Box<dyn Error>> {
    println!("This May Take a While. Progress Bar shows Completion of Data Retrieval.");

    let pb = ProgressBar::new(game_list_input.len() as u64);
    let style = ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")?
        .progress_chars("#>-");

    pb.set_style(style);

    let data_total: Vec<_> = game_list_input
        .par_iter()
        .map(|&game_id| {
            let url = format!(
                "https://statsapi.mlb.com/api/v1.1/game/{}/feed/live",
                game_id
            );
            let resp = blocking::get(&url)
                .and_then(|r| r.json::<Value>())
                .unwrap_or(Value::Null);

            pb.inc(1);
            resp
        })
        .collect();

    pb.finish_with_message("Data retrieval complete.");

    Ok(data_total)
}
