mod dataframe;
mod get_data;
mod schedule;

use dataframe::live_data_to_df;
use get_data::get_data;
use polars::prelude::*;
use schedule::{GameInfo, fetch_game_schedule};
use std::collections::HashSet;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- Fetch schedule ---

    let games = fetch_game_schedule(1, "R", 2025)?;

    /* println!("Found {} games:", games.len());
    println!(
        "{:<10} {:<20} {:<12} {:<20} {:<8} {:<20} {:<8} {:<6} {:<8} {:<20} {:<6}",
        "Game ID",
        "Time",
        "Date",
        "Away",
        "Away ID",
        "Home",
        "Home ID",
        "State",
        "Venue ID",
        "Venue Name",
        "Type"
    );
    println!("{}", "-".repeat(140));

    for g in &games {

        println!(
            "{:<10} {:<20} {:<12} {:<20} {:<8} {:<20} {:<8} {:<6} {:<8} {:<20} {:<6}",
            g.game_id,
            g.time,
            g.date,
            g.away,
            g.away_id,
            g.home,
            g.home_id,
            g.state,
            g.venue_id,
            g.venue_name,
            g.gameday_type


        );


    }

    */

    // --- Fetch unique game IDs ---
    let mut game_ids_set = HashSet::new();
    for g in &games {
        game_ids_set.insert(g.game_id);
    }

    // --- Fetch live data ---
    // Convert back to Vec<u64> for get_data
    let unique_game_ids: Vec<u64> = game_ids_set.into_iter().collect();
    let test_game_ids = vec![745444]; // example gamePk
    let live_data = get_data(&test_game_ids)?;
    //let live_data = get_data(&unique_game_ids)?;

    println!("Retrieved {} live game feeds.", live_data.len());

    let mut df = live_data_to_df(&live_data)?;
    println!("{:?}", df);

    let mut file = std::fs::File::create("live_game_data_2.csv")?;
    CsvWriter::new(&mut file).finish(&mut df)?;

    Ok(())
}
