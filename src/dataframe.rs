use polars::prelude::*;
use serde_json::Value;
use std::error::Error;

pub fn live_data_to_df(live_data: &[Value]) -> Result<DataFrame, Box<dyn Error>> {
    println!("Converting live data to DataFrame...");

    // Define vectors for all the columns
    let mut game_id: Vec<Option<u64>> = Vec::new();
    let mut game_date: Vec<Option<String>> = Vec::new();
    let mut batter_id: Vec<Option<u64>> = Vec::new();
    let mut batter_name: Vec<Option<String>> = Vec::new();
    let mut batter_hand: Vec<Option<String>> = Vec::new();
    let mut batter_team: Vec<Option<String>> = Vec::new();
    let mut batter_team_id: Vec<Option<u64>> = Vec::new();
    let mut pitcher_id: Vec<Option<u64>> = Vec::new();
    let mut pitcher_name: Vec<Option<String>> = Vec::new();
    let mut pitcher_hand: Vec<Option<String>> = Vec::new();
    let mut pitcher_team: Vec<Option<String>> = Vec::new();
    let mut pitcher_team_id: Vec<Option<u64>> = Vec::new();
    let mut ab_number: Vec<Option<u32>> = Vec::new();
    let mut inning: Vec<Option<u32>> = Vec::new();
    let mut play_description: Vec<Option<String>> = Vec::new();
    let mut play_code: Vec<Option<String>> = Vec::new();
    let mut in_play: Vec<Option<bool>> = Vec::new();
    let mut is_strike: Vec<Option<bool>> = Vec::new();
    let mut is_swing: Vec<Option<bool>> = Vec::new();
    let mut is_whiff: Vec<Option<bool>> = Vec::new();
    let mut is_out: Vec<Option<bool>> = Vec::new();
    let mut is_ball: Vec<Option<bool>> = Vec::new();
    let mut is_review: Vec<Option<bool>> = Vec::new();
    let mut pitch_type: Vec<Option<String>> = Vec::new();
    let mut pitch_description: Vec<Option<String>> = Vec::new();
    let mut strikes: Vec<Option<u32>> = Vec::new();
    let mut balls: Vec<Option<u32>> = Vec::new();
    let mut outs: Vec<Option<u32>> = Vec::new();
    let mut strikes_after: Vec<Option<u32>> = Vec::new();
    let mut balls_after: Vec<Option<u32>> = Vec::new();
    let mut outs_after: Vec<Option<u32>> = Vec::new();

    // You can continue adding all other pitch / hit columns similarly...

    let swing_list = [
        "X", "F", "S", "D", "E", "T", "W", "L", "M", "Q", "Z", "R", "O", "J",
    ];
    let whiff_list = ["S", "T", "W", "M", "Q", "O"];

    for game in live_data {
    if let Some(all_plays) = game
        .get("liveData")
        .and_then(|d| d.get("plays"))
        .and_then(|p| p.get("allPlays"))
        .and_then(|ap| ap.as_array())
    {
        for play in all_plays {
            // extract per-play info
            let game_id_val = game.get("gamePk").and_then(|v| v.as_u64());
            let game_date_val = game
                .get("gameData")
                .and_then(|d| d.get("datetime"))
                .and_then(|dt| dt.get("officialDate"))
                .and_then(|s| s.as_str())
                .map(|s| s.to_string());

            let (batter_id_val, batter_name_val, batter_hand_val, pitcher_id_val, pitcher_name_val, pitcher_hand_val) =
                if let Some(matchup) = play.get("matchup") {
                    (
                        matchup.get("batter").and_then(|b| b.get("id")).and_then(|v| v.as_u64()),
                        matchup.get("batter").and_then(|b| b.get("fullName")).and_then(|s| s.as_str()).map(|s| s.to_string()),
                        matchup.get("batSide").and_then(|b| b.get("code")).and_then(|s| s.as_str()).map(|s| s.to_string()),
                        matchup.get("pitcher").and_then(|p| p.get("id")).and_then(|v| v.as_u64()),
                        matchup.get("pitcher").and_then(|p| p.get("fullName")).and_then(|s| s.as_str()).map(|s| s.to_string()),
                        matchup.get("pitchHand").and_then(|h| h.get("code")).and_then(|s| s.as_str()).map(|s| s.to_string()),
                    )
                } else {
                    (None, None, None, None, None, None)
                };

            // Determine teams and at-bat info
            let (batter_team_val, batter_team_id_val, pitcher_team_val, pitcher_team_id_val, ab_number_val, inning_val) =
                if let Some(about) = play.get("about") {
                    if about.get("isTopInning").and_then(|v| v.as_bool()).unwrap_or(true) {
                        (
                            game.get("gameData").and_then(|d| d.get("teams")).and_then(|t| t.get("away")).and_then(|team| team.get("abbreviation")).and_then(|s| s.as_str()).map(|s| s.to_string()),
                            game.get("gameData").and_then(|d| d.get("teams")).and_then(|t| t.get("away")).and_then(|team| team.get("id")).and_then(|v| v.as_u64()),
                            game.get("gameData").and_then(|d| d.get("teams")).and_then(|t| t.get("home")).and_then(|team| team.get("abbreviation")).and_then(|s| s.as_str()).map(|s| s.to_string()),
                            game.get("gameData").and_then(|d| d.get("teams")).and_then(|t| t.get("home")).and_then(|team| team.get("id")).and_then(|v| v.as_u64()),
                            about.get("atBatIndex").and_then(|v| v.as_u64()).map(|v| v as u32),
                            about.get("inning").and_then(|v| v.as_u64()).map(|v| v as u32),
                        )
                    } else {
                        (
                            game.get("gameData").and_then(|d| d.get("teams")).and_then(|t| t.get("home")).and_then(|team| team.get("abbreviation")).and_then(|s| s.as_str()).map(|s| s.to_string()),
                            game.get("gameData").and_then(|d| d.get("teams")).and_then(|t| t.get("home")).and_then(|team| team.get("id")).and_then(|v| v.as_u64()),
                            game.get("gameData").and_then(|d| d.get("teams")).and_then(|t| t.get("away")).and_then(|team| team.get("abbreviation")).and_then(|s| s.as_str()).map(|s| s.to_string()),
                            game.get("gameData").and_then(|d| d.get("teams")).and_then(|t| t.get("away")).and_then(|team| team.get("id")).and_then(|v| v.as_u64()),
                            about.get("atBatIndex").and_then(|v| v.as_u64()).map(|v| v as u32),
                            about.get("inning").and_then(|v| v.as_u64()).map(|v| v as u32),
                        )
                    }
                } else {
                    (None, None, None, None, None, None)
                };

            // Extract playEvents
            if let Some(events) = play.get("playEvents").and_then(|e| e.as_array()) {
                for event in events {
                    let details = event.get("details");

                    // push every column **once per event**
                    game_id.push(game_id_val);
                    game_date.push(game_date_val.clone());
                    batter_id.push(batter_id_val);
                    batter_name.push(batter_name_val.clone());
                    batter_hand.push(batter_hand_val.clone());
                    batter_team.push(batter_team_val.clone());
                    batter_team_id.push(batter_team_id_val);
                    pitcher_id.push(pitcher_id_val);
                    pitcher_name.push(pitcher_name_val.clone());
                    pitcher_hand.push(pitcher_hand_val.clone());
                    pitcher_team.push(pitcher_team_val.clone());
                    pitcher_team_id.push(pitcher_team_id_val);
                    ab_number.push(ab_number_val);
                    inning.push(inning_val);

                    play_description.push(
                        details.and_then(|d| d.get("description")).and_then(|s| s.as_str()).map(|s| s.to_string())
                    );
                    play_code.push(
                        details.and_then(|d| d.get("code")).and_then(|s| s.as_str()).map(|s| s.to_string())
                    );
                    in_play.push(details.and_then(|d| d.get("isInPlay")).and_then(|v| v.as_bool()));
                    is_strike.push(details.and_then(|d| d.get("isStrike")).and_then(|v| v.as_bool()));

                    // You can keep your swing/whiff logic here
                    is_swing.push(details.and_then(|d| d.get("code")).and_then(|c| c.as_str()).map(|s| swing_list.contains(&s)));
                    is_whiff.push(details.and_then(|d| d.get("code")).and_then(|c| c.as_str()).map(|s| whiff_list.contains(&s)));

                    is_out.push(details.and_then(|d| d.get("isOut")).and_then(|v| v.as_bool()));
                    is_ball.push(details.and_then(|d| d.get("isBall")).and_then(|v| v.as_bool()));
                    is_review.push(details.and_then(|d| d.get("hasReview")).and_then(|v| v.as_bool()));
                    pitch_type.push(details.and_then(|d| d.get("type")).and_then(|t| t.get("code")).and_then(|s| s.as_str()).map(|s| s.to_string()));
                    pitch_description.push(details.and_then(|d| d.get("type")).and_then(|t| t.get("description")).and_then(|s| s.as_str()).map(|s| s.to_string()));

                    strikes.push(None);
                    balls.push(None);
                    outs.push(None);
                    strikes_after.push(None);
                    balls_after.push(None);
                    outs_after.push(None);
                }
            }
        }
    }
}


    // Construct DataFrame
    let df = DataFrame::new(vec![
        Series::new("game_id".into(), game_id).into(),
        Series::new("game_date".into(), game_date).into(),
        Series::new("batter_id".into(), batter_id).into(),
        Series::new("batter_name".into(), batter_name).into(),
        Series::new("batter_hand".into(), batter_hand).into(),
        Series::new("batter_team".into(), batter_team).into(),
        Series::new("batter_team_id".into(), batter_team_id).into(),
        Series::new("pitcher_id".into(), pitcher_id).into(),
        Series::new("pitcher_name".into(), pitcher_name).into(),
        Series::new("pitcher_hand".into(), pitcher_hand).into(),
        Series::new("pitcher_team".into(), pitcher_team).into(),
        Series::new("pitcher_team_id".into(), pitcher_team_id).into(),
        Series::new("ab_number".into(), ab_number).into(),
        Series::new("inning".into(), inning).into(),
        Series::new("play_description".into(), play_description).into(),
        Series::new("play_code".into(), play_code).into(),
        Series::new("in_play".into(), in_play).into(),
        Series::new("is_strike".into(), is_strike).into(),
        Series::new("is_swing".into(), is_swing).into(),
        Series::new("is_whiff".into(), is_whiff).into(),
        Series::new("is_out".into(), is_out).into(),
        Series::new("is_ball".into(), is_ball).into(),
        Series::new("is_review".into(), is_review).into(),
        Series::new("pitch_type".into(), pitch_type).into(),
        Series::new("pitch_description".into(), pitch_description).into(),
        Series::new("strikes".into(), strikes).into(),
        Series::new("balls".into(), balls).into(),
        Series::new("outs".into(), outs).into(),
        Series::new("strikes_after".into(), strikes_after).into(),
        Series::new("balls_after".into(), balls_after).into(),
        Series::new("outs_after".into(), outs_after).into(),
    ])?;

    Ok(df)
}
