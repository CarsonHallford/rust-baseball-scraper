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
        let game_pk = game.get("gamePk").and_then(|v| v.as_u64());
        let game_date_val = game
            .get("gameData")
            .and_then(|d| d.get("datetime"))
            .and_then(|dt| dt.get("officialDate"))
            .and_then(|s| s.as_str())
            .map(|s| s.to_string());

        if let Some(all_plays) = game
            .get("liveData")
            .and_then(|d| d.get("plays"))
            .and_then(|p| p.get("allPlays"))
            .and_then(|ap| ap.as_array())
        {
            for play in all_plays {
                if let Some(events) = play.get("playEvents").and_then(|e| e.as_array()) {
                    for event in events {
                        let details = event.get("details");

                        // Only include events where in_play is Some(true) or Some(false)
                        let in_play_val = details
                            .and_then(|d| d.get("isInPlay"))
                            .and_then(|v| v.as_bool());

                        if in_play_val.is_none() {
                            continue; // skip events without a valid in_play
                        }

                        // Batter / pitcher / team info
                        let matchup = play.get("matchup");
                        let about = play.get("about");

                        game_id.push(game_pk);
                        game_date.push(game_date_val.clone());

                        batter_id.push(
                            matchup
                                .and_then(|m| m.get("batter"))
                                .and_then(|b| b.get("id"))
                                .and_then(|v| v.as_u64()),
                        );
                        batter_name.push(
                            matchup
                                .and_then(|m| m.get("batter"))
                                .and_then(|b| b.get("fullName"))
                                .and_then(|s| s.as_str())
                                .map(|s| s.to_string()),
                        );
                        batter_hand.push(
                            matchup
                                .and_then(|m| m.get("batSide"))
                                .and_then(|b| b.get("code"))
                                .and_then(|s| s.as_str())
                                .map(|s| s.to_string()),
                        );

                        pitcher_id.push(
                            matchup
                                .and_then(|m| m.get("pitcher"))
                                .and_then(|p| p.get("id"))
                                .and_then(|v| v.as_u64()),
                        );
                        pitcher_name.push(
                            matchup
                                .and_then(|m| m.get("pitcher"))
                                .and_then(|p| p.get("fullName"))
                                .and_then(|s| s.as_str())
                                .map(|s| s.to_string()),
                        );
                        pitcher_hand.push(
                            matchup
                                .and_then(|m| m.get("pitchHand"))
                                .and_then(|h| h.get("code"))
                                .and_then(|s| s.as_str())
                                .map(|s| s.to_string()),
                        );

                        // Determine teams based on inning
                        let (bat_team_val, bat_team_id_val, pit_team_val, pit_team_id_val) =
                            if let Some(about) = about {
                                let is_top = about
                                    .get("isTopInning")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(true);
                                let teams = game.get("gameData").and_then(|d| d.get("teams"));
                                if let Some(teams) = teams {
                                    if is_top {
                                        (
                                            teams.get("away").and_then(|t| t.get("abbreviation")).and_then(|s| s.as_str()).map(|s| s.to_string()),
                                            teams.get("away").and_then(|t| t.get("id")).and_then(|v| v.as_u64()),
                                            teams.get("home").and_then(|t| t.get("abbreviation")).and_then(|s| s.as_str()).map(|s| s.to_string()),
                                            teams.get("home").and_then(|t| t.get("id")).and_then(|v| v.as_u64()),
                                        )
                                    } else {
                                        (
                                            teams.get("home").and_then(|t| t.get("abbreviation")).and_then(|s| s.as_str()).map(|s| s.to_string()),
                                            teams.get("home").and_then(|t| t.get("id")).and_then(|v| v.as_u64()),
                                            teams.get("away").and_then(|t| t.get("abbreviation")).and_then(|s| s.as_str()).map(|s| s.to_string()),
                                            teams.get("away").and_then(|t| t.get("id")).and_then(|v| v.as_u64()),
                                        )
                                    }
                                } else {
                                    (None, None, None, None)
                                }
                            } else {
                                (None, None, None, None)
                            };

                        batter_team.push(bat_team_val);
                        batter_team_id.push(bat_team_id_val);
                        pitcher_team.push(pit_team_val);
                        pitcher_team_id.push(pit_team_id_val);

                        ab_number.push(
                            about
                                .and_then(|a| a.get("atBatIndex"))
                                .and_then(|v| v.as_u64())
                                .map(|v| v as u32),
                        );
                        inning.push(
                            about
                                .and_then(|a| a.get("inning"))
                                .and_then(|v| v.as_u64())
                                .map(|v| v as u32),
                        );

                        // Event details
                        play_description.push(
                            details
                                .and_then(|d| d.get("description"))
                                .and_then(|s| s.as_str())
                                .map(|s| s.to_string()),
                        );
                        play_code.push(
                            details
                                .and_then(|d| d.get("code"))
                                .and_then(|s| s.as_str())
                                .map(|s| s.to_string()),
                        );
                        in_play.push(in_play_val);
                        is_strike.push(
                            details
                                .and_then(|d| d.get("isStrike"))
                                .and_then(|v| v.as_bool()),
                        );
                        is_swing.push(
                            details
                                .and_then(|d| d.get("code"))
                                .and_then(|c| c.as_str())
                                .map(|s| swing_list.contains(&s)),
                        );
                        is_whiff.push(
                            details
                                .and_then(|d| d.get("code"))
                                .and_then(|c| c.as_str())
                                .map(|s| whiff_list.contains(&s)),
                        );
                        is_out.push(
                            details
                                .and_then(|d| d.get("isOut"))
                                .and_then(|v| v.as_bool()),
                        );
                        is_ball.push(
                            details
                                .and_then(|d| d.get("isBall"))
                                .and_then(|v| v.as_bool()),
                        );
                        is_review.push(
                            details
                                .and_then(|d| d.get("hasReview"))
                                .and_then(|v| v.as_bool()),
                        );
                        pitch_type.push(
                            details
                                .and_then(|d| d.get("type"))
                                .and_then(|t| t.get("code"))
                                .and_then(|s| s.as_str())
                                .map(|s| s.to_string()),
                        );
                        pitch_description.push(
                            details
                                .and_then(|d| d.get("type"))
                                .and_then(|t| t.get("description"))
                                .and_then(|s| s.as_str())
                                .map(|s| s.to_string()),
                        );

                        // Dummy columns
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
