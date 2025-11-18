use chrono::{DateTime, FixedOffset};
use reqwest::blocking;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct GameInfo {
    pub game_id: u64,
    pub time: String,
    pub date: String,
    pub away: String,
    pub away_id: u64,
    pub home: String,
    pub home_id: u64,
    pub state: String,
    pub venue_id: u64,
    pub venue_name: String,
    pub gameday_type: String,
}


pub fn fetch_game_schedule(
    sport_id: u32,
    game_type: &str,
    season: u32,
) -> Result<Vec<GameInfo>, Box<dyn Error>> {
    let url = format!(
        "https://statsapi.mlb.com/api/v1/schedule?sportId={}&gameTypes={}&season={}",
        sport_id, game_type, season
    );

    let response = blocking::get(&url)?;
    let json: Value = response.json()?;

    let mut games_info = Vec::new();

    if let Some(dates) = json.get("dates").and_then(|d| d.as_array()) {
        for date_entry in dates {
            if let Some(games) = date_entry.get("games").and_then(|g| g.as_array()) {
                for game in games {
                    let teams = game.get("teams").unwrap_or(&Value::Null);

                    let away_team = teams.get("away").and_then(|a| a.get("team"));
                    let home_team = teams.get("home").and_then(|h| h.get("team"));

                    // Parse gameDate as UTC and convert to EST
                    let dt_est = game
                        .get("gameDate")
                        .and_then(|t| t.as_str())
                        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                        .map(|dt_utc| {
                            let est_offset = FixedOffset::west_opt(5 * 3600).unwrap(); // EST UTC-5
                            dt_utc.with_timezone(&est_offset)
                        });

                    let game_info = GameInfo {
                        game_id: game.get("gamePk").and_then(|p| p.as_u64()).unwrap_or(0),
                        time: dt_est
                            .map(|dt| dt.format("%I:%M %p").to_string()) // 12-hour AM/PM
                            .unwrap_or_default(),
                        date: dt_est
                            .map(|dt| dt.format("%m/%d/%Y").to_string()) // MM/DD/YYYY
                            .unwrap_or_default(),
                        away: away_team
                            .and_then(|t| t.get("name"))
                            .and_then(|n| n.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        away_id: away_team
                            .and_then(|t| t.get("id"))
                            .and_then(|id| id.as_u64())
                            .unwrap_or(0),
                        home: home_team
                            .and_then(|t| t.get("name"))
                            .and_then(|n| n.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        home_id: home_team
                            .and_then(|t| t.get("id"))
                            .and_then(|id| id.as_u64())
                            .unwrap_or(0),
                        state: game
                            .get("status")
                            .and_then(|s| s.get("abstractGameState"))
                            .and_then(|st| st.as_str())
                            .map(|s| s.chars().next().unwrap_or_default().to_string()) // Take the first char
                            .unwrap_or_default(),
                        venue_id: game
                            .get("venue")
                            .and_then(|v| v.get("id"))
                            .and_then(|id| id.as_u64())
                            .unwrap_or(0),
                        venue_name: game
                            .get("venue")
                            .and_then(|v| v.get("name"))
                            .and_then(|n| n.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        gameday_type: game
                            .get("gamedayType")
                            .and_then(|t| t.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    };

                    games_info.push(game_info);
                }
            }
        }
    }

    Ok(games_info)
}
