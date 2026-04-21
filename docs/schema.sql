-- Logical schema export for CS2 Anti-Strat (SQLite)

CREATE TABLE maps (
    map_id INTEGER PRIMARY KEY AUTOINCREMENT,
    map_name VARCHAR NOT NULL UNIQUE,
    pos_x FLOAT NOT NULL,
    pos_y FLOAT NOT NULL,
    scale FLOAT NOT NULL,
    radar_image_path VARCHAR
);

CREATE TABLE teams (
    team_id INTEGER PRIMARY KEY AUTOINCREMENT,
    team_name VARCHAR NOT NULL UNIQUE
);

CREATE TABLE players (
    player_id INTEGER PRIMARY KEY AUTOINCREMENT,
    steam_id VARCHAR NOT NULL UNIQUE,
    player_name VARCHAR NOT NULL,
    team_id INTEGER,
    FOREIGN KEY(team_id) REFERENCES teams(team_id)
);

CREATE TABLE matches (
    match_id INTEGER PRIMARY KEY AUTOINCREMENT,
    demo_file_name VARCHAR NOT NULL,
    map_id INTEGER NOT NULL,
    match_date DATETIME,
    FOREIGN KEY(map_id) REFERENCES maps(map_id)
);

CREATE TABLE rounds (
    round_id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id INTEGER NOT NULL,
    round_number INTEGER NOT NULL,
    winner_side VARCHAR,
    FOREIGN KEY(match_id) REFERENCES matches(match_id)
);

CREATE TABLE tick_data (
    tick_id INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    tick INTEGER NOT NULL,
    pos_x FLOAT NOT NULL,
    pos_y FLOAT NOT NULL,
    pos_z FLOAT NOT NULL,
    pixel_x FLOAT,
    pixel_y FLOAT,
    FOREIGN KEY(round_id) REFERENCES rounds(round_id),
    FOREIGN KEY(player_id) REFERENCES players(player_id)
);
