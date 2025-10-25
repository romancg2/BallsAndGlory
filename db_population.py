import os
import csv
import math
import random
import sqlite3
from tabulate import tabulate
from faker import Faker
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import datetime as dt
from typing import Tuple
import decision_making
from decision_making import adjust_board_satisfaction,season_end_board_adjustments


# Nationality weighting by home league country
# (Only uses nationalities you already support in fakers)
NATIONALITY_PROFILES = {
    "Spain": [
        ("Spain", 65),   # strong domestic core
        ("Argentina", 10),
        ("France", 7),
        ("Italy", 5),
        ("Germany", 3),
        ("Netherlands", 3),
        ("England", 2),
        ("Random", 5),
    ],
    "England": [
        ("England", 70),
        ("France", 6),
        ("Germany", 5),
        ("Netherlands", 5),
        ("Spain", 4),
        ("Italy", 4),
        ("Argentina", 3),
        ("Random", 3),
    ],
}

# Fallback used when no profile is defined
DEFAULT_NATIONALITY_WEIGHTS = [
    ("England", 40), ("Spain", 20), ("France", 10),
    ("Germany", 10), ("Italy", 8), ("Netherlands", 5),
    ("Argentina", 4), ("Random", 3),
]


POSITION_ATTRIBUTE_CAPS = {
    "GK": {
        "at_scoring": 150,
        "at_dribbling": 400,
        "at_passing": 700,
        "at_defending": 400,
        "at_goalkeeping": 2000
    },
    "CB": {"at_scoring": 300, "at_dribbling": 600, "at_passing": 900, "at_goalkeeping": 150},
    "RB": {"at_scoring": 400, "at_passing": 1000, "at_goalkeeping": 150},
    "LB": {"at_scoring": 400, "at_passing": 1000, "at_goalkeeping": 150},
    "CDM": {"at_scoring": 400, "at_passing": 1200, "at_defending": 1500, "at_goalkeeping": 150},
    "CM": {"at_scoring": 600, "at_passing": 1500, "at_defending": 1000, "at_goalkeeping": 150},
    "CAM": {"at_scoring": 900, "at_passing": 1600, "at_defending": 800, "at_goalkeeping": 150},
    "RM": {"at_scoring": 700, "at_passing": 1400, "at_defending": 900, "at_speed": 1600, "at_goalkeeping": 150},
    "LM": {"at_scoring": 700, "at_passing": 1400, "at_defending": 900, "at_speed": 1600, "at_goalkeeping": 150},
    "RW": {"at_scoring": 1000, "at_passing": 1300, "at_defending": 800, "at_goalkeeping": 150},
    "LW": {"at_scoring": 1000, "at_passing": 1300, "at_defending": 800, "at_goalkeeping": 150},
    "ST": {"at_defending": 600, "at_passing": 1000, "at_goalkeeping": 150}
}


position_groups = {
    "GK": "GK",
    "CB": "DEF",
    "RB": "DEF",
    "LB": "DEF",
    "CDM": "MID",
    "CM": "MID",
    "CAM": "MID",
    "RM": "MID",
    "LM": "MID",
    "RW": "MID",
    "LW": "MID",
    "ST": "ST"
}




# Positional weights (GK improved via weight, not hardcoded exceptions)
position_attribute_weights = {
    "GK": {
        "at_defending": 3, "at_selfcont": 2, "at_confidence": 2,
        "at_speed": 0.5, "at_passing": 1, "at_scoring": 0.1, "at_goalkeeping": 5,
        "at_dribbling": 0.1
    },
    "CB": {
        "at_defending": 3, "at_confidence": 2, "at_working": 2,
        "at_speed": 0.7, "at_passing": 1, "at_scoring": 0.3, "at_dribbling": 0.2
    },
    "RB": {
        "at_defending": 2, "at_speed": 2, "at_passing": 1.5,
        "at_dribbling": 0.5, "at_confidence": 1.5
    },
    "LB": {
        "at_defending": 2, "at_speed": 2, "at_passing": 1.5,
        "at_dribbling": 0.5, "at_confidence": 1.5
    },
    "CDM": {
        "at_defending": 2.5, "at_passing": 2, "at_working": 2,
        "at_confidence": 1.5, "at_scoring": 0.5, "at_dribbling": 0.5
    },
    "CM": {
        "at_passing": 3, "at_dribbling": 0.8, "at_working": 2,
        "at_confidence": 1.5, "at_defending": 1.5, "at_scoring": 1
    },
    "CAM": {
        "at_passing": 2.5, "at_dribbling": 1.3, "at_scoring": 2,
        "at_confidence": 1.5, "at_speed": 1.2
    },
    "RM": {
        "at_speed": 2, "at_dribbling": 2.2, "at_passing": 1.5,
        "at_confidence": 1.2, "at_scoring": 1.2
    },
    "LM": {
        "at_speed": 2, "at_dribbling": 2.2, "at_passing": 1.5,
        "at_confidence": 1.2, "at_scoring": 1.2
    },
    "RW": {
        "at_speed": 2.5, "at_dribbling": 2.5, "at_scoring": 2,
        "at_confidence": 1.5, "at_passing": 1.2
    },
    "LW": {
        "at_speed": 2.5, "at_dribbling": 2.5, "at_scoring": 2,
        "at_confidence": 1.5, "at_passing": 1.2
    },
    "ST": {
        "at_scoring": 3, "at_speed": 2, "at_confidence": 1.5,
        "at_dribbling": 2.0, "at_passing": 0.8, "at_defending": 0.3
    }
}



# Adjacency graph (primary → nearby roles that make sense together)
_ADJ = {
    "GK": [],
    "CB": ["RB", "LB"],
    "RB": ["CB", "RM"],
    "LB": ["CB", "LM"],
    "CM": ["RM", "LM", "ST"],
    "RM": ["RB", "CM", "ST"],
    "LM": ["LB", "CM", "ST"],
    "ST": ["RM", "LM", "CM"],
}

# If we still need one more related role, look one hop further from these seeds
_SECOND_HOP_FALLBACK = {
    "CB": ["CM"],
    "RB": ["RM", "CM"],
    "LB": ["LM", "CM"],
    "CM": ["CB", "ST"],
    "RM": ["RB", "CM", "ST"],
    "LM": ["LB", "CM", "ST"],
    "ST": ["RM", "LM"],
    "GK": [],
}

def _weighted_choice(weights):
    """Return index 0..n-1 chosen by given integer weights."""
    total = sum(weights)
    r = random.uniform(0, total)
    upto = 0.0
    for i, w in enumerate(weights):
        if upto + w >= r:
            return i
        upto += w
    return len(weights) - 1  # fallback

def _pick_foot(primary: str) -> str:
    """
    Foot distribution:
      - Side roles: bias to the natural side, but allow opposite + both.
      - Central roles: near-even.
    """
    right_roles = {"RB", "RM"}
    left_roles  = {"LB", "LM"}
    # weights are (Right, Left, Both)
    if primary in right_roles:
        opts, w = ("Right", "Left", "Both"), (60, 25, 15)
    elif primary in left_roles:
        opts, w = ("Right", "Left", "Both"), (25, 60, 15)
    else:  # CB, CM, ST, GK
        opts, w = ("Right", "Left", "Both"), (45, 45, 10)
    return opts[_weighted_choice(w)]

def random_positions_and_foot(primary: str) -> Tuple[Tuple[str, ...], str]:
    """
    Given a primary position (GK, CB, RB, LB, CM, RM, LM, ST),
    return (positions_tuple, foot_str).

    - #positions ~ 70%:1, 20%:2, 10%:3
    - Extra positions are role-adjacent (first-hop, then second-hop if needed)
    - Footedness is biased but sometimes “illogical” by design.
    """
    primary = primary.upper()
    if primary not in _ADJ:
        # unknown role: treat as single-role, neutral foot
        return (primary,), random.choice(["Right", "Left", "Both"])

    # how many positions?
    n_positions = (1, 2, 3)[_weighted_choice((70, 20, 10))]

    positions = [primary]
    if n_positions == 1 or primary == "GK":
        return (tuple(positions), _pick_foot(primary))

    # build a pool of related roles (first hop)
    pool = list(_ADJ[primary])

    # if we still need more, enrich with a second hop that keeps things sensible
    if len(pool) < (n_positions - 1):
        for extra in _SECOND_HOP_FALLBACK.get(primary, []):
            if extra not in pool and extra != primary:
                pool.append(extra)

    # still short? add a very loose final fallback from same “band”
    if len(pool) < (n_positions - 1):
        bands = {
            "DEF": {"CB", "RB", "LB"},
            "MID": {"CM", "RM", "LM"},
            "ATT": {"ST"},
            "GK" : {"GK"},
        }
        band = "GK" if primary == "GK" else ("DEF" if primary in bands["DEF"]
                    else "MID" if primary in bands["MID"] else "ATT")
        loose = list(bands[band] - {primary} - set(pool))
        random.shuffle(loose)
        pool.extend(loose)

    random.shuffle(pool)
    for pos in pool:
        if len(positions) >= n_positions:
            break
        if pos not in positions:
            positions.append(pos)

    return (tuple(positions[:n_positions]), _pick_foot(primary))



def calculate_player_fame(age, curr_ability, club_fame):
    # Normalize ability (0–1)
    ability_score = curr_ability / 2000.0

    # Age effect: peak at 27, less for very young/old
    if age < 20:
        age_mult = 0.4
    elif age < 24:
        age_mult = 0.7
    elif age < 30:
        age_mult = 1.0
    elif age < 34:
        age_mult = 0.6
    else:
        age_mult = 0.3

    # Club fame effect (normalize to 0.5–1.0)
    club_mult = 0.5 + (club_fame / 4000.0)  # max 1.0 if club_fame=2000

    # Fame formula (max possible: 2000 * 1.0 * 1.0 = 2000, but only for perfect player at perfect club and age)
    fame = 2000 * ability_score * age_mult * club_mult

    # Clamp to 1–2000
    return int(max(1, min(fame, 2000)))

def gen_logs_insert(DB_PATH, GAME_DATE, log_type, log_desc):
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()
    
    cur.execute(
        "INSERT INTO gen_logs (real_date, game_date, log_type, log_desc) VALUES (?, ?, ?, ?)",
        (datetime.now(), GAME_DATE.isoformat(), log_type, log_desc)
    )
    
    conn.commit()     
    
    conn.close()
    

def init_db(DB_PATH, GAME_DATE):
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()

    cur.executescript("""
    DROP TABLE IF EXISTS fixtures;
    DROP TABLE IF EXISTS players;
    DROP TABLE IF EXISTS players_positions;
    DROP TABLE IF EXISTS players_attr;
    DROP TABLE IF EXISTS player_situ;
    DROP TABLE IF EXISTS players_stats;
    DROP TABLE IF EXISTS players_contract;
    DROP TABLE IF EXISTS consequences;
    DROP TABLE IF EXISTS options_conseq;
    DROP TABLE IF EXISTS options;
    DROP TABLE IF EXISTS situ_options;
    DROP TABLE IF EXISTS situations;
    DROP TABLE IF EXISTS staff;
    DROP TABLE IF EXISTS staff_contract;
    DROP TABLE IF EXISTS staff_attr;
    DROP TABLE IF EXISTS clubs;
    DROP TABLE IF EXISTS global_val;
    DROP TABLE IF EXISTS match_scorers;
    DROP TABLE IF EXISTS competitions;
    DROP TABLE IF EXISTS clubs_competition;
    DROP TABLE IF EXISTS clubs_monthly_economy;
    DROP TABLE IF EXISTS clubs_board;
    DROP TABLE IF EXISTS gen_logs;
    DROP TABLE IF EXISTS transfers_log;
    DROP TABLE IF EXISTS player_stats_summary;
    DROP TABLE IF EXISTS league_links;  
    DROP TABLE IF EXISTS league_movements;  
    """)

    cur.executescript("""
     CREATE TABLE global_val (
         var_id INTEGER PRIMARY KEY AUTOINCREMENT,
         var_name TEXT NOT NULL,
         value_text TEXT,
         value_int INTEGER,
         value_date DATE
     );
     
    CREATE TABLE IF NOT EXISTS gen_logs (
        --id INTEGER PRIMARY KEY AUTOINCREMENT,
        real_date DATE NOT NULL,        
        game_date DATE NOT NULL,
        log_type TEXT NOT NULL,   
        log_desc TEXT NOT NULL
    );  

    CREATE TABLE IF NOT EXISTS transfers_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts DATE NOT NULL,
        type TEXT NOT NULL,                -- 'free' | 'transfer'
        from_club_id INTEGER,
        to_club_id INTEGER,
        player_id INTEGER NOT NULL,
        fee INTEGER DEFAULT 0,
        wage INTEGER DEFAULT 0,
        contract_end DATE
    );


    CREATE TABLE competitions (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        country TEXT NOT NULL,
        level INTEGER,
        rounds INTEGER,
        rules TEXT,
        relegated_clubs INTEGER,
        promoted_clubs INTEGER,
        total_clubs INTEGER,
        is_league BOOLEAN,
        is_cup BOOLEAN
    );

    CREATE TABLE league_links (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        parent_league_id  INTEGER NOT NULL,   -- superior league (e.g., League 1)
        child_league_id   INTEGER NOT NULL,   -- inferior league (e.g., League 2 / North)
        promote_automatic INTEGER NOT NULL DEFAULT 2,  -- auto promotions from child to parent
        promote_playoff   INTEGER NOT NULL DEFAULT 0,  -- (optional) extra via playoffs
        relegate_automatic INTEGER NOT NULL DEFAULT 2, -- auto relegations from parent down to this child
        relegate_playoff   INTEGER NOT NULL DEFAULT 0, -- (optional) extra via playoffs
        priority          INTEGER NOT NULL DEFAULT 1,  -- when multiple children feed one parent, fill in this order
        is_active         INTEGER NOT NULL DEFAULT 1,
        UNIQUE(parent_league_id, child_league_id),
        FOREIGN KEY(parent_league_id) REFERENCES leagues(id),
        FOREIGN KEY(child_league_id)  REFERENCES leagues(id)
    );
    
    CREATE TABLE league_movements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        season TEXT NOT NULL,        -- e.g. '2025/26'
        ts DATE NOT NULL,            -- when applied
        club_id INTEGER NOT NULL,
        from_league_id INTEGER NOT NULL,
        to_league_id   INTEGER NOT NULL,
        reason TEXT,                 -- 'promotion', 'relegation', 'playoff'
        FOREIGN KEY(club_id) REFERENCES clubs(id)
    );

    CREATE TABLE clubs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        short_name TEXT,
        league_id INTEGER,
        stadium TEXT,
        fame INTEGER,
        current_balance_EUR INTEGER,
        FOREIGN KEY (league_id) REFERENCES competitions(id)
    );
    
    
    CREATE TABLE clubs_board (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        manager_satisf INTEGER,
        squad_satisf INTEGER,
        economic_confid INTEGER,
        at_patience INTEGER,
        club_id INTEGER,
        last_manager_change DATE,
        FOREIGN KEY (club_id) REFERENCES clubs(id)
    );

    
    CREATE TABLE clubs_monthly_economy (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        month_date TEXT,
        club_id INTEGER,
        income_total INTEGER,
        expenditure_total INTEGER,
        wages_total INTEGER,
        balance_before INTEGER,
        balance_after INTEGER,
        FOREIGN KEY (club_id) REFERENCES clubs(id)
    );

    CREATE TABLE clubs_competition (
        clubs_competition_id INTEGER PRIMARY KEY AUTOINCREMENT,
        club_id INTEGER,
        competition_id INTEGER,
        is_active BOOLEAN DEFAULT TRUE,
        round INTEGER,
        FOREIGN KEY (competition_id) REFERENCES competitions(id),
        FOREIGN KEY (club_id) REFERENCES clubs(id)
    );

    CREATE TABLE players (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        date_of_birth DATE,
        nationality TEXT,
        second_nationality TEXT,
        position TEXT,
        club_id INTEGER,
        value INTEGER,
        is_retired BOOLEAN DEFAULT FALSE,
        fame INTEGER DEFAULT 0,
        peak_fame INTEGER DEFAULT 0,
        last_transfer_ts DATE,
        FOREIGN KEY (club_id) REFERENCES clubs(id)
    );
    
    CREATE TABLE players_positions (
        player_id INTEGER,
        position TEXT,
        foot TEXT,
        FOREIGN KEY (player_id) REFERENCES players(id)
    );

    CREATE TABLE players_contract (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER,
        club_id INTEGER,
        contract_type TEXT,
        contract_start DATE,
        contract_end DATE,
        wage INTEGER,
        is_terminated BOOLEAN DEFAULT FALSE,
        FOREIGN KEY (club_id) REFERENCES clubs(id),
        FOREIGN KEY (player_id) REFERENCES players(id)
    );

    CREATE TABLE players_attr (
        player_id INTEGER PRIMARY KEY,
        at_luck INTEGER,
        at_selfcont INTEGER,
        at_honour INTEGER,
        at_crazyness INTEGER,
        at_working INTEGER,
        at_sexatract INTEGER,
        at_friendship INTEGER,
        at_speed INTEGER,
        at_dribbling INTEGER,
        at_goalkeeping INTEGER,
        at_defending INTEGER,
        at_passing INTEGER,
        at_scoring INTEGER,
        at_happiness INTEGER,
        at_confidence INTEGER,
        at_hope INTEGER,
        at_curr_ability INTEGER,
        at_pot_ability INTEGER,
        FOREIGN KEY (player_id) REFERENCES players(id)
    );
    
    CREATE TABLE players_stats (
        player_id INTEGER,
        fixture_id INTEGER,
        club_id INTEGER,
        minutes_played INTEGER,
        tackles_attempted INTEGER,
        tackles_comp INTEGER,
        
        passes_attempted INTEGER,
        passes_comp INTEGER,
        
        shoots_attempted INTEGER,
        shoots_target INTEGER,
        
        goals_scored INTEGER,

        yellow_cards INTEGER,

        red_cards INTEGER,        

        FOREIGN KEY (player_id) REFERENCES players(id),
        FOREIGN KEY (fixture_id) REFERENCES fixtures(id)
        
    );

    CREATE TABLE player_stats_summary (
        season TEXT,
        player_id INTEGER,
        club_id INTEGER,
        competition_id INTEGER,
        
        matches_played INTEGER,
        
        minutes_played_avg INTEGER,

        tackles_comp_percent INTEGER,
        
        passes_comp_percent INTEGER,
        
        shoots_target_percent INTEGER,
        
        goals_scored_total INTEGER,

        yellow_cards_total INTEGER,

        red_cards_total INTEGER,        

        FOREIGN KEY (player_id) REFERENCES players(id),
        FOREIGN KEY (competition_id) REFERENCES competitions(id),
        FOREIGN KEY (club_id) REFERENCES clubs(id)
        
    );

    CREATE TABLE staff (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        date_of_birth DATE,
        nationality TEXT,
        second_nationality TEXT,
        role TEXT,
        fame INTEGER DEFAULT 0,
        club_id INTEGER,
        former_player_id INTEGER,
        preferred_formation TEXT,
        is_retired BOOLEAN DEFAULT FALSE,
        FOREIGN KEY (former_player_id) REFERENCES players(id),
        FOREIGN KEY (club_id) REFERENCES clubs(id)
    );
    
    CREATE TABLE staff_contract (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        staff_id INTEGER,
        club_id INTEGER,
        contract_type TEXT,
        contract_start DATE,
        contract_end DATE,
        wage INTEGER,
        is_terminated BOOLEAN DEFAULT FALSE,
        FOREIGN KEY (staff_id) REFERENCES staff(id),
        FOREIGN KEY (club_id) REFERENCES clubs(id)
    );
    
    CREATE TABLE staff_attr (
        staff_id INTEGER PRIMARY KEY,
        at_goalkeeping INTEGER,
        at_tackling INTEGER,
        at_passing INTEGER,
        at_shooting INTEGER,
        at_physio INTEGER,
        at_medical INTEGER,
        at_scouting INTEGER,
        at_curr_ability INTEGER,
        at_pot_ability INTEGER,
        FOREIGN KEY (staff_id) REFERENCES staff(id)
    );

    CREATE TABLE fixtures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_date DATE NOT NULL,
        home_club_id INTEGER,
        away_club_id INTEGER,
        competition_id INTEGER,
        home_goals INTEGER,
        away_goals INTEGER,
        played BOOLEAN DEFAULT 0,
        competition_round INTEGER,
        home_goals_pk INTEGER,
        away_goals_pk INTEGER,
        season TEXT,   -- store as string like "2025/26"
        FOREIGN KEY (home_club_id) REFERENCES clubs(id),
        FOREIGN KEY (away_club_id) REFERENCES clubs(id)
    );

    CREATE TABLE match_scorers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL,
        fixture_id INTEGER NOT NULL,
        goal_minute TEXT,
        FOREIGN KEY (player_id) REFERENCES players(id),
        FOREIGN KEY (fixture_id) REFERENCES fixtures(id)
    );
    """)

    leagues = [
        # England
        (1, "Premier League",     "England", 1, 20, 1, 0),
        (2, "Championship",       "England", 2, 20, 1, 0),  # keep 20 to match your current setup
        (3, "FA Cup",             "England", 99, 40, 0, 1),
    
        # Spain
        (4, "LaLiga",             "Spain",   1, 20, 1, 0),  # Spanish First Division
        (5, "Segunda División",   "Spain",   2, 22, 1, 0),  # Spanish Second Division (commonly 22)
        (6, "Copa del Rey",       "Spain", 99, 42, 0, 1)
    ]
    cur.executemany("""
        INSERT INTO competitions (id, name, country, level, total_clubs, is_league, is_cup)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, leagues)
    
    
    cur.execute("""
            INSERT INTO league_links(parent_league_id, child_league_id, promote_automatic, relegate_automatic, priority)
            VALUES (1, 2, 2, 2, 1);
    """)
    
    cur.execute("""
            INSERT INTO league_links(parent_league_id, child_league_id, promote_automatic, relegate_automatic, priority)
            VALUES (4, 5, 2, 2, 1);
    """)

    cur.execute("INSERT INTO global_val (var_name, value_date) VALUES (?, ?)", ("GAME_DATE", GAME_DATE.isoformat()))
    cur.execute("INSERT INTO global_val (var_name, value_text) VALUES (?, ?)", ("SEASON", "2025/26"))
    
    conn.commit()
    

    cur.execute("CREATE INDEX IF NOT EXISTS idx_transfers_log_ts ON transfers_log(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_transfers_log_player ON transfers_log(player_id)")
    # prevent two moves for same player on the same day
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_transfers_log_player_day ON transfers_log(player_id, ts)")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_players_club_alive ON players(club_id, is_retired)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_players_positions_pos ON players_positions(position, player_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_players_positions_player ON players_positions(player_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_players_attr_player ON players_attr(player_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_transfers_log_player_ts ON transfers_log(player_id, ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_staff_club_role ON staff(club_id, role)")        
        
    conn.commit()     
    
    conn.close()
    print("✅ Database initialized:", DB_PATH)
    
    
def player_stats_summary_func(DB_PATH):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO player_stats_summary(season, player_id, club_id, competition_id, matches_played, minutes_played_avg, tackles_comp_percent, passes_comp_percent, shoots_target_percent, goals_scored_total, yellow_cards_total, red_cards_total)
        SELECT
        	f.season,
            ps.player_id,
            ps.club_id,
            f.competition_id,
            count(f.id),
            AVG(ps.minutes_played)            AS avg_minutes,
            ROUND(
                100.0 * SUM(COALESCE(ps.tackles_comp, 0))
                / NULLIF(SUM(COALESCE(ps.tackles_attempted, 0)), 0)
            , 2) 	AS tackles_comp_pct,
            ROUND(
                100.0 * SUM(COALESCE(ps.passes_comp, 0))
                / NULLIF(SUM(COALESCE(ps.passes_attempted, 0)), 0)
            , 2) 	AS passes_comp_pct,
            ROUND(
                100.0 * SUM(COALESCE(ps.shoots_target, 0))
                / NULLIF(SUM(COALESCE(ps.shoots_attempted, 0)), 0)
            , 2) 	AS shoots_target_pct,
        	sum(goals_scored) as goals_scored_total,
        	sum(yellow_cards) as yellow_cards_total,
        	sum(red_cards) as red_cards_total
        FROM players_stats ps
        JOIN fixtures f ON f.id = ps.fixture_id
        GROUP BY f.season, ps.player_id, f.competition_id, ps.club_id
    """)
    conn.commit()
    
    cur.execute("""
            DELETE FROM players_stats
    """)
    
    cur.execute("""
            DELETE FROM match_scorers
    """)    
    
    conn.commit()
    conn.close()
    

def distribute_attributes(curr_ability, pot_ability, position, club_fame=1000):
    """
    Generate attributes so that:
    - Average attribute ~ curr_ability
    - Positional weights push some attributes higher/lower
    - Fame gives a clearer edge to bigger clubs (0.9 → 1.1 range)
    """
    weights = position_attribute_weights.get(position, {})

    # Fame multiplier (0.9 – 1.1)
    fame_mult = 0.9 + (club_fame / 2000.0) * 0.2

    attrs = {}
    for attr in [
        "at_luck", "at_selfcont", "at_honour", "at_crazyness", "at_working",
        "at_sexatract", "at_friendship", "at_speed", "at_dribbling",
        "at_goalkeeping", "at_defending", "at_passing", "at_scoring",
        "at_happiness", "at_confidence", "at_hope"
    ]:
        weight = weights.get(attr, 1.0)

        # Base around CA
        val = curr_ability * weight

        # Noise ±15%
        val *= random.uniform(0.85, 1.15)

        # Fame effect
        val *= fame_mult

        # Clamp to potential and hard cap
        val = max(200, min(int(val), pot_ability, 2000))

        # Apply position-specific hard caps
        cap = POSITION_ATTRIBUTE_CAPS.get(position, {}).get(attr, 2000)
        val = min(val, cap)

        attrs[attr] = val

    return attrs
    
# -----------------------------
# Player generation & values
# -----------------------------
def random_age(position=None):
    if position in ("GK",):
        buckets = [(17,20,0.05),(21,24,0.20),(25,28,0.35),(29,32,0.30),(33,38,0.10)]
    elif position in ("CB","RB","LB"):
        buckets = [(17,20,0.08),(21,24,0.28),(25,28,0.37),(29,32,0.22),(33,36,0.05)]
    elif position in ("CM","CDM","CAM"):
        buckets = [(17,20,0.12),(21,24,0.32),(25,28,0.37),(29,31,0.16),(32,35,0.03)]
    elif position in ("RW","LW","ST","CF","FW"):
        buckets = [(17,20,0.18),(21,24,0.37),(25,28,0.32),(29,31,0.11),(32,35,0.02)]
    else:
        buckets = [(17,20,0.12),(21,24,0.35),(25,28,0.37),(29,31,0.14),(32,35,0.02)]
    lo, hi, _ = random.choices(buckets, weights=[b[2] for b in buckets])[0]
    return random.randint(lo, hi)
    

def random_potential():
    buckets = [
        (1000, 1400, 0.55),
        (1401, 1700, 0.30),
        (1701, 1850, 0.12),
        (1851, 2000, 0.03),
    ]
    lo, hi, _ = random.choices(buckets, weights=[b[2] for b in buckets])[0]
    return random.randint(lo, hi)

def generate_player(GAME_DATE, fakers, position=None, club_id=None, club_fame=None, force_youth=False, home_country=None):
    # --- Identity / Nationality (country-aware) ---
    if home_country and home_country in NATIONALITY_PROFILES:
        choices = NATIONALITY_PROFILES[home_country]
    else:
        choices = DEFAULT_NATIONALITY_WEIGHTS

    nat_labels = [n for n, _ in choices]
    nat_weights = [w for _, w in choices]
    nationality = random.choices(nat_labels, weights=nat_weights, k=1)[0]

    faker = fakers[nationality] if nationality in fakers else fakers["Random"]
    if nationality == "Random":
        nationality = faker.country()

    first_name = faker.first_name_male()
    last_name  = faker.last_name()
    # (rest of your generate_player stays the same)

    # --- Position ---
    if position is None:
        position = random.choice(list(position_groups.keys()))

    # --- Age ---
    if force_youth:
        age = random.randint(16, 18)
    else:
        age = random_age(position)

    birth_year = GAME_DATE.year - age
    birth_month = random.randint(1, 12)
    birth_day = random.randint(1, 28)
    date_of_birth = date(birth_year, birth_month, birth_day)

    # --- Fame normalization ---
    if club_fame is None:
        club_fame = 1000
    fame_norm = 0.8 + (club_fame / 2000.0) * 0.4   # ≈ 0.8 → 1.2

    # --- Market Value ---
    base_value = random.randint(100_000, 50_000_000)
    value = int(base_value * fame_norm)

    # Age-related decline
    if age > 31:
        value = int(value * (0.2 if age >= 34 else 0.4))

    # --- Contract (aligned to seasons) ---
    start_year = GAME_DATE.year - random.randint(1, 3)
    contract_start = date(start_year, 9, 1)

    end_year = GAME_DATE.year + random.randint(1, 3)
    contract_end = date(end_year, 8, 31)

    wage = max(150_000, min(int(value * 0.05 * random.uniform(0.8, 1.2)), 20_000_000))

    # --- Abilities ---
    pot_ability = random_potential()
    pot_ability = int(pot_ability * fame_norm)
    pot_ability = min(2000, max(800, pot_ability))  # clamp

    if age <= 18:
        curr_pct = random.uniform(0.15, 0.25)
    elif age <= 21:
        curr_pct = random.uniform(0.25, 0.4)
    elif age <= 25:
        curr_pct = random.uniform(0.4, 0.65)
    elif age <= 29:
        curr_pct = random.uniform(0.65, 0.9)
    elif age <= 32:
        curr_pct = random.uniform(0.7, 0.95)
    else:
        curr_pct = random.uniform(0.4, 0.7)

    curr_ability = int(min(pot_ability * curr_pct * fame_norm, pot_ability))
    attrs = distribute_attributes(curr_ability, pot_ability, position)

    # --- Fame ---
    fame = calculate_player_fame(age, curr_ability, club_fame)

    # --- Pack results ---
    player_attr = tuple(attrs[a] for a in attrs) + (curr_ability, pot_ability)

    # Include peak_fame as equal to fame at creation
    player = (first_name, last_name, date_of_birth, nationality,
              position, club_id, value, fame, fame)  # last fame = peak_fame

    contract = (club_id, "Professional", contract_start.isoformat(),
                contract_end.isoformat(), wage)

    return player, player_attr, contract

def calculate_age(birth_date, game_date):
    if isinstance(birth_date, str):
        birth_date = dt.datetime.strptime(birth_date, "%Y-%m-%d").date()
    elif isinstance(birth_date, dt.datetime):
        birth_date = birth_date.date()
    if isinstance(game_date, str):
        game_date = dt.datetime.strptime(game_date, "%Y-%m-%d").date()
    elif isinstance(game_date, dt.datetime):
        game_date = game_date.date()
    age = game_date.year - birth_date.year
    if (game_date.month, game_date.day) < (birth_date.month, birth_date.day):
        age -= 1
    return age



def top_up_free_agents(DB_PATH, GAME_DATE, fakers, per_club=5):
    """
    Ensure there are at least (per_club × #league clubs) free agents available.
    Keeps positional balance similar to club needs.
    """
    import sqlite3, datetime as dt, random

    # Positional mix for the pool (GK fewer, ST/CB a bit more)
    POS_WEIGHTS = {
        "GK": 1, "CB": 3, "RB": 2, "LB": 2, "CDM": 1, "CM": 2, "CAM": 1,
        "RM": 1, "LM": 1, "RW": 1, "LW": 1, "ST": 3
    }
    POSITIONS = list(POS_WEIGHTS.keys())
    WEIGHTS   = [POS_WEIGHTS[p] for p in POSITIONS]

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Count league clubs (ignore cups)
    cur.execute("""
        SELECT COUNT(*)
        FROM clubs c
        JOIN competitions comp ON comp.id = c.league_id
        WHERE comp.is_league = 1
    """)
    clubs_n = cur.fetchone()[0] or 0

    # Current free agents (unemployed + not retired)
    cur.execute("SELECT COUNT(*) FROM players WHERE club_id IS NULL AND is_retired = 0")
    free_now = cur.fetchone()[0] or 0

    target = per_club * clubs_n
    need = max(0, target - free_now)
    if need == 0:
        conn.close()
        print(f"✅ Free-agent pool already sufficient: {free_now}/{target}")
        return

    print(f"➕ Creating {need} free agents to reach {target} total ({free_now} → {target})")

    new_players, new_attrs, new_contracts, pos_rows = [], [], [], []

    # Build a small helper to save one player
    def _append_free(position):
        # Use a neutral market context for FAs (club_id=None, club_fame=1000)
        p, a, c = generate_player(
            GAME_DATE, fakers,
            position=position, club_id=None, club_fame=1000,
            home_country=None  # default nationality mix
        )

        # Fame from ability/age for consistency
        age = calculate_age(p[2], GAME_DATE)
        curr_ability = a[-2]
        fame = calculate_player_fame(age, curr_ability, 1000)

        dob = p[2].isoformat() if isinstance(p[2], dt.date) else p[2]

        # players row (note: second_nationality None)
        new_players.append((
            p[0], p[1], dob, p[3], None, p[4], None, p[6], fame, fame
        ))

        # attrs as-is
        new_attrs.append(a)

        # unemployed contract (0 wage, open end)
        new_contracts.append((None, "Unemployed", GAME_DATE.isoformat(), None, 0))

        # players_positions (primary + extras)
        positions_tuple, foot_str = random_positions_and_foot(p[4])
        for pos in positions_tuple:
            pos_rows.append((None, pos, foot_str))  # player_id filled after insert

    # Choose positions according to weights
    positions_to_make = random.choices(POSITIONS, weights=WEIGHTS, k=need)
    for pos in positions_to_make:
        _append_free(pos)

    # --- Insert all rows ---
    cur.executemany("""
        INSERT INTO players (
            first_name, last_name, date_of_birth, nationality, second_nationality,
            position, club_id, value, fame, peak_fame
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, new_players)

    # Resolve new IDs
    last_rowid = cur.execute("SELECT last_insert_rowid()").fetchone()[0]
    start_id = last_rowid - len(new_players) + 1
    pids = list(range(start_id, start_id + len(new_players)))

    # Attributes
    cur.executemany("""
        INSERT INTO players_attr (
            player_id,
            at_luck, at_selfcont, at_honour, at_crazyness, at_working,
            at_sexatract, at_friendship, at_speed, at_dribbling,
            at_goalkeeping, at_defending, at_passing, at_scoring,
            at_happiness, at_confidence, at_hope,
            at_curr_ability, at_pot_ability
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(pid, *a) for pid, a in zip(pids, new_attrs)])

    # Contracts
    cur.executemany("""
        INSERT INTO players_contract (
            player_id, club_id, contract_type, contract_start, contract_end, wage
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, [(pid, *c) for pid, c in zip(pids, new_contracts)])

    # players_positions
    # Replace placeholder None with actual pid
    filled_pos_rows = []
    for pid, (ppid, pos, foot) in zip(pids, pos_rows[::len(pos_rows)//len(pids) or 1]):  # robust stride
        # Each player may have 1–3 positions; rebuild per player to keep variety
        primary = new_players[pids.index(pid)][5]
        positions_tuple, foot_str = random_positions_and_foot(primary)
        for ppos in positions_tuple:
            filled_pos_rows.append((pid, ppos, foot_str))

    cur.executemany(
        "INSERT INTO players_positions (player_id, position, foot) VALUES (?, ?, ?)",
        filled_pos_rows
    )

    conn.commit()
    conn.close()
    print(f"✅ Added {need} new free agents (total now ≥ {target})")



def populate_all_players(DB_PATH, GAME_DATE, fakers):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    def create_players_for_league(league_id):
        # get league country once
        cur.execute("SELECT country FROM competitions WHERE id = ?", (league_id,))
        row = cur.fetchone()
        league_country = row[0] if row else None
    
        cur.execute("SELECT id, fame FROM clubs WHERE league_id = ?", (league_id,))
        clubs = cur.fetchall()
        if not clubs:
            print(f"⚠️ No clubs found for league {league_id}")
            return [], [], []

        position_counts = {
            "GK": 2, "CB": 3, "RB": 2, "LB": 2,
            "CDM": 1, "CM": 2, "CAM": 1,
            "RM": 1, "LM": 1, "RW": 1, "LW": 1,
            "ST": 3
        }
    
        players, attrs, contracts = [], [], []
        for club_id, club_fame in clubs:
            for pos, count in position_counts.items():
                for _ in range(count):
                    p, a, c = generate_player(
                        GAME_DATE, fakers,
                        position=pos, club_id=club_id, club_fame=club_fame,
                        home_country=league_country  # ← key change
                    )
    
                    # (your fame/ISO DOB packing exactly as before)
                    age = calculate_age(p[2], GAME_DATE)
                    curr_ability = a[-2]
                    fame = calculate_player_fame(age, curr_ability, club_fame)
                    dob = p[2].isoformat() if isinstance(p[2], dt.date) else p[2]
    
                    players.append((p[0], p[1], dob, p[3], None, p[4], p[5], p[6], fame, fame))
                    attrs.append(a)
                    contracts.append(c)

        return players, attrs, contracts

    # 🔎 Find all league_ids that actually have clubs (works for ENG + ESP + future leagues)
    cur.execute("SELECT DISTINCT league_id FROM clubs WHERE league_id IS NOT NULL")
    league_ids = [r[0] for r in cur.fetchall()]
    if not league_ids:
        print("⚠️ No leagues found in clubs; nothing to generate.")
        conn.close()
        return

    # Generate for every league present (e.g., 1,2,4,5)
    all_players, all_attrs, all_contracts = [], [], []
    for lid in league_ids:
        p, a, c = create_players_for_league(lid)
        all_players += p
        all_attrs += a
        all_contracts += c

    if not all_players:
        print("⚠️ No players generated.")
        conn.close()
        return

    # Insert players
    cur.executemany("""
        INSERT INTO players (
            first_name, last_name, date_of_birth, nationality, second_nationality,
            position, club_id, value, fame, peak_fame
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, all_players)

    # IDs for attrs/contracts
    last_rowid = cur.execute("SELECT last_insert_rowid()").fetchone()[0]
    start_id = last_rowid - len(all_players) + 1
    player_ids = list(range(start_id, start_id + len(all_players)))

    # Attributes
    players_attr_with_ids = [(pid, *attr) for pid, attr in zip(player_ids, all_attrs)]
    cur.executemany("""
        INSERT INTO players_attr (
            player_id,
            at_luck, at_selfcont, at_honour, at_crazyness, at_working,
            at_sexatract, at_friendship, at_speed, at_dribbling,
            at_goalkeeping, at_defending, at_passing, at_scoring,
            at_happiness, at_confidence, at_hope,
            at_curr_ability, at_pot_ability
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, players_attr_with_ids)

    # Contracts
    players_contracts_with_ids = [(pid, *c) for pid, c in zip(player_ids, all_contracts)]
    cur.executemany("""
        INSERT INTO players_contract (
            player_id, club_id, contract_type, contract_start, contract_end, wage
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, players_contracts_with_ids)

    # players_positions (primary + extras)
    pos_rows = []
    for pid, p in zip(player_ids, all_players):
        primary_pos = p[5]  # position column in all_players tuple
        positions_tuple, foot_str = random_positions_and_foot(primary_pos)
        for pos in positions_tuple:
            pos_rows.append((pid, pos, foot_str))
    if pos_rows:
        cur.executemany(
            "INSERT INTO players_positions (player_id, position, foot) VALUES (?, ?, ?)",
            pos_rows
        )

    conn.commit()
    conn.close()
    print(f"✅ {len(all_players)} league players generated with contracts")
    print("✅ players_positions rows added")


# -----------------------------
# Situations mini-game (optional)
# -----------------------------
def init_db_possib(DB_PATH):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executescript("""
        DROP TABLE IF EXISTS situations;
        DROP TABLE IF EXISTS situ_options;
        DROP TABLE IF EXISTS options;
        DROP TABLE IF EXISTS options_conseq;
        DROP TABLE IF EXISTS consequences;
        DROP TABLE IF EXISTS player_situ;
    """)
    cur.executescript("""
    CREATE TABLE situations (
        sit_id INTEGER PRIMARY KEY AUTOINCREMENT,
        sit_title TEXT NOT NULL,
        sit_description TEXT NOT NULL
    );
    CREATE TABLE situ_options (
        sit_id INTEGER,
        opt_id INTEGER,
        FOREIGN KEY (sit_id) REFERENCES situations(sit_id),
        FOREIGN KEY (opt_id) REFERENCES options(opt_id)
    );
    CREATE TABLE options (
        opt_id INTEGER PRIMARY KEY AUTOINCREMENT,
        opt_title TEXT NOT NULL,
        opt_description TEXT NOT NULL
    );
    CREATE TABLE options_conseq (
        opt_id INTEGER,
        conseq_id INTEGER,
        conseq_probab INTEGER,
        FOREIGN KEY (opt_id) REFERENCES options(opt_id),
        FOREIGN KEY (conseq_id) REFERENCES consequences(conseq_id)
    );
    CREATE TABLE consequences (
        conseq_id INTEGER PRIMARY KEY AUTOINCREMENT,
        conseq_title TEXT NOT NULL,
        conseq_description TEXT NOT NULL,
        conseq_luck INTEGER,
        conseq_selfcont INTEGER,
        conseq_honour INTEGER,
        conseq_crazyness INTEGER,
        conseq_working INTEGER,
        conseq_sexatract INTEGER,
        conseq_friendship INTEGER,
        conseq_speed INTEGER,
        conseq_dribbling INTEGER,
        conseq_defending INTEGER,
        conseq_passing INTEGER,
        conseq_scoring INTEGER,
        conseq_happiness INTEGER,
        conseq_confidence INTEGER,
        conseq_hope INTEGER
    );
    CREATE TABLE player_situ (
        player_situ_id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER,
        sit_id INTEGER,
        conseq_id INTEGER,
        player_situ_date DATE,
        FOREIGN KEY (player_id) REFERENCES players(id),
        FOREIGN KEY (sit_id) REFERENCES situations(sit_id),
        FOREIGN KEY (conseq_id) REFERENCES consequences(conseq_id)
    );
    """)
    situations = [
        ("Penalty taking", "You have to kick a penalty. The goalkeeper is lefty."),
        ("Penalty taking", "You have to kick a penalty. The goalkeeper is righty.")
    ]
    cur.executemany("INSERT INTO situations (sit_title, sit_description) VALUES (?, ?)", situations)

    options = [
        ("You kick it to the left", "You kick the penalty to the left side of the goalkeeper."),
        ("You kick it to the right", "You kick the penalty to the right side of the goalkeeper."),
        ("You kick it to the center", "You kick the penalty to the center of the goal.")
    ]
    cur.executemany("INSERT INTO options (opt_title, opt_description) VALUES (?, ?)", options)

    consequences = [
        ("Goal scored!", "Goaaaaaaal! The goalkeeper was fooled.", 0,0,0,0,0,0,0, 0,0,0,0,1,2,2,1),
        ("Goal scored!", "The ball hits the post and it goes inside!", 1,0,0,0,0,0,0, 0,0,0,0,1,2,1,1),
        ("Penalty stopped. Corner.", "The goalkeeper stops the ball and it goes to corner.", 0,0,0,0,0,0,0, 0,0,0,0,0,-1,-1,-1),
        ("Penalty stopped. Ball held.", "The goalkeeper stops the ball and secures it.", 0,0,0,0,0,0,0, 0,0,0,0,-1,-1,-1,-1),
        ("Penalty stopped. Rebound to defender.", "Keeper saves and defender clears.", 0,0,0,0,0,0,0, 0,0,0,0,-1,-2,-2,-2),
        ("Penalty saved. Rebound to player.", "Keeper saves but rebound is live.", 0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0)
    ]
    cur.executemany("""
        INSERT INTO consequences (conseq_title, conseq_description,
            conseq_luck, conseq_selfcont, conseq_honour, conseq_crazyness, conseq_working, conseq_sexatract, conseq_friendship,
            conseq_speed, conseq_dribbling, conseq_defending, conseq_passing, conseq_scoring, conseq_happiness, conseq_confidence, conseq_hope)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, consequences)

    situ_options = [(1,1),(1,2),(1,3),(2,1),(2,2),(2,3)]
    cur.executemany("INSERT INTO situ_options (sit_id, opt_id) VALUES (?, ?)", situ_options)

    options_conseq = [
        (1,1,20),(1,2,15),(1,3,20),(1,4,15),(1,5,15),(1,6,15),
        (2,1,40),(2,2,20),(2,3,10),(2,4,10),(2,5,10),(2,6,10),
        (3,1,30),(3,2,0),(3,3,15),(3,4,30),(3,5,15),(3,6,10)
    ]
    cur.executemany("INSERT INTO options_conseq (opt_id, conseq_id, conseq_probab) VALUES (?, ?, ?)", options_conseq)
    conn.commit()
    conn.close()
    print("✅ Possibility DB initialized.")