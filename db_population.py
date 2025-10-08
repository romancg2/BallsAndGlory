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

import decision_making
from decision_making import adjust_board_satisfaction,season_end_board_adjustments

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

def init_db(DB_PATH, GAME_DATE):
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()

    cur.executescript("""
    DROP TABLE IF EXISTS fixtures;
    DROP TABLE IF EXISTS players;
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
    """)

    cur.executescript("""
     CREATE TABLE global_val (
         var_id INTEGER PRIMARY KEY AUTOINCREMENT,
         var_name TEXT NOT NULL,
         value_text TEXT,
         value_int INTEGER,
         value_date DATE
     );


    CREATE TABLE competitions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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

    leagues = [("Premier League", "England", 1, 20, True, False),
               ("Championship", "England", 2, 20, True, False),
               ("FA Cup", "England", 99, 40, False, True)
               ]
    cur.executemany("INSERT INTO competitions (name, country, level, total_clubs, is_league, is_cup) VALUES (?, ?, ?, ?, ?, ?)", leagues)

    cur.execute("INSERT INTO global_val (var_name, value_date) VALUES (?, ?)", ("GAME_DATE", GAME_DATE.isoformat()))
    cur.execute("INSERT INTO global_val (var_name, value_text) VALUES (?, ?)", ("SEASON", "2025/26"))
    
    conn.commit()
    conn.close()
    print("✅ Database initialized:", DB_PATH)
    
    

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

def generate_player(GAME_DATE, fakers, position=None, club_id=None, club_fame=None, force_youth=False):
    # --- Identity ---
    nationalities = ["England", "Argentina", "Spain", "Germany", "Netherlands", "France", "Italy", "Random"]
    weights = [90, 1, 1, 2, 1, 1, 1, 3]
    nationality = random.choices(nationalities, weights=weights, k=1)[0]
    faker = fakers[nationality]
    if nationality == "Random":
        nationality = faker.country()

    first_name = faker.first_name_male()
    last_name = faker.last_name()

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


def populate_all_players(DB_PATH, GAME_DATE, fakers):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    def create_players_for_league(league_id):
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
                    p, a, c = generate_player(GAME_DATE,fakers,position=pos, club_id=club_id, club_fame=club_fame)

                    # Calculate fame & peak_fame
                    age = calculate_age(p[2], GAME_DATE)
                    curr_ability = a[-2]
                    fame = calculate_player_fame(age, curr_ability, club_fame)

                    # Ensure date is ISO string
                    if isinstance(p[2], dt.date):
                        dob = p[2].isoformat()
                    else:
                        dob = p[2]

                    # p structure: (first_name, last_name, date_of_birth, nationality, position, club_id, value)
                    players.append((
                        p[0],        # first_name
                        p[1],        # last_name
                        dob,         # date_of_birth
                        p[3],        # nationality
                        None,        # second_nationality (not used yet)
                        p[4],        # position
                        p[5],        # club_id
                        p[6],        # value
                        fame,        # fame
                        fame         # peak_fame
                    ))
                    attrs.append(a)
                    contracts.append(c)

        return players, attrs, contracts

    # --- League players
    players1, attrs1, contracts1 = create_players_for_league(1)
    players2, attrs2, contracts2 = create_players_for_league(2)

    players = players1 + players2
    attrs = attrs1 + attrs2
    contracts = contracts1 + contracts2

    if not players:
        print("⚠️ No players generated.")
        conn.close()
        return

    # Insert players
    cur.executemany("""
        INSERT INTO players (
            first_name, last_name, date_of_birth, nationality, second_nationality,
            position, club_id, value, fame, peak_fame
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, players)

    last_rowid = cur.execute("SELECT last_insert_rowid()").fetchone()[0]
    start_id = last_rowid - len(players) + 1
    player_ids = list(range(start_id, start_id + len(players)))

    # Attributes
    players_attr_with_ids = [(pid, *attr) for pid, attr in zip(player_ids, attrs)]
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
    players_contracts_with_ids = [(pid, *c) for pid, c in zip(player_ids, contracts)]
    cur.executemany("""
        INSERT INTO players_contract (
            player_id, club_id, contract_type, contract_start, contract_end, wage
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, players_contracts_with_ids)

    print(f"✅ {len(players)} league players generated with contracts")

    # --- Free agent pool (50 players)
    free_players, free_attrs, free_contracts = [], [], []
    for _ in range(50):
        p, a, c = generate_player(GAME_DATE,fakers, position=None, club_id=None, club_fame=1000)
        age = calculate_age(p[2], GAME_DATE)
        curr_ability = a[-2]
        fame = calculate_player_fame(age, curr_ability, 1000)

        dob = p[2].isoformat() if isinstance(p[2], dt.date) else p[2]

        free_players.append((
            p[0],   # first_name
            p[1],   # last_name
            dob,    # date_of_birth
            p[3],   # nationality
            None,   # second_nationality
            p[4],   # position
            None,   # no club
            p[6],   # value
            fame,
            fame
        ))
        free_attrs.append(a)
        free_contracts.append((None, "Unemployed", GAME_DATE.isoformat(), None, 0))

    cur.executemany("""
        INSERT INTO players (
            first_name, last_name, date_of_birth, nationality, second_nationality,
            position, club_id, value, fame, peak_fame
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, free_players)

    last_rowid = cur.execute("SELECT last_insert_rowid()").fetchone()[0]
    start_id = last_rowid - len(free_players) + 1
    free_ids = list(range(start_id, start_id + len(free_players)))

    # Attributes
    players_attr_with_ids = [(pid, *attr) for pid, attr in zip(free_ids, free_attrs)]
    cur.executemany("""
        INSERT INTO players_attr (
            player_id, at_luck, at_selfcont, at_honour, at_crazyness, at_working,
            at_sexatract, at_friendship, at_speed, at_dribbling,
            at_goalkeeping, at_defending, at_passing, at_scoring,
            at_happiness, at_confidence, at_hope,
            at_curr_ability, at_pot_ability
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, players_attr_with_ids)

    conn.commit()
    conn.close()
    print(f"✅ {len(free_players)} free agent players generated")
    
    

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