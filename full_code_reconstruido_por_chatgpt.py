# BallsAndGlory_single.py
# One-file version with all latest tweaks (player gen, fame bias, attributes, values, fixtures, simulation)

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

# -----------------------------
# Paths & SQLite 3.12 adapters
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "db")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "fm_database.sqlite")

sqlite3.register_adapter(dt.date, lambda d: d.isoformat())
sqlite3.register_adapter(dt.datetime, lambda t: t.isoformat(" "))
sqlite3.register_converter("DATE", lambda s: dt.date.fromisoformat(s.decode("utf-8")))
sqlite3.register_converter("DATETIME", lambda s: dt.datetime.fromisoformat(s.decode("utf-8")))

# -----------------------------
# Global config
# -----------------------------
GAME_DATE = date(2025, 9, 1)
SEASON = '2025/26'

LEAGUE_DEBUGGING = False
CUP_DEBUGGING = False

# League scoring tuning (final: slightly reduced totals)
GOAL_SCALING = 6.00
DEFENSE_EXP = 0.65
DEF_SUPPRESS = 0.35

# League baselines (lazy)
LEAGUE_ATK_MEAN = None
LEAGUE_DEF_MEAN = None

SCORER_BOOST = 1.15

# Fame → name fakers
fakers = {
    "England": Faker('en_GB'),
    "Argentina": Faker('es_AR'),
    "Spain": Faker('es_ES'),
    "Germany": Faker('de_DE'),
    "Netherlands": Faker('nl_NL'),
    "France": Faker('fr_FR'),
    "Italy": Faker('it_IT'),
    "Random": Faker()
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

# -----------------------------
# DB schema
# -----------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()

    cur.executescript("""
    DROP TABLE IF EXISTS fixtures;
    DROP TABLE IF EXISTS players;
    DROP TABLE IF EXISTS players_attr;
    DROP TABLE IF EXISTS player_situ;
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

# -----------------------------
# Utility
# -----------------------------
def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def to_next_monday(d):
    d = d if isinstance(d, dt.date) else d.date()
    # Monday == 0
    return d + timedelta(days=(0 - d.weekday()) % 7)

def print_table(table_name):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name}")
    rows = cur.fetchall()
    headers = [d[0] for d in cur.description]
    print(f"\n📋 Table: {table_name}")
    print(tabulate(rows, headers=headers, tablefmt="grid"))
    conn.close()

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

def next_saturday(start_dt):
    d = start_dt.date() if isinstance(start_dt, dt.datetime) else start_dt
    days_ahead = (5 - d.weekday()) % 7
    return d + timedelta(days=days_ahead)

def round_robin_rounds(team_ids):
    teams = list(team_ids)
    n = len(teams)
    if n % 2 == 1:
        teams.append(None)
    rounds = []
    for r in range(n - 1):
        pairs = []
        for i in range(n // 2):
            a = teams[i]; b = teams[n - 1 - i]
            if a is not None and b is not None:
                pairs.append((a, b) if r % 2 == 0 else (b, a))
        rounds.append(pairs)
        teams = [teams[0]] + teams[-1:] + teams[1:-1]
    return rounds

def normalize_fame(fame):
    return clamp(0.7 + fame / 2000.0, 0.7, 1.3)

# -----------------------------
# Clubs & fixtures population
# -----------------------------


def initialize_club_balances():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, fame FROM clubs")
    for club_id, fame in cur.fetchall():
        # Example: big clubs start richer
        base_balance = random.randint(20_000_000, 50_000_000)
        fame_bonus = fame * 50_000  # fame influences balance
        balance = base_balance + fame_bonus
        cur.execute("UPDATE clubs SET current_balance_EUR=? WHERE id=?", (balance, club_id))
    conn.commit()

def process_monthly_finances(conn, game_date):
    cur = conn.cursor()
    month_str = game_date.strftime("%Y-%m-01")  # YYYY-MM-DD (first day of month)

    cur.execute("SELECT id, current_balance_EUR, fame FROM clubs")
    clubs = cur.fetchall()

    for club_id, balance_before, fame in clubs:
        # --- Wages (spread over 12 months) ---
        cur.execute("""
            SELECT COALESCE(SUM(wage),0)
            FROM players_contract
            WHERE club_id=? AND contract_start<=? AND contract_end>=?
        """, (club_id, game_date, game_date))
        yearly_wages = cur.fetchone()[0]
        wages_total = yearly_wages // 12   # monthly slice

        # --- Income (tickets, sponsorship, etc.) ---
        # Example: fame drives income, add some randomness
        # Income ~ wages +/- 30%, scaled by fame
        income_total = int(wages_total * random.uniform(0.8, 1.2) * (0.8 + fame / 3000))

        # --- Other expenditure (staff, facilities, etc.) ---
        expenditure_total = int(wages_total * 0.15)  # 15% of monthly wages

        # --- Balance ---
        balance_after = balance_before + income_total - wages_total - expenditure_total

        # Save history
        cur.execute("""
            INSERT INTO clubs_monthly_economy
                (month_date, club_id, income_total, expenditure_total, wages_total,
                 balance_before, balance_after)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (month_str, club_id, income_total, expenditure_total, wages_total,
              balance_before, balance_after))

        # Update club balance
        cur.execute("UPDATE clubs SET current_balance_EUR=? WHERE id=?", (balance_after, club_id))

    conn.commit()
    print(f"✅ Monthly finances processed for {month_str}")



def populate_clubs():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    csv_path = os.path.join(BASE_DIR, "premier_league_clubs.csv")
    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append((row['name'], row.get('short_name'), row.get('league_id'), row.get('stadium'), row.get('fame')))
    cur.executemany("INSERT INTO clubs (name, short_name, league_id, stadium, fame) VALUES (?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()
    print("✅ Clubs populated")

def depopulate_clubs():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM clubs")
    cur.execute("DELETE FROM sqlite_sequence WHERE name='clubs'")
    conn.commit()
    conn.close()
    print("✅ Clubs depopulated.")

def populate_fixtures(competition_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Get competition type
    cur.execute("SELECT is_league, is_cup FROM competitions WHERE id = ?", (competition_id,))
    comp_row = cur.fetchone()
    if not comp_row:
        print(f"⚠️ Competition {competition_id} not found")
        conn.close()
        return
    is_league, is_cup = comp_row

    # Fetch all clubs for this competition
    cur.execute("SELECT club_id FROM clubs_competition WHERE competition_id = ? AND is_active=1", (competition_id,))
    club_ids = [r[0] for r in cur.fetchall()]

    if len(club_ids) < 2:
        print(f"⚠️ Not enough clubs in competition {competition_id}. Found {len(club_ids)}")
        conn.close()
        return

    # Get current season
    cur.execute("SELECT value_text FROM global_val WHERE var_name='SEASON'")
    row = cur.fetchone()
    season = row[0] if row else f"{GAME_DATE.year}/{GAME_DATE.year+1}"

    fixtures_to_insert = []

    if is_league:
        # -------------------
        # League: Round Robin
        # -------------------
        random.shuffle(club_ids)
        if len(club_ids) % 2 == 1:
            club_ids.append(None)

        first_half = round_robin_rounds(club_ids)
        second_half = [[(a2, h2) for (h2, a2) in rnd] for rnd in first_half]
        all_rounds = first_half + second_half

        first_sat = next_saturday(GAME_DATE)
        for round_index, rnd in enumerate(all_rounds):
            sat = first_sat + timedelta(weeks=round_index)
            sun = sat + timedelta(days=1)
            matches = list(rnd)
            random.shuffle(matches)
            k = len(matches) // 2
            for home, away in matches[:k]:
                if home is None or away is None:
                    continue
                fixtures_to_insert.append(
                    (sat.isoformat(), home, away, competition_id, None, None, 0, 1, season)
                )
            for home, away in matches[k:]:
                if home is None or away is None:
                    continue
                fixtures_to_insert.append(
                    (sun.isoformat(), home, away, competition_id, None, None, 0, 1, season)
                )

    # Save fixtures
    if fixtures_to_insert:
        cur.executemany("""
            INSERT INTO fixtures (
                fixture_date, home_club_id, away_club_id,
                competition_id, home_goals, away_goals,
                played, competition_round, season
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, fixtures_to_insert)

    conn.commit()
    conn.close()
    print(f"✅ Fixtures populated for competition {competition_id} ({season})")





def depopulate_fixtures():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM fixtures")
    cur.execute("DELETE FROM sqlite_sequence WHERE name='fixtures'")
    conn.commit()
    conn.close()
    print("✅ Fixtures depopulated.")

def depopulate_match_scorers():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM match_scorers")
    cur.execute("DELETE FROM sqlite_sequence WHERE name='match_scorers'")
    conn.commit()
    conn.close()
    print("✅ Match_scorers depopulated.")

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

def fame_bias_for_attributes(club_fame: int) -> float:
    # Moderate fame bias: 0.85 (low fame) → 1.15 (high fame)
    return clamp(0.85 + (club_fame - 1000) / 4000.0, 0.85, 1.15)

def fame_bias_for_potential(club_fame: int) -> float:
    # Slightly stronger on potential to separate big/small clubs
    return clamp(0.85 + (club_fame - 1000) / 3500.0, 0.8, 1.2)

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

def calculate_player_value(curr_ability, pot_ability, age, fame=1000):
    """
    Estimate player transfer value in euros (€).
    Factors: ability, potential, age, fame.
    """
    # Normalize ability (0–1 scale relative to 2000 max)
    ability_score = curr_ability / 2000.0

    # Youth premium (U23 are worth more if talented)
    if age < 20:
        age_mult = 1.4
    elif age < 23:
        age_mult = 1.2
    elif age < 28:
        age_mult = 1.0
    elif age < 31:
        age_mult = 0.7
    elif age < 34:
        age_mult = 0.4
    else:
        age_mult = 0.2

    # Fame multiplier (big clubs inflate value)
    fame_mult = 0.8 + fame / 2000.0  # ~0.8 (small club) → ~1.8 (big club)

    # Base scaling (nonlinear: cheap players cluster, stars stand out)
    base_value = (ability_score ** 3) * 100_000_000  # max ~100M for 2000 CA

    # Apply age + fame + randomness
    value = base_value * age_mult * fame_mult * random.uniform(0.85, 1.15)

    # Clamp to realistic bounds
    return int(max(50_000, min(value, 200_000_000)))

def renew_expired_contracts(conn, game_date):
    """
    Renew contracts that end on the given game_date (e.g., 31 Aug).
    Must be called before monthly finances are processed.
    """
    cur = conn.cursor()

    cur.execute("""
        SELECT pc.player_id, pc.club_id, p.is_retired, pa.at_curr_ability, pa.at_pot_ability, p.value
        FROM players_contract pc
        JOIN players p ON pc.player_id = p.id
        JOIN players_attr pa ON p.id = pa.player_id
        WHERE pc.contract_end = ? AND p.is_retired = 0
    """, (game_date.isoformat(),))

    expired_players = cur.fetchall()
    if not expired_players:
        print("📄 No contracts to renew.")
        return

    for player_id, club_id, _retired, curr_ability, pot_ability, value in expired_players:
        # Wage based on player value + ability
        base_wage = int(value * 0.05 * random.uniform(0.8, 1.2))
        wage = max(150_000, min(base_wage, 20_000_000))

        # New contract starts next day (1 Sep) and ends 1–3 years later
        start_year = game_date.year + 1  # since contract_start is 1 Sep
        contract_start = date(start_year, 9, 1)
        contract_end = date(start_year + random.randint(1, 3), 8, 31)

        cur.execute("""
            INSERT INTO players_contract (
                player_id, club_id, contract_type, contract_start, contract_end, wage
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (player_id, club_id, "Renewed Contract",
              contract_start.isoformat(), contract_end.isoformat(), wage))

        print(f"📝 Renewed contract for player {player_id} at club {club_id}: {wage}€/year until {contract_end}")

    conn.commit()



def generate_player(position=None, club_id=None, club_fame=None, force_youth=False):
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

    # --- Pack results ---
    player_attr = tuple(attrs[a] for a in attrs) + (curr_ability, pot_ability)
    player = (first_name, last_name, date_of_birth, nationality,
              position, club_id, value)
    contract = (club_id, "Professional", contract_start.isoformat(),
                contract_end.isoformat(), wage)

    return player, player_attr, contract





def depopulate_players():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM players")
    cur.execute("DELETE FROM players_attr")
    cur.execute("DELETE FROM sqlite_sequence WHERE name='players'")
    cur.execute("DELETE FROM sqlite_sequence WHERE name='players_attr'")
    conn.commit()
    conn.close()
    print("✅ Players depopulated.")


def maybe_convert_to_staff(conn, player_id):
    cur = conn.cursor()

    # Get player info
    cur.execute("""
        SELECT club_id, first_name, last_name, fame, position, date_of_birth, nationality, second_nationality
        FROM players WHERE id = ?
    """, (player_id,))
    row = cur.fetchone()
    if not row:
        return
    _club_id, first_name, last_name, fame, pos, dob, nat, nat2 = row

    # Chance to convert
    chance = 0.05
    if fame > 800: chance += 0.10
    if random.random() > chance:
        return

    # Pick role(s)
    roles = []
    if pos == "GK":
        roles.append("Goalkeeping Coach")
    elif pos in ("ST", "CF", "FW"):
        roles.append("Attacking Coach")
    elif pos in ("CM", "CDM", "CAM", "RM", "LM"):
        roles.append("Tactical Coach")
    else:
        roles.append("Coach")

    if fame > 700:
        roles.append("Assistant Manager")
    if fame > 500:
        roles.append("Scout")

    role = random.choice(roles)

    # Insert staff record as UNEMPLOYED (no club_id)
    cur.execute("""
        INSERT INTO staff (first_name, last_name, date_of_birth, nationality,
                           second_nationality, role, fame, club_id, former_player_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)
    """, (first_name, last_name, dob, nat, nat2, role, fame, player_id))
    staff_id = cur.lastrowid

    # Map player attributes → staff attributes
    cur.execute("""
        SELECT at_goalkeeping, at_defending, at_passing, at_scoring
        FROM players_attr WHERE player_id = ?
    """, (player_id,))
    pa = cur.fetchone() or (0, 0, 0, 0)
    gk, tac, pas, sho = pa

    if role == "Scout":
        at_scouting = fame // 5 + random.randint(100, 300)
    elif role == "Assistant Manager":
        at_scouting = fame // 10 + random.randint(50, 150)
    else:
        at_scouting = fame // 15 + random.randint(20, 100)

    cur.execute("""
        INSERT INTO staff_attr (staff_id, at_goalkeeping, at_tackling, at_passing,
                                at_shooting, at_physio, at_medical, at_scouting)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        staff_id,
        gk // 2,
        tac // 2,
        pas // 2,
        sho // 2,
        random.randint(200, 600),   # physio
        random.randint(200, 600),   # medical
        at_scouting
    ))

    conn.commit()
    print(f"👔 {first_name} {last_name} retired and became an UNEMPLOYED {role} (staff_id={staff_id})")

# -----------------------------
# Weekly progression
# -----------------------------


def update_players_in_db(conn, game_date):
    cur = conn.cursor()

    if isinstance(game_date, str):
        game_date = datetime.datetime.strptime(game_date, "%Y-%m-%d").date()

    cur.execute("""
        SELECT p.id, p.date_of_birth, p.position, p.club_id,
               pa.at_curr_ability, pa.at_pot_ability
        FROM players p
        JOIN players_attr pa ON p.id = pa.player_id
        WHERE p.is_retired = 0
    """)
    players = cur.fetchall()

    staff_cache = {}

    for player in players:
        player_id, birth_date, pos, club_id, curr_ability, pot_ability = player
        age = calculate_age(birth_date, game_date)

        # --- Retirement check ---
        if age > 35:
            cur.execute("UPDATE players SET is_retired=1, value=0 WHERE id=?", (player_id,))
            cur.execute("SELECT fame FROM clubs WHERE id=?", (club_id,))
            row = cur.fetchone()
            club_fame = row[0] if row else 1000
            
            
            
            maybe_convert_to_staff(conn, player_id)
            

            # Spawn regen
            youth, youth_attr, youth_contract = generate_player(
                position=pos, club_id=club_id,
                club_fame=club_fame, force_youth=True
            )
            cur.execute("""
                INSERT INTO players (
                    first_name, last_name, date_of_birth, nationality,
                    position, club_id, value
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, youth)
            new_id = cur.lastrowid

            cur.execute("""
                INSERT INTO players_attr (
                    player_id,
                    at_luck, at_selfcont, at_honour, at_crazyness, at_working,
                    at_sexatract, at_friendship, at_speed, at_dribbling,
                    at_goalkeeping, at_defending, at_passing, at_scoring,
                    at_happiness, at_confidence, at_hope,
                    at_curr_ability, at_pot_ability
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (new_id, *youth_attr))

            # Contract for youth regen (aligned with season)
            if GAME_DATE.month >= 9:
                start_year = GAME_DATE.year
            else:
                start_year = GAME_DATE.year - 1
            contract_start = date(start_year, 9, 1)

            end_year = GAME_DATE.year + random.randint(1, 3)
            contract_end = date(end_year, 8, 31)

            wage = max(
                150_000,
                min(int(youth[6] * 0.05 * random.uniform(0.8, 1.2)), 20_000_000)
            )

            cur.execute("""
                INSERT INTO players_contract (
                    player_id, club_id, contract_type, contract_start, contract_end, wage
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (new_id, club_id, "Youth Contract",
                  contract_start.isoformat(), contract_end.isoformat(), wage))
            continue

        # --- Development ---
        dev_gap = max(0, pot_ability - curr_ability)

        if age < 20:
            growth = dev_gap * 0.015 * random.uniform(0.8, 1.2)
        elif age < 23:
            growth = dev_gap * 0.025 * random.uniform(0.8, 1.2)
        elif age < 27:
            growth = dev_gap * 0.015 * random.uniform(0.8, 1.2)
        elif age < 30:
            growth = dev_gap * 0.003 * random.uniform(0.8, 1.2)
        elif age < 32:
            growth = 0
        else:
            growth = -curr_ability * 0.012 * random.uniform(0.8, 1.2)

        # --- Staff effect ---
        if club_id not in staff_cache:
            staff_cache[club_id] = compute_staff_multipliers(cur, club_id)
        mult = staff_cache[club_id]

        if pos == "GK":
            growth *= mult["gk"]
        elif pos in ("CB", "RB", "LB", "CDM"):
            growth *= mult["def"]
        elif pos in ("CM", "CAM", "RM", "LM"):
            growth *= mult["pass"]
        elif pos in ("ST", "CF", "FW", "LW", "RW"):
            growth *= mult["shoot"]
        else:
            growth *= mult["fitness"]

        new_curr_ability = max(100, min(int(curr_ability + growth), pot_ability, 2000))
        attrs = distribute_attributes(new_curr_ability, pot_ability, pos)

        # Update value with new ability
        cur.execute("SELECT fame FROM clubs WHERE id=?", (club_id,))
        row = cur.fetchone()
        club_fame = row[0] if row else 1000
        value = calculate_player_value(new_curr_ability, pot_ability, age, fame=club_fame)

        # Save back
        cur.execute("""
            UPDATE players_attr
            SET at_luck=?, at_selfcont=?, at_honour=?, at_crazyness=?, at_working=?,
                at_sexatract=?, at_friendship=?, at_speed=?, at_dribbling=?,
                at_goalkeeping=?, at_defending=?, at_passing=?, at_scoring=?,
                at_happiness=?, at_confidence=?, at_hope=?,
                at_curr_ability=?, at_pot_ability=?
            WHERE player_id=?
        """, (*[attrs[a] for a in attrs], new_curr_ability, pot_ability, player_id))

        cur.execute("UPDATE players SET value=? WHERE id=?", (value, player_id))

    conn.commit()



# -----------------------------
# Match simulation
# -----------------------------
def get_club_fame(cur, club_id):
    cur.execute("SELECT fame FROM clubs WHERE id = ?", (club_id,))
    row = cur.fetchone()
    return row[0] if row else 1000

def get_team_form(cur, club_id, limit=5):
    cur.execute("""
        SELECT home_club_id, away_club_id, home_goals, away_goals
        FROM fixtures
        WHERE (home_club_id = ? OR away_club_id = ?)
          AND played = 1
        ORDER BY fixture_date DESC
        LIMIT ?
    """, (club_id, club_id, limit))
    matches = cur.fetchall()
    points = 0
    for home_id, away_id, hg, ag in matches:
        if hg is None or ag is None:
            continue
        if club_id == home_id:
            points += 3 if hg > ag else (1 if hg == ag else 0)
        else:
            points += 3 if ag > hg else (1 if ag == hg else 0)
    return 1 + (points - 5) / 20.0  # ~0.75–1.25 typical

def team_strengths(cur, club_id):
    """
    Calculate attack & defense strength from players.
    Attack = heavily scoring + speed, with some passing/dribbling.
    Defense = defending + goalkeeping + discipline.
    Fame is applied later in the match simulation.
    """
    cur.execute("""
        SELECT pa.at_scoring, pa.at_speed, pa.at_passing, pa.at_dribbling,
               pa.at_defending, pa.at_goalkeeping, pa.at_selfcont,
               p.position
        FROM players_attr pa
        JOIN players p ON pa.player_id = p.id
        WHERE p.club_id = ? AND p.is_retired = 0
    """, (club_id,))
    rows = cur.fetchall()

    if not rows:
        return 1000, 1000  # fallback neutral

    attack_vals, defense_vals = [], []
    for scoring, speed, passing, dribbling, defending, goalkeeping, selfcont, pos in rows:
        # Attack: strikers/wingers contribute the most
        if pos in ("ST", "CF", "FW"):
            attack_vals.append(scoring * 2.0 + speed * 1.2 + dribbling * 0.6 + passing * 0.4)
        elif pos in ("LW", "RW", "CAM", "AM"):
            attack_vals.append(scoring * 1.4 + speed * 1.0 + dribbling * 1.0 + passing * 0.7)
        elif pos in ("CM", "RM", "LM"):
            attack_vals.append(scoring * 0.7 + speed * 0.6 + dribbling * 0.8 + passing * 1.0)
        else:  # defenders & GK rarely attack
            attack_vals.append(scoring * 0.3 + speed * 0.3 + passing * 0.5)

        # Defense: GK + CBs dominate
        if pos == "GK":
            defense_vals.append(goalkeeping * 2.2 + defending * 0.4 + selfcont * 0.5)
        elif pos in ("CB",):
            defense_vals.append(defending * 1.6 + selfcont * 0.6)
        elif pos in ("RB", "LB", "CDM"):
            defense_vals.append(defending * 1.2 + speed * 0.4 + selfcont * 0.5)
        else:  # midfielders/attackers contribute lightly
            defense_vals.append(defending * 0.5 + selfcont * 0.3)

    attack = sum(attack_vals) / len(rows)
    defense = sum(defense_vals) / len(rows)
    return attack, defense


def compute_league_strength_baselines(conn):
    cur = conn.cursor()
    cur.execute("SELECT id FROM clubs WHERE league_id = 1")
    club_ids = [r[0] for r in cur.fetchall()]
    if not club_ids:
        return 1500.0, 1500.0
    atk_vals, def_vals = [], []
    for cid in club_ids:
        a, d = team_strengths(cur, cid)
        atk_vals.append(a); def_vals.append(d)
    return sum(atk_vals)/len(atk_vals), sum(def_vals)/len(def_vals)

def pick_scorers(cur, club_id, goals, fixture_id, team_name):
    if goals == 0:
        return [], []

    # Get players with ability and position
    cur.execute("""
        SELECT p.id, p.first_name, p.last_name, p.position,
               pa.at_curr_ability, pa.at_scoring, pa.at_speed
        FROM players p
        JOIN players_attr pa ON pa.player_id = p.id
        WHERE p.club_id = ? AND p.is_retired = 0
    """, (club_id,))
    players = cur.fetchall()

    if not players:
        return [], []

    # Get club fame
    cur.execute("SELECT fame FROM clubs WHERE id=?", (club_id,))
    fame_row = cur.fetchone()
    fame = fame_row[0] if fame_row else 1000

    # Fame multiplier (0.9 – 1.1 range)
    fame_mult = 0.9 + (fame / 2000.0) * 0.2

    weighted_pool = []
    for pid, fn, ln, pos, ability, scoring, speed in players:
        # Attribute score: scoring is king, speed helps
        attr_score = scoring * 1.5 + speed * 0.5 + ability * 0.3

        # Position bias
        if pos in ("ST", "CF", "FW"):
            attr_score *= 2.5
        elif pos in ("LW", "RW", "AM", "CAM"):
            attr_score *= 1.5
        elif pos in ("CM", "RM", "LM"):
            attr_score *= 0.8
        elif pos in ("CB", "LB", "RB"):
            attr_score *= 0.3
        else:  # GK
            attr_score *= 0.1

        # Apply fame influence
        weight = attr_score * fame_mult
        weighted_pool.append((pid, fn, ln, weight))

    scorers, names = [], []

    # Choose goal scorers based on weighted attributes
    for _ in range(goals):
        scorer = random.choices(
            weighted_pool, weights=[w for _, _, _, w in weighted_pool], k=1
        )[0]
        pid, fn, ln, _ = scorer
        scorers.append((pid, fixture_id))
        names.append(f"{fn} {ln} ({team_name})")

    return scorers, names


def simulate_fixtures_for_day(conn, day):
    global LEAGUE_ATK_MEAN, LEAGUE_DEF_MEAN
    if LEAGUE_ATK_MEAN is None or LEAGUE_DEF_MEAN is None:
        LEAGUE_ATK_MEAN, LEAGUE_DEF_MEAN = compute_league_strength_baselines(conn)

    cur = conn.cursor()
    cur.execute("""
            SELECT f.id, f.home_club_id, hc.name, f.away_club_id, ac.name, f.competition_id, comp.name, comp.is_cup, f.competition_round
            FROM fixtures f
            JOIN clubs hc ON hc.id = f.home_club_id
            JOIN clubs ac ON ac.id = f.away_club_id
            JOIN competitions comp ON comp.id = f.competition_id
            WHERE f.fixture_date = ? AND f.played = 0
    """, (day,))
    fixtures = cur.fetchall()
    if not fixtures:
        print(f"⚠️ No fixtures found for {day}")
        return

    def fame_effect(fame):
        return clamp(1.0 + (fame - 1000) / 12000.0, 0.94, 1.06)

    def expected_goals_local(attack, opp_defense, fame, form_mult, home_side):
        """
        Expected goals per team in a match.
        - Stronger attacks score more
        - Strong defenses concede less
        - Fame adds extra separation
        - Goal totals tuned slightly lower
        """
        # Base slightly reduced
        base = 0.28 if home_side else 0.22

        # Normalize
        atk_n = attack / max(1.0, LEAGUE_ATK_MEAN)
        def_n = opp_defense / max(1.0, LEAGUE_DEF_MEAN)

        # Attack vs defense ratio (balanced curve)
        ratio = (atk_n ** 1.2) / (def_n ** 0.8)

        # Fame multiplier (moderate)
        fame_mult = fame ** 1.25

        # Raw λ
        lam = base * ratio * fame_mult * (0.92 + 0.18 * form_mult)

        # Defense suppression
        defense_factor = 1.0 / (1.0 + (def_n - 1.0) * 0.85)
        lam *= defense_factor

        # Weak defenses leak a bit more
        if def_n < 0.9:
            lam *= 1.1

        # Random variance
        lam *= random.uniform(0.95, 1.05)

        # Global tuning knob (adjust manually if needed)
        lam *= GOAL_SCALING * 0.9  # reduce totals ~10%

        # Cap
        max_cap = 3.0 if home_side else 2.6
        return max(0.05, min(lam, max_cap))


    def poisson_draw(lmbda, kmax=6):
        lmbda = max(0.05, lmbda)
        probs = [math.exp(-lmbda) * (lmbda ** k) / math.factorial(k) for k in range(kmax)]
        probs[-1] = max(0.0, 1.0 - sum(probs[:-1]))
        return random.choices(range(kmax), weights=probs, k=1)[0]

    for fixture_id, home_id, home_name, away_id, away_name, league_id, league_name, is_cup, competition_round in fixtures:
        home_attack, home_defense = team_strengths(cur, home_id)
        away_attack, away_defense = team_strengths(cur, away_id)

        home_fm = fame_effect(get_club_fame(cur, home_id))
        away_fm = fame_effect(get_club_fame(cur, away_id))
        home_form = get_team_form(cur, home_id)
        away_form = get_team_form(cur, away_id)

        home_lambda = expected_goals_local(home_attack, away_defense, home_fm, home_form, True)
        away_lambda = expected_goals_local(away_attack, home_defense, away_fm, away_form, False)

        home_goals = poisson_draw(home_lambda, kmax=6)
        away_goals = poisson_draw(away_lambda, kmax=6)

        scorers, names = [], []
        hs, hn = pick_scorers(cur, home_id, home_goals, fixture_id, home_name)
        as_, an = pick_scorers(cur, away_id, away_goals, fixture_id, away_name)
        scorers.extend(hs + as_)
        names.extend(hn + an)

        cur.execute("""
            SELECT count(1)
            FROM clubs_competition
            WHERE competition_id = ? and is_active
        """, (league_id,))
        row = cur.fetchone()
        total_clubs = row[0] if row else 0


        if total_clubs==8:
            round_name="Quarter Finals"
        elif total_clubs==4:
            round_name="Semifinals"
        elif total_clubs==2:
            round_name="FINAL"
        else:
            round_name=f"Round {competition_round}"


        if is_cup:

            cur.execute("UPDATE fixtures SET home_goals=?, away_goals=?, played=1 WHERE id=?",
                    (home_goals, away_goals, fixture_id))
            #if scorers:
            #    cur.executemany("INSERT INTO match_scorers (player_id, fixture_id) VALUES (?, ?)", scorers)

            # When its a CUP, we check both legs if they were played to decide penalty kicking in the case of draw, also for finals

            cur.execute("""
                SELECT team1_id, team2_id, goals_team1, goals_team2,
                       (ABS(RANDOM()) % 5) + 1 AS rand1,
                       (ABS(RANDOM()) % 5) + 1 AS rand2,
                       matches_played, total_matches
                FROM (
                    SELECT
                        CASE WHEN home_club_id < away_club_id THEN home_club_id ELSE away_club_id END AS team1_id,
                        CASE WHEN home_club_id < away_club_id THEN away_club_id ELSE home_club_id END AS team2_id,
                        SUM(CASE WHEN home_club_id < away_club_id THEN home_goals ELSE away_goals END) AS goals_team1,
                        SUM(CASE WHEN home_club_id < away_club_id THEN away_goals ELSE home_goals END) AS goals_team2,
                        SUM(CASE WHEN played = 1 THEN 1 ELSE 0 END) AS matches_played,
                        COUNT(1) AS total_matches
                    FROM fixtures
                    WHERE competition_id = ?
                      AND competition_round = ?
                      AND (home_club_id = ? OR away_club_id = ?)
                    GROUP BY team1_id, team2_id
                    ORDER BY team1_id
                )
            """, (league_id, competition_round,home_id,home_id,))
            cup_both_legs = cur.fetchall()
            for team1_id, team2_id, goals_team1, goals_team2, rand1, rand2, matches_played, total_matches in cup_both_legs:
                #print(team1_id, team2_id, goals_team1, goals_team2, rand1, rand2, matches_played, total_matches)

                if matches_played == 1 and total_matches == 2:
                    if CUP_DEBUGGING:
                        print(f"⚽ [{league_name}] - {round_name} - First Leg: {home_name} {home_goals} - {away_goals} {away_name}")
                elif matches_played == 2 and total_matches == 2:

                    if goals_team1 > goals_team2:
                        if CUP_DEBUGGING:
                            print(f"⚽ [{league_name}] - {round_name} - Second Leg: {home_name} {home_goals}({goals_team1}) - {away_goals}({goals_team2}) {away_name}")
                            print(f"⚽ {home_name} advances to the next stage")
                    elif goals_team1 == goals_team2:


                        if rand1 > rand2:
                            if CUP_DEBUGGING:
                                print(f"⚽ [{league_name}] - {round_name} - Second Leg: {home_name} {home_goals}({goals_team1}) - {away_goals}({goals_team2}) {away_name}")
                                print(f"⚽ {home_name} advances to the next stage by penalties ({rand1} - {rand1-1})")
                            cur.execute("UPDATE fixtures SET home_goals_pk=?, away_goals_pk=? WHERE id=?",
                                    (rand1, rand1-1, fixture_id))
                        else:
                            if CUP_DEBUGGING:
                                print(f"⚽ [{league_name}] - {round_name} - Second Leg: {home_name} {home_goals}({goals_team1}) - {away_goals}({goals_team2}) {away_name}")
                                print(f"⚽ {away_name} advances to the next stage by penalties ({rand2} - {rand2-1})")
                            cur.execute("UPDATE fixtures SET home_goals_pk=?, away_goals_pk=? WHERE id=?",
                                    (rand2-1, rand2, fixture_id))
                    else:
                        if CUP_DEBUGGING:
                            print(f"⚽ [{league_name}] - {round_name} - Second Leg: {home_name} {home_goals}({goals_team1}) - {away_goals}({goals_team2}) {away_name}")
                            print(f"⚽ {away_name} advances to the next stage")

                else:
                    if CUP_DEBUGGING:
                        print(f"⚽ [{league_name}] - {round_name}: {home_name} {home_goals} - {away_goals} {away_name}")
                    if home_goals > away_goals:
                        if CUP_DEBUGGING:
                            print(f"⚽ {home_name} is the champion of the {league_name}!! Congratulations!!")
                    elif home_goals == away_goals:

                        if CUP_DEBUGGING:
                            print(f"⚽ {home_name} is the champion of the {league_name}!! Congratulations!!")

                        if rand1 > rand2:
                            if CUP_DEBUGGING:
                                print(f"⚽ [{league_name}] - {round_name}: {home_goals}({rand1}) - {away_goals}({rand1-1}) {away_name}")
                                print(f"⚽ {home_name} is the champion of the {league_name} by penalties ({rand1} - {rand1-1})!! Congratulations!!")
                            cur.execute("UPDATE fixtures SET home_goals_pk=?, away_goals_pk=? WHERE id=?",
                                    (rand1, rand1-1, fixture_id))
                        else:
                            if CUP_DEBUGGING:
                                print(f"⚽ [{league_name}] - {round_name}: {home_goals}({rand2-1}) - {away_goals}({rand2}) {away_name}")
                                print(f"⚽ {away_name} is the champion of the {league_name} by penalties ({rand2} - {rand2-1})!! Congratulations!!")
                            cur.execute("UPDATE fixtures SET home_goals_pk=?, away_goals_pk=? WHERE id=?",
                                    (rand2-1, rand2, fixture_id))


                    else:
                        if CUP_DEBUGGING:
                            print(f"⚽ {away_name} is the champion of the {league_name}!! Congratulations!!")


        else:
            #print("Es Liga")
            cur.execute("UPDATE fixtures SET home_goals=?, away_goals=?, played=1 WHERE id=?",
                        (home_goals, away_goals, fixture_id))
            #if scorers:
            #    cur.executemany("INSERT INTO match_scorers (player_id, fixture_id) VALUES (?, ?)", scorers)
            if LEAGUE_DEBUGGING:
                print(f"⚽ [{league_name}] {home_name} {home_goals} - {away_goals} {away_name}")


        if names:
            # ...inside simulate_fixtures_for_day, replace scorer handling and DB insert...

            scorers, names = [], []
            scorer_minutes = []
            hs, hn = pick_scorers(cur, home_id, home_goals, fixture_id, home_name)
            as_, an = pick_scorers(cur, away_id, away_goals, fixture_id, away_name)
            all_scorers = hs + as_
            all_names = hn + an

            # Assign random minutes to each goal and store for DB and display
            for scorer, name in zip(all_scorers, all_names):
                minute = random.randint(1, 90)
                if minute == 90:
                    plus = random.randint(1, 5)
                    display_minute = f"90+{plus}'"
                    db_minute = 90 + plus
                else:
                    display_minute = f"{minute}'"
                    db_minute = minute
                scorer_minutes.append((db_minute, name, scorer[0], scorer[1]))  # (minute, name, player_id, fixture_id)

            # Sort by minute for display
            scorer_minutes.sort(key=lambda x: x[0])
            if LEAGUE_DEBUGGING or CUP_DEBUGGING:
                print("   Scorers:", ", ".join([f"{x[1]} {x[0] if x[0] < 91 else '90+' + str(x[0]-90)}'" for x in scorer_minutes]))

            # Insert into match_scorers with minute
            if scorer_minutes:
                cur.executemany(
                    "INSERT INTO match_scorers (player_id, fixture_id, goal_minute) VALUES (?, ?, ?)",
                    [(x[2], x[3], x[0]) for x in scorer_minutes]
                )

    conn.commit()

# -----------------------------
# Situations mini-game (optional)
# -----------------------------
def init_db_possib():
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

def apply_consequence(player_id, conseq_id, conn):
    cur = conn.cursor()
    cur.execute("SELECT at_curr_ability, at_pot_ability FROM players_attr WHERE player_id=?", (player_id,))
    row = cur.fetchone()
    if not row:
        return
    curr_ability, pot_ability = row
    if curr_ability > pot_ability:
        return
    cur.execute("""
        SELECT COALESCE(conseq_luck,0), COALESCE(conseq_selfcont,0), COALESCE(conseq_honour,0),
               COALESCE(conseq_crazyness,0), COALESCE(conseq_working,0), COALESCE(conseq_sexatract,0),
               COALESCE(conseq_friendship,0), COALESCE(conseq_speed,0), COALESCE(conseq_dribbling,0),
               COALESCE(conseq_defending,0), COALESCE(conseq_passing,0), COALESCE(conseq_scoring,0),
               COALESCE(conseq_happiness,0), COALESCE(conseq_confidence,0), COALESCE(conseq_hope,0)
        FROM consequences WHERE conseq_id=?
    """, (conseq_id,))
    deltas = cur.fetchone()
    if not deltas:
        return
    total_delta = sum(deltas)
    if curr_ability + total_delta > pot_ability:
        return
    cur.execute("""
        UPDATE players_attr
        SET at_luck = MIN(at_luck + ?, 2000),
            at_selfcont = MIN(at_selfcont + ?, 2000),
            at_honour = MIN(at_honour + ?, 2000),
            at_crazyness = MIN(at_crazyness + ?, 2000),
            at_working = MIN(at_working + ?, 2000),
            at_sexatract = MIN(at_sexatract + ?, 2000),
            at_friendship = MIN(at_friendship + ?, 2000),
            at_speed = MIN(at_speed + ?, 2000),
            at_dribbling = MIN(at_dribbling + ?, 2000),
            at_defending = MIN(at_defending + ?, 2000),
            at_passing = MIN(at_passing + ?, 2000),
            at_scoring = MIN(at_scoring + ?, 2000),
            at_happiness = MIN(at_happiness + ?, 2000),
            at_confidence = MIN(at_confidence + ?, 2000),
            at_hope = MIN(at_hope + ?, 2000),
            at_curr_ability = MIN(at_curr_ability + ?, ?)
        WHERE player_id=?
    """, (*deltas, total_delta, pot_ability, player_id))
    conn.commit()

def run_game(player_id):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    while True:
        cur.execute("SELECT sit_id, sit_title, sit_description FROM situations ORDER BY RANDOM() LIMIT 1")
        row = cur.fetchone()
        if not row:
            print("No situations. Did you call init_db_possib()?")
            break
        sit_id, sit_title, sit_description = row
        print(f"\n⚽ Situation: {sit_title}\n{sit_description}")
        cur.execute("""
            SELECT o.opt_id, o.opt_title, o.opt_description
            FROM options o JOIN situ_options so ON o.opt_id = so.opt_id
            WHERE so.sit_id=?
        """, (sit_id,))
        options = cur.fetchall()
        for opt_id, opt_title, opt_desc in options:
            print(f" {opt_id}) {opt_title} - {opt_desc}")
        print(" Q) Quit game")
        choice = input("👉 Choose an option (ID or Q): ").strip().upper()
        if choice == "Q":
            print("👋 Game over. Thanks for playing!")
            break
        try:
            choice = int(choice)
        except ValueError:
            print("❌ Invalid input.")
            continue
        if not any(opt_id == choice for opt_id,_,_ in options):
            print("❌ Option not available.")
            continue
        cur.execute("""
            SELECT c.conseq_id, c.conseq_title, c.conseq_description, oc.conseq_probab
            FROM consequences c
            JOIN options_conseq oc ON c.conseq_id = oc.conseq_id
            WHERE oc.opt_id = ?
        """, (choice,))
        conseqs = cur.fetchall()
        if not conseqs:
            print("⚠️ No consequences found.")
            continue
        idx = random.choices(range(len(conseqs)), weights=[r[3] for r in conseqs], k=1)[0]
        conseq_id, title, desc, _p = conseqs[idx]
        print(f"\n🎲 Result: {title}\n{desc}")
        cur.execute("INSERT INTO player_situ (player_id, sit_id, conseq_id, player_situ_date) VALUES (?, ?, ?, DATE('now'))",
                    (player_id, sit_id, conseq_id))
        conn.commit()
        apply_consequence(player_id, conseq_id, conn)
        print("\n🟢 Player attributes updated!")
    conn.close()

def clean_player_situ():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='player_situ'")
    if cur.fetchone() is None:
        print("⚠️ The 'player_situ' table does not exist.")
        conn.close()
        return
    cur.execute("DELETE FROM player_situ WHERE player_situ_date IS NOT NULL")
    cur.execute("DELETE FROM sqlite_sequence WHERE name='player_situ'")
    conn.commit()
    conn.close()
    print("✅ player_situ depopulated.")

# -----------------------------
# Game loop & date management
# -----------------------------
def update_game_date_db():
    global SEASON
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Always update the GAME_DATE
    cur.execute(
        "UPDATE global_val SET value_date=? WHERE var_name='GAME_DATE'",
        (GAME_DATE.isoformat(),)
    )

    # Only bump season when it's Aug 31 (end of season)
    if GAME_DATE.month == 8 and GAME_DATE.day == 31:
        cur.execute("SELECT value_text FROM global_val WHERE var_name='SEASON'")
        row = cur.fetchone()

        if row and row[0]:
            current = row[0]
            try:
                start, end = map(int, current.split("/"))
                new_start = start + 1
                new_end = end + 1
                new_season = f"{new_start}/{new_end}"
            except ValueError:
                # fallback if format was not set yet
                new_season = f"{GAME_DATE.year}/{GAME_DATE.year+1}"
        else:
            # if SEASON not initialized yet
            new_season = f"{GAME_DATE.year}/{GAME_DATE.year+1}"

        cur.execute(
            "UPDATE global_val SET value_text=? WHERE var_name='SEASON'",
            (new_season,)
        )
        
        SEASON = new_season
        
        print(f"📅 Season rolled over → {new_season}")

    conn.commit()
    conn.close()


def advance_game_day(current_date):
    return current_date + relativedelta(days=1)

def handle_promotion_relegation():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # --- Compute table for League 1 (bottom 3) ---
    cur.execute("""
        SELECT c.id, c.name,
               COALESCE(SUM(CASE
                       WHEN f.home_club_id = c.id AND f.home_goals > f.away_goals THEN 3
                       WHEN f.away_club_id = c.id AND f.away_goals > f.home_goals THEN 3
                       WHEN f.home_goals = f.away_goals AND (f.home_club_id = c.id OR f.away_club_id = c.id) THEN 1
                       ELSE 0 END), 0) as points,
               COALESCE(SUM(CASE WHEN f.home_club_id = c.id THEN f.home_goals ELSE f.away_goals END), 0) as goals_for,
               COALESCE(SUM(CASE WHEN f.home_club_id = c.id THEN f.away_goals ELSE f.home_goals END), 0) as goals_against
        FROM clubs c
        LEFT JOIN fixtures f ON (f.home_club_id = c.id OR f.away_club_id = c.id)
        WHERE c.league_id = 1
        GROUP BY c.id
        ORDER BY points ASC, (goals_for - goals_against) ASC
        LIMIT 3
    """)
    relegated = cur.fetchall()

    # --- Compute table for League 2 (top 3) ---
    cur.execute("""
        SELECT c.id, c.name,
               COALESCE(SUM(CASE
                       WHEN f.home_club_id = c.id AND f.home_goals > f.away_goals THEN 3
                       WHEN f.away_club_id = c.id AND f.away_goals > f.home_goals THEN 3
                       WHEN f.home_goals = f.away_goals AND (f.home_club_id = c.id OR f.away_club_id = c.id) THEN 1
                       ELSE 0 END), 0) as points,
               COALESCE(SUM(CASE WHEN f.home_club_id = c.id THEN f.home_goals ELSE f.away_goals END), 0) as goals_for,
               COALESCE(SUM(CASE WHEN f.home_club_id = c.id THEN f.away_goals ELSE f.home_goals END), 0) as goals_against
        FROM clubs c
        LEFT JOIN fixtures f ON (f.home_club_id = c.id OR f.away_club_id = c.id)
        WHERE c.league_id = 2
        GROUP BY c.id
        ORDER BY points DESC, (goals_for - goals_against) DESC
        LIMIT 3
    """)
    promoted = cur.fetchall()

    # --- Swap leagues ---
    for cid, name, *_ in relegated:
        cur.execute("UPDATE clubs SET league_id = 2 WHERE id = ?", (cid,))
        cur.execute("UPDATE clubs_competition SET competition_id = 2 WHERE club_id = ? AND competition_id = 1", (cid,))
        print(f"⬇️ Relegated: {name} → Championship")

    for cid, name, *_ in promoted:
        cur.execute("UPDATE clubs SET league_id = 1 WHERE id = ?", (cid,))
        cur.execute("UPDATE clubs_competition SET competition_id = 1 WHERE club_id = ? AND competition_id = 2", (cid,))
        print(f"⬆️ Promoted: {name} → Premier League")

    conn.commit()
    conn.close()
    print("✅ Promotion/Relegation complete")


def game_loop():
    global GAME_DATE, LEAGUE_ATK_MEAN, LEAGUE_DEF_MEAN
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM global_val WHERE var_name='GAME_DATE'")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO global_val (var_name, value_date) VALUES (?, ?)", ("GAME_DATE", GAME_DATE.isoformat()))
        conn.commit()

    print(f"Game started on {GAME_DATE}. Press Enter to tick a day, or Q to quit.")
    while True:
        user_input = input("Press Enter to advance (1 day), or Q to quit: ").strip().lower()
        if user_input == "q":
            print("Quitting the game...")
            break

        # Update variable day values
        update_game_date_db()

        # 🔹 Simulamos fixtures del día
        simulate_fixtures_for_day(conn, GAME_DATE)

        # 🔹 End of season reset (31 de agosto)
        if GAME_DATE.month == 8 and GAME_DATE.day == 31:
            handle_promotion_relegation()
            print("📅 End of season! Resetting fixtures...")
            #depopulate_fixtures()
            populate_fixtures(1)
            populate_fixtures(2)
            cup_manage(3)
            #depopulate_match_scorers()
            LEAGUE_ATK_MEAN = None
            LEAGUE_DEF_MEAN = None
            print("✅ New season fixtures generated!")
            
            # Contract renewal
            renew_expired_contracts(conn, GAME_DATE)

        # 🔹 Cup progression (solo los viernes)
        if GAME_DATE.weekday() == 4:  # viernes
            cup_manage(3)
        #     cur.execute("""
        #         SELECT MIN(competition_round)
        #         FROM fixtures
        #         WHERE competition_id=3 AND played=0
        #     """)
        #     row = cur.fetchone()
        #     current_round = row[0] if row and row[0] is not None else 0

        #     winners = resolve_cup_round(conn, 3, current_round)
        #     if winners:
        #         advance_cup_round(conn, 3, GAME_DATE + timedelta(days=14), current_round, winners)

        # Finances updating
        if GAME_DATE.day == 1:  # viernes
            process_monthly_finances(conn, GAME_DATE)
            

            


        # 🔹 Avanzamos un día
        GAME_DATE = advance_game_day(GAME_DATE)
        cur.execute("UPDATE global_val SET value_date=? WHERE var_name='GAME_DATE'", (GAME_DATE.isoformat(),))
        conn.commit()
        
        
        
        

        # 🔹 Weekly updates (lunes)
        if GAME_DATE.weekday() == 0:
            update_players_in_db(conn, GAME_DATE)

        print(f"Game Date: {GAME_DATE}")

    conn.close()

def populate_all_players():
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
                    p, a, c = generate_player(position=pos, club_id=club_id, club_fame=club_fame)
                    # make sure date is ISO string for sqlite
                    p = list(p)
                    if isinstance(p[2], dt.date):
                        p[2] = p[2].isoformat()
                    players.append(tuple(p))
                    attrs.append(a)
                    contracts.append(c)

        return players, attrs, contracts

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
            first_name, last_name, date_of_birth, nationality,
            position, club_id, value
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, players)

    last_rowid = cur.execute("SELECT last_insert_rowid()").fetchone()[0]
    start_id = last_rowid - len(players) + 1
    player_ids = list(range(start_id, start_id + len(players)))

    # Insert attributes
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

    # Insert contracts
    players_contracts_with_ids = [(pid, *c) for pid, c in zip(player_ids, contracts)]
    cur.executemany("""
        INSERT INTO players_contract (
            player_id, club_id, contract_type, contract_start, contract_end, wage
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, players_contracts_with_ids)

    conn.commit()
    conn.close()
    print(f"✅ {len(players)} players generated with contracts")




def populate_competition_clubs():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Example: all clubs with league_id = 1 → Premier League (competition_id = 1)
    cur.execute("SELECT id FROM competitions WHERE name = 'Premier League'")
    premier_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM clubs WHERE league_id = 1")
    premier_clubs = [r[0] for r in cur.fetchall()]
    cur.executemany("""
        INSERT INTO clubs_competition (club_id, competition_id, is_active, round)
        VALUES (?, ?,1,1)
    """, [(cid, premier_id) for cid in premier_clubs])

    # Championship
    cur.execute("SELECT id FROM competitions WHERE name = 'Championship'")
    champ_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM clubs WHERE league_id = 2")
    champ_clubs = [r[0] for r in cur.fetchall()]
    cur.executemany("""
        INSERT INTO clubs_competition (club_id, competition_id, is_active, round)
        VALUES (?, ?,1,1)
    """, [(cid, champ_id) for cid in champ_clubs])

    # FA Cup (just drop both leagues in for now)
    cur.execute("SELECT id FROM competitions WHERE name = 'FA Cup'")
    fa_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM clubs")
    fa_clubs = [r[0] for r in cur.fetchall()]
    cur.executemany("""
        INSERT INTO clubs_competition (club_id, competition_id, is_active)
        VALUES (?, ?,1)
    """, [(cid, fa_id) for cid in fa_clubs])

    conn.commit()
    conn.close()
    print("✅ Clubs linked to competitions")





def cup_manage(competition_id: int):

    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()

    # # Reset cup at the beginning of the season
    if GAME_DATE.month == 8 and GAME_DATE.day == 31:
        cur.execute("""
            UPDATE clubs_competition
            SET is_active = TRUE, round = NULL
            WHERE competition_id = ?
        """, (competition_id,))
        conn.commit()
        print(f"🏆 Cup {competition_id} reset: all clubs set active again for new season.")
        


    # Check if there are matches to be played in the cup for the season
    cur.execute("""
        SELECT count(1)
        FROM fixtures f
        WHERE competition_id = ? and played = 0 and season = ?
    """, (competition_id,SEASON,))
    row = cur.fetchone()
    pending_matches = row[0] if row else 0
    print(f"Matches to be played: {pending_matches}")


    if pending_matches == 0:


        # Find total clubs and decide preliminary rounds
        cur.execute("""
            SELECT count(1)
            FROM clubs_competition
            WHERE competition_id = ? and is_active
        """, (competition_id,))
        row = cur.fetchone()
        total_clubs = row[0] if row else 0
        print(f"Total clubs: {total_clubs}")

        cur.execute("""
            SELECT IFNULL(MAX(competition_round), 0)+1
            FROM fixtures f
            WHERE competition_id = ? and season = ?
        """, (competition_id,SEASON,))
        row = cur.fetchone()
        next_cup_round = row[0] if row else 0
        if CUP_DEBUGGING:
            print(f"Season: {SEASON}")
            print(f"Next cup round to be played: {next_cup_round}")

        # We decide the winners from the previous round

        if next_cup_round > 1 or total_clubs in {2, 4, 8, 16, 32, 64, 128}:
            #print("Decide winners and create next round")

            # Decide winners
            cur.execute("""
                UPDATE clubs_competition SET is_active = FALSE WHERE club_id IN (
                       SELECT
                           CASE
                               WHEN goals_team1 > goals_team2
                               THEN team2_id
                               WHEN goals_team1 = goals_team2
                               THEN
                                   CASE
                                   WHEN goals_team1_pk > goals_team2_pk
                                   THEN team2_id
                                   ELSE team1_id
                                   END
                            ELSE team1_id
                            END AS loser_team_id
                       FROM (
                            SELECT
                            CASE
                                WHEN home_club_id < away_club_id THEN home_club_id
                                ELSE away_club_id
                            END AS team1_id,
                            CASE
                                WHEN home_club_id < away_club_id THEN away_club_id
                                ELSE home_club_id
                            END AS team2_id,
                            SUM(
                                CASE WHEN home_club_id < away_club_id THEN home_goals ELSE away_goals END
                            ) AS goals_team1,
                            SUM(
                                CASE WHEN home_club_id < away_club_id THEN away_goals ELSE home_goals END
                            ) AS goals_team2,

                            SUM(
                                CASE WHEN home_club_id < away_club_id THEN home_goals_pk ELSE away_goals_pk END
                            ) AS goals_team1_pk,
                            SUM(
                                CASE WHEN home_club_id < away_club_id THEN away_goals_pk ELSE home_goals_pk END
                            ) AS goals_team2_pk
                        FROM fixtures
                        WHERE competition_id = ?
                          AND competition_round = ?-1
                          AND played = 1
                          AND season = ?
                        GROUP BY team1_id, team2_id
                        ORDER BY team1_id
                       )
                ) AND competition_id = ?
            """, (competition_id, next_cup_round,SEASON, competition_id, ))

            conn.commit()

            # Seed all teams for the next round, waiting ones and last round winners
            cur.execute("""
                UPDATE clubs_competition
                SET round = ?
                WHERE club_id IN (
                    SELECT club_id FROM (
                        SELECT c.id AS club_id
                        FROM clubs c
                        JOIN clubs_competition cl ON cl.club_id = c.id
                        WHERE cl.competition_id = ? AND cl.is_active
                        AND (cl.round is NULL OR cl.round = ?-1)
                    )
                ) AND competition_id = ?
            """, (next_cup_round, competition_id,next_cup_round,competition_id,))

            conn.commit()

            # Create next round fixtures
            cur.execute("""
                INSERT INTO fixtures(home_club_id, away_club_id, fixture_date, competition_id, played, competition_round, season)
                SELECT home_id, away_id, match_date, ? as competition_id, 0 as played, ? as competition_round, ? FROM (
                               WITH ordered AS (
                                               SELECT
                                                               ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn,
                                                               COUNT(*) OVER () AS total,
                                                               c.id AS club_id,
                                                               c.name
                                               FROM clubs c
                                               JOIN clubs_competition cl ON cl.club_id = c.id
                                               WHERE cl.competition_id = ?
                                                 AND cl.is_active
                                                 AND cl.round = ?
                               )
                               SELECT
                                               t1.club_id AS home_id, --t1.name AS home_team,
                                               t2.club_id AS away_id, --t2.name AS away_team,
                               CASE
                                               WHEN t1.rn <= t1.total / 2
                                                               THEN date(gv.value_date, 'weekday 2', '+28 days')   -- Tuesday
                                               ELSE
                                                               date(gv.value_date, 'weekday 3', '+28 days')        -- Wednesday
                               END AS match_date
                               FROM ordered t1
                               JOIN ordered t2
                                 ON t2.rn = t1.rn + 1
                               JOIN global_val gv
                                 ON gv.var_name = 'GAME_DATE'
                               WHERE t1.rn % 2 = 1
                               ORDER BY t1.rn
                )
                final_query
            """, (competition_id, next_cup_round, SEASON, competition_id,next_cup_round,))

            conn.commit()

            #Show next cup draws


            #Calculate again team number after deciding winners
            cur.execute("""
                SELECT count(1)
                FROM clubs_competition
                WHERE competition_id = ? and is_active
            """, (competition_id,))
            row = cur.fetchone()
            total_clubs = row[0] if row else 0
            #print(f"Total clubs: {total_clubs}")

            if total_clubs >= 4:
                cur.execute("""
                    INSERT INTO fixtures(home_club_id, away_club_id, fixture_date, competition_id, played, competition_round, season)
                    SELECT f.away_club_id as home_club_id, f.home_club_id as away_club_id, date(f.fixture_date, '+7 days'), ? as competition_id, 0 as played, ? as competition_round, ?
                    FROM fixtures f WHERE competition_id = ? and played = 0 and competition_round = ?
                """, (competition_id, next_cup_round, SEASON, competition_id,next_cup_round,))

                conn.commit()
                #print(f"Second leg for round {next_cup_round} created")


        elif total_clubs not in {2, 4, 8, 16, 32, 64, 128} and total_clubs > 1:

            print("NOT correct number. Preliminary round needed")



            if total_clubs==8:
                round_name="Quarter Finals"
            elif total_clubs==4:
                round_name="Semifinals"
            elif total_clubs==2:
                round_name="FINAL"
            else:
                round_name=f"round {next_cup_round}"

            if total_clubs > 128:
                round_target=128
            elif total_clubs > 64:
                round_target=64
            elif total_clubs > 32:
                round_target=32
            elif total_clubs > 16:
                round_target=16
            elif total_clubs > 8:
                round_target=8
            elif total_clubs > 4:
                round_target=4
            elif total_clubs > 2:
                round_target=2

            preliminary_teams=2*(total_clubs-round_target)


            #We take the last n teams by fame and put them into first round
            cur.execute("""
                UPDATE clubs_competition
                SET round = ?
                WHERE club_id IN (
                    SELECT club_id FROM (
                        SELECT c.id AS club_id
                        FROM clubs c
                        JOIN clubs_competition cl ON cl.club_id = c.id
                        WHERE cl.competition_id = ? AND cl.is_active
                        ORDER BY fame ASC
                        LIMIT ?
                    )
                ) AND competition_id = ?
            """, (next_cup_round, competition_id,preliminary_teams,competition_id,))

            conn.commit()

            #First round

            cur.execute("""
                INSERT INTO fixtures(home_club_id, away_club_id, fixture_date, competition_id, played, competition_round, season)
                SELECT home_id, away_id, match_date, ? as competition_id, 0 as played, ? as competition_round, ? FROM (
                       WITH ordered AS (
                                       SELECT
                                        ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn,
                                        COUNT(*) OVER () AS total,
                                        c.id AS club_id,
                                        c.name
                                       FROM clubs c
                                       JOIN clubs_competition cl ON cl.club_id = c.id
                                       WHERE cl.competition_id = ?
                                         AND cl.is_active
                                         AND cl.round = ?
                       )
                       SELECT
                        t1.club_id AS home_id,
                        t2.club_id AS away_id,
                       CASE
                            WHEN t1.rn <= t1.total / 2
                                            THEN date(gv.value_date, 'weekday 2', '+28 days')   -- Tuesday
                            ELSE
                                            date(gv.value_date, 'weekday 3', '+28 days')        -- Wednesday
                       END AS match_date
                       FROM ordered t1
                       JOIN ordered t2
                         ON t2.rn = t1.rn + 1
                       JOIN global_val gv
                         ON gv.var_name = 'GAME_DATE'
                       WHERE t1.rn % 2 = 1
                       ORDER BY t1.rn
                )
                final_query
            """, (competition_id, next_cup_round, SEASON, competition_id,next_cup_round,))
            print(f"First leg for {round_name} created")
            conn.commit()

            #Second round, if there are more than 4 teams

            if total_clubs >= 4:
                cur.execute("""
                    INSERT INTO fixtures(home_club_id, away_club_id, fixture_date, competition_id, played, competition_round, season)
                    SELECT f.away_club_id as home_club_id, f.home_club_id as away_club_id, date(f.fixture_date, '+7 days'), ? as competition_id, 0 as played, ? as competition_round, ?
                    FROM fixtures f WHERE competition_id = ? and played = 0 and competition_round = ?
                """, (competition_id, next_cup_round, SEASON, competition_id,next_cup_round,))

                conn.commit()
                print(f"Second leg for {round_name} created")


    conn.close()




def populate_staff():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Get clubs with fame & competition country
    cur.execute("""
        SELECT c.id, c.fame, co.country
        FROM clubs c
        JOIN competitions co ON c.league_id = co.id
    """)
    clubs = cur.fetchall()

    if not clubs:
        print("⚠️ No clubs found.")
        conn.close()
        return

    # Roles setup: we expand with 3 Coaches
    base_roles = ["Manager", "Assistant Coach", "Physio", "Medical", "Scout"]
    staff_insert, contracts_insert, attrs_insert = [], [], []

    for club_id, club_fame, club_country in clubs:
        roles = base_roles + ["Coach"] * 3  # add 3 coaches per club

        for role in roles:
            faker = Faker()
            first_name = faker.first_name_male()
            last_name = faker.last_name()

            # Nationality (90% local, 10% foreign like players)
            if random.random() < 0.9:
                nationality = club_country
            else:
                nationality = random.choice(
                    ["England", "Argentina", "Spain", "Germany",
                     "Netherlands", "France", "Italy"]
                )

            # Rare second nationality (10%)
            second_nationality = faker.country() if random.random() < 0.1 else None

            # Age
            age = random.randint(30, 65)
            birth_year = GAME_DATE.year - age
            birth_month = random.randint(1, 12)
            birth_day = random.randint(1, 28)
            date_of_birth = date(birth_year, birth_month, birth_day)

            # --- Staff record ---
            staff_insert.append(
                (first_name, last_name, date_of_birth.isoformat(),
                 nationality, second_nationality, role, club_id)
            )

            # --- Contract (Sept 1 – Aug 31) ---
            start_year = GAME_DATE.year - random.randint(0, 2)
            contract_start = date(start_year, 9, 1)
            end_year = GAME_DATE.year + random.randint(1, 3)
            contract_end = date(end_year, 8, 31)

            # Wage logic
            if role == "Physio":
                wage = random.randint(50_000, 200_000)
            elif role == "Medical":
                wage = random.randint(80_000, 250_000)
            elif role == "Scout":
                wage = random.randint(100_000, 400_000)
            elif role == "Assistant Coach":
                wage = random.randint(200_000, 600_000)
            elif role == "Coach":
                wage = random.randint(150_000, 500_000)
            elif role == "Manager":
                fame_factor = (club_fame / 2000.0)  # ~0.5 → 2.0
                wage = int(random.randint(500_000, 2_000_000) * fame_factor)
            else:
                wage = random.randint(50_000, 300_000)

            contracts_insert.append(
                (club_id, "Professional", contract_start.isoformat(),
                 contract_end.isoformat(), wage)
            )

            # --- Attributes (1–2000) ---
            attrs_insert.append(generate_staff_attributes(role))

    # --- Insert staff ---
    cur.executemany("""
        INSERT INTO staff (
            first_name, last_name, date_of_birth, nationality, second_nationality,
            role, club_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, staff_insert)

    last_rowid = cur.execute("SELECT last_insert_rowid()").fetchone()[0]
    start_id = last_rowid - len(staff_insert) + 1
    staff_ids = list(range(start_id, start_id + len(staff_insert)))

    # --- Insert contracts ---
    contracts_with_ids = [(sid, *c) for sid, c in zip(staff_ids, contracts_insert)]
    cur.executemany("""
        INSERT INTO staff_contract (
            staff_id, club_id, contract_type, contract_start, contract_end, wage
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, contracts_with_ids)

    # --- Insert attributes ---
    attrs_with_ids = [(sid, *a) for sid, a in zip(staff_ids, attrs_insert)]
    cur.executemany("""
        INSERT INTO staff_attr (
            staff_id, at_goalkeeping, at_tackling, at_passing,
            at_shooting, at_physio, at_medical, at_scouting
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, attrs_with_ids)

    conn.commit()
    conn.close()
    print(f"✅ {len(staff_insert)} staff generated with contracts and attributes")

def generate_staff_attributes(role: str):
    def rand(low, high): return random.randint(low, high)

    if role == "Manager":
        return (
            rand(800, 1600),  # goalkeeping
            rand(800, 1600),  # tackling
            rand(1000, 1800), # passing
            rand(800, 1600),  # shooting
            rand(200, 800),   # physio
            rand(200, 800),   # medical
            rand(1000, 2000)  # scouting
        )
    elif role == "Assistant Coach":
        return (
            rand(600, 1500),
            rand(600, 1500),
            rand(800, 1600),
            rand(600, 1500),
            rand(200, 600),
            rand(200, 600),
            rand(800, 1600)
        )
    elif role == "Coach":
        return (
            rand(1000, 2000),
            rand(1000, 2000),
            rand(1000, 2000),
            rand(1000, 2000),
            rand(200, 600),
            rand(200, 600),
            rand(200, 800)
        )
    elif role == "Physio":
        return (
            rand(200, 600),
            rand(200, 600),
            rand(200, 600),
            rand(200, 600),
            rand(1200, 2000),  # physio very high
            rand(200, 600),
            rand(200, 600)
        )
    elif role == "Medical":
        return (
            rand(200, 600),
            rand(200, 600),
            rand(200, 600),
            rand(200, 600),
            rand(200, 600),
            rand(1200, 2000),  # medical very high
            rand(200, 600)
        )
    elif role == "Scout":
        return (
            rand(200, 600),
            rand(200, 600),
            rand(400, 1200),
            rand(200, 600),
            rand(200, 600),
            rand(200, 600),
            rand(1200, 2000)  # scouting very high
        )
    else:
        # generic fallback
        return tuple(rand(200, 2000) for _ in range(7))



def compute_staff_multipliers(cur, club_id):
    cur.execute("""
        SELECT s.role, sa.at_goalkeeping, sa.at_tackling, sa.at_passing,
               sa.at_shooting, sa.at_physio, sa.at_medical, sa.at_scouting
        FROM staff s
        JOIN staff_attr sa ON sa.staff_id = s.id
        WHERE s.club_id = ?
    """, (club_id,))
    rows = cur.fetchall()

    if not rows:
        return { "gk": 1.0, "def": 1.0, "pass": 1.0, "shoot": 1.0, "fitness": 1.0 }

    gk = sum(r[1] for r in rows) / len(rows)
    tackling = sum(r[2] for r in rows) / len(rows)
    passing = sum(r[3] for r in rows) / len(rows)
    shooting = sum(r[4] for r in rows) / len(rows)
    physio = sum(r[5] for r in rows) / len(rows)
    medical = sum(r[6] for r in rows) / len(rows)

    def scale(x):  # normalize 0.9–1.1 around ~1000
        return clamp(0.9 + (x / 2000.0) * 0.4, 0.8, 1.2)

    return {
        "gk": scale(gk),
        "def": scale(tackling),
        "pass": scale(passing),
        "shoot": scale(shooting),
        "fitness": scale((physio + medical) / 2),
    }




# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    # Uncomment to hard reset DB schema & base tables:
    init_db()

    # If you reset DB, (optionally) load clubs from CSV:
    depopulate_clubs()
    populate_clubs()
    initialize_club_balances()

    populate_competition_clubs()

    update_game_date_db()           # keep GAME_DATE in DB in sync

    # Fresh players & fixtures each run (like your previous workflow)
    depopulate_players()
    #populate_400_players()
    populate_all_players()

    depopulate_fixtures()
    populate_fixtures(1)
    populate_fixtures(2)

    cup_manage(3)

    populate_staff()


    # FA Cup: usar semillado con fama para byes + prelim
    #conn = sqlite3.connect(DB_PATH)
    #seed_cup_round0_two_tier(conn, 3, GAME_DATE)  # competition_id=3 (FA Cup)
    #conn.close()

    depopulate_match_scorers()

    # Optional: mini-situations system
    # init_db_possib()
    # clean_player_situ()
    # run_game(16)

    # Kick off loop
    game_loop()

    # Debug helpers:
    # print_table("players_attr")
    # test_regen_creation(3, position="GK")
