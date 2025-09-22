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
    DROP TABLE IF EXISTS players_attr;
    DROP TABLE IF EXISTS player_situ;
    DROP TABLE IF EXISTS consequences;
    DROP TABLE IF EXISTS options_conseq;
    DROP TABLE IF EXISTS options;
    DROP TABLE IF EXISTS situ_options;
    DROP TABLE IF EXISTS situations;
    DROP TABLE IF EXISTS staff;
    DROP TABLE IF EXISTS clubs;
    DROP TABLE IF EXISTS players;
    DROP TABLE IF EXISTS global_val;
    DROP TABLE IF EXISTS match_scorers;
    DROP TABLE IF EXISTS competitions;
    DROP TABLE IF EXISTS clubs_competition;
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
        FOREIGN KEY (league_id) REFERENCES competitions(id)
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
        position TEXT,
        club_id INTEGER,
        value INTEGER,
        wage INTEGER,
        contract_until DATE,
        is_retired BOOLEAN DEFAULT FALSE,
        FOREIGN KEY (club_id) REFERENCES clubs(id)
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
        role TEXT,
        club_id INTEGER,
        wage INTEGER,
        contract_until DATE,
        FOREIGN KEY (club_id) REFERENCES clubs(id)
    );

    CREATE TABLE fixtures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date DATE NOT NULL,
        home_club_id INTEGER,
        away_club_id INTEGER,
        competition_id INTEGER,
        home_goals INTEGER,
        away_goals INTEGER,
        played BOOLEAN DEFAULT 0,
        competition_round INTEGER,
        FOREIGN KEY (home_club_id) REFERENCES clubs(id),
        FOREIGN KEY (away_club_id) REFERENCES clubs(id)
    );

    CREATE TABLE match_scorers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL,
        fixture_id INTEGER NOT NULL,
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

    # Remove old fixtures for this competition
    cur.execute("DELETE FROM fixtures WHERE competition_id = ?", (competition_id,))

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
                fixtures_to_insert.append((sat.isoformat(), home, away, competition_id, None, None, 0, 1))
            for home, away in matches[k:]:
                if home is None or away is None:
                    continue
                fixtures_to_insert.append((sun.isoformat(), home, away, competition_id, None, None, 0, 1))

    elif is_cup:
        # -------------------
        # Cup: Fame-based prelims
        # -------------------
        cur.execute("""
            SELECT c.id, c.fame
            FROM clubs_competition cc
            JOIN clubs c ON cc.club_id = c.id
            WHERE cc.competition_id=? AND cc.is_active=1
        """, (competition_id,))
        clubs = cur.fetchall()
        n = len(clubs)
        P = 1 << (n.bit_length() - 1)  # largest power of 2 <= n
        prelim_ties = n - P
        byes_count = 2 * P - n

        # Sort by fame
        clubs_sorted = sorted(clubs, key=lambda x: x[1], reverse=True)
        byes = [cid for cid, _ in clubs_sorted[:byes_count]]
        prelim = [cid for cid, _ in clubs_sorted[byes_count:]]

        # Update competition rounds
        cur.executemany("UPDATE clubs_competition SET round=1 WHERE competition_id=? AND club_id=?", 
                        [(competition_id, cid) for cid in byes])
        cur.executemany("UPDATE clubs_competition SET round=0 WHERE competition_id=? AND club_id=?", 
                        [(competition_id, cid) for cid in prelim])

        start_monday = GAME_DATE + timedelta(days=(0 - GAME_DATE.weekday()) % 7)
        random.shuffle(prelim)
        pairs = [(prelim[i], prelim[i+1]) for i in range(0, len(prelim), 2)]

        for i, (home, away) in enumerate(pairs):
            # First leg
            tue1 = start_monday + timedelta(days=1)
            wed1 = start_monday + timedelta(days=2)
            leg1 = tue1 if i % 2 == 0 else wed1
            fixtures_to_insert.append((leg1.isoformat(), home, away, competition_id, None, None, 0, 0))

            # Second leg (two weeks later)
            tue2 = start_monday + timedelta(days=15)
            wed2 = start_monday + timedelta(days=16)
            leg2 = tue2 if i % 2 == 0 else wed2
            fixtures_to_insert.append((leg2.isoformat(), away, home, competition_id, None, None, 0, 0))

        print(f"🏆 Cup {competition_id}: {n} clubs → {len(pairs)} prelim ties, {byes_count} byes.")

    # Save fixtures
    if fixtures_to_insert:
        cur.executemany("""
            INSERT INTO fixtures (date, home_club_id, away_club_id,
                                  competition_id, home_goals, away_goals, played, competition_round)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, fixtures_to_insert)

    conn.commit()
    conn.close()
    print(f"✅ Fixtures populated for competition {competition_id}")


def populate_cup_round(competition_id: int, club_ids: list, start_date: date, current_round: int = 1):
    """
    Generate one knockout round for a cup (two-legged ties).
    - First leg: Tue/Wed of start_date week
    - Second leg: Tue/Wed two weeks later
    """
    random.shuffle(club_ids)

    if len(club_ids) % 2 == 1:
        bye = club_ids.pop()
        print(f"⚠️ Club {bye} advances by bye in competition {competition_id}")
        return [], [bye]

    fixtures = []
    winners_auto = []

    for i in range(0, len(club_ids), 2):
        home, away = club_ids[i], club_ids[i+1]

        # First leg
        tue1 = start_date + timedelta(days=1)   # Tuesday
        wed1 = start_date + timedelta(days=2)   # Wednesday
        leg1_date = tue1 if (i//2) % 2 == 0 else wed1
        fixtures.append((leg1_date.isoformat(), home, away, competition_id, None, None, 0, current_round))

        # Second leg (two weeks later)
        tue2 = start_date + timedelta(days=15)  # Tuesday + 2 weeks
        wed2 = start_date + timedelta(days=16)  # Wednesday + 2 weeks
        leg2_date = tue2 if (i//2) % 2 == 0 else wed2
        fixtures.append((leg2_date.isoformat(), away, home, competition_id, None, None, 0, current_round))

    return fixtures, winners_auto



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
    fame_norm = 0.8 + (club_fame / 2000.0) * 0.4   # range ≈ 0.8 → 1.2

    # --- Market Value ---
    base_value = random.randint(100_000, 50_000_000)
    value = int(base_value * fame_norm)

    # Age-related value decline
    if age > 31:
        value = int(value * (0.2 if age >= 34 else 0.4))

    # Wage (based on value)
    wage = max(150_000, min(int(value * 0.05 * random.uniform(0.8, 1.2)), 20_000_000))

    # Contract
    contract_until = faker.date_between(start_date="+1y", end_date="+5y").isoformat()

    # --- Abilities ---
    pot_ability = random_potential()

    # Fame scaling for potential (big clubs attract better talent)
    pot_ability = int(pot_ability * fame_norm)
    pot_ability = min(2000, max(800, pot_ability))  # clamp between 800 and 2000

    # Current ability depends on age + fame
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

    # --- Attribute distribution ---
    attrs = distribute_attributes(curr_ability, pot_ability, position)

    # --- Pack results ---
    player_attr = tuple(attrs[a] for a in attrs) + (curr_ability, pot_ability)
    player = (first_name, last_name, date_of_birth, nationality,
              position, club_id, value, wage, contract_until)

    return player, player_attr


# -----------------------------
# Players table operations
# -----------------------------
def populate_400_players():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, fame FROM clubs WHERE league_id = 1")
    clubs = cur.fetchall()
    if len(clubs) != 20:
        print(f"⚠️ Expected 20 Premier League clubs, found {len(clubs)}")
        conn.close()
        return

    position_counts = {"GK": 2,"CB": 3,"RB": 2,"LB": 2,"CDM": 1,"CM": 2,"CAM": 1,"RM": 1,"LM": 1,"RW": 1,"LW": 1,"ST": 3}
    players, players_attr = [], []

    for club_id, club_fame in clubs:
        for position, count in position_counts.items():
            for _ in range(count):
                player, attr = generate_player(position=position, club_id=club_id, club_fame=club_fame)
                # convert date to iso string for sqlite stability
                p = list(player)
                if isinstance(p[2], dt.date):
                    p[2] = p[2].isoformat()
                players.append(tuple(p))
                players_attr.append(attr)

    cur.executemany("""
        INSERT INTO players (
            first_name, last_name, date_of_birth, nationality,
            position, club_id, value, wage, contract_until
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, players)

    last_rowid = cur.execute("SELECT last_insert_rowid()").fetchone()[0]
    start_id = last_rowid - len(players) + 1
    player_ids = list(range(start_id, start_id + len(players)))

    players_attr_with_ids = [(pid, *attr) for pid, attr in zip(player_ids, players_attr)]
    cur.executemany("""
        INSERT INTO players_attr (
            player_id,
            at_luck, at_selfcont, at_honour, at_crazyness, at_working, at_sexatract, at_friendship,
            at_speed, at_dribbling, at_goalkeeping, at_defending, at_passing, at_scoring,
            at_happiness, at_confidence, at_hope,
            at_curr_ability, at_pot_ability
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, players_attr_with_ids)

    conn.commit()
    conn.close()
    print("✅ 400 players generated and inserted.")

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

# -----------------------------
# Weekly progression
# -----------------------------
def update_players_in_db(conn, game_date):
    """
    Update all players' attributes based on age, potential, and development curve.
    Retire >35, spawn youth replacements.
    Attributes are proportional to curr_ability & position weights.
    """
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

    for player in players:
        player_id, birth_date, pos, club_id, curr_ability, pot_ability = player
        age = calculate_age(birth_date, game_date)

        # --- Retirement ---
        if age > 35:
            cur.execute("UPDATE players SET is_retired=1, value=0 WHERE id=?", (player_id,))
            cur.execute("SELECT fame FROM clubs WHERE id=?", (club_id,))
            row = cur.fetchone()
            club_fame = row[0] if row else 1000

            # Spawn regen (youth)
            youth, youth_attr = generate_player(position=pos, club_id=club_id,
                                                club_fame=club_fame, force_youth=True)
            cur.execute("""
                INSERT INTO players (
                    first_name, last_name, date_of_birth, nationality,
                    position, club_id, value, wage, contract_until
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            continue

        # --- Development / Decline ---
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

        new_curr_ability = max(100, min(int(curr_ability + growth), pot_ability, 2000))

        # --- Redistribute attributes ---
        attrs = distribute_attributes(new_curr_ability, pot_ability, pos)

        # --- Market value ---
        cur.execute("SELECT fame FROM clubs WHERE id=?", (club_id,))
        row = cur.fetchone()
        club_fame = row[0] if row else 1000
        value = calculate_player_value(new_curr_ability, pot_ability, age, fame=club_fame)

        # --- Save updates ---
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
        ORDER BY date DESC
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
            SELECT f.id, f.home_club_id, hc.name, f.away_club_id, ac.name, f.competition_id, comp.name
            FROM fixtures f
            JOIN clubs hc ON hc.id = f.home_club_id
            JOIN clubs ac ON ac.id = f.away_club_id
            JOIN competitions comp ON comp.id = f.competition_id
            WHERE f.date = ? AND f.played = 0
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

    for fixture_id, home_id, home_name, away_id, away_name, league_id, league_name in fixtures:
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

        cur.execute("UPDATE fixtures SET home_goals=?, away_goals=?, played=1 WHERE id=?",
                    (home_goals, away_goals, fixture_id))
        if scorers:
            cur.executemany("INSERT INTO match_scorers (player_id, fixture_id) VALUES (?, ?)", scorers)

        print(f"⚽ [{league_name}] {home_name} {home_goals} - {away_goals} {away_name}")

        if names:
            print("   Scorers:", ", ".join(names))

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
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE global_val SET value_date=? WHERE var_name='GAME_DATE'", (GAME_DATE.isoformat(),))
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
        print(f"⬇️ Relegated: {name} → Championship")

    for cid, name, *_ in promoted:
        cur.execute("UPDATE clubs SET league_id = 1 WHERE id = ?", (cid,))
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

        # 🔹 Simulamos fixtures del día
        simulate_fixtures_for_day(conn, GAME_DATE)

        # 🔹 End of season reset (31 de agosto)
        if GAME_DATE.month == 8 and GAME_DATE.day == 31:
            handle_promotion_relegation()
            print("📅 End of season! Resetting fixtures...")
            depopulate_fixtures()
            populate_fixtures(1)
            populate_fixtures(2)
            depopulate_match_scorers()
            LEAGUE_ATK_MEAN = None
            LEAGUE_DEF_MEAN = None
            print("✅ New season fixtures generated!")

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


        # 🔹 Avanzamos un día
        GAME_DATE = advance_game_day(GAME_DATE)
        cur.execute("UPDATE global_val SET value_date=? WHERE var_name='GAME_DATE'", (GAME_DATE.isoformat(),))
        conn.commit()

        # 🔹 Weekly updates (lunes)
        if GAME_DATE.weekday() == 0:
            update_players_in_db(conn, GAME_DATE)

        print(f"Game Date: {GAME_DATE}")

    conn.close()


def populate_800_players():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    def create_players_for_league(league_id, expected_clubs=20):
        cur.execute("SELECT id, fame FROM clubs WHERE league_id = ?", (league_id,))
        clubs = cur.fetchall()
        if not clubs:
            print(f"⚠️ No clubs found for league {league_id}")
            return [], []

        position_counts = {
            "GK": 2, "CB": 3, "RB": 2, "LB": 2,
            "CDM": 1, "CM": 2, "CAM": 1,
            "RM": 1, "LM": 1, "RW": 1, "LW": 1,
            "ST": 3
        }

        players, attrs = [], []
        for club_id, club_fame in clubs:
            for pos, count in position_counts.items():
                for _ in range(count):
                    p, a = generate_player(position=pos, club_id=club_id, club_fame=club_fame)
                    # make sure date is ISO string for sqlite
                    p = list(p)
                    if isinstance(p[2], dt.date):
                        p[2] = p[2].isoformat()
                    players.append(tuple(p))
                    attrs.append(a)

        return players, attrs

    # --- League 1 ---
    players1, attrs1 = create_players_for_league(1)
    # --- League 2 ---
    players2, attrs2 = create_players_for_league(2)

    players = players1 + players2
    attrs = attrs1 + attrs2

    if not players:
        print("⚠️ No players generated.")
        conn.close()
        return

    # Insert players
    cur.executemany("""
        INSERT INTO players (
            first_name, last_name, date_of_birth, nationality,
            position, club_id, value, wage, contract_until
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, players)

    last_rowid = cur.execute("SELECT last_insert_rowid()").fetchone()[0]
    start_id = last_rowid - len(players) + 1
    player_ids = list(range(start_id, start_id + len(players)))

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

    conn.commit()
    conn.close()
    print(f"✅ {len(players)} players generated and inserted across leagues 1 & 2")

def populate_competition_clubs():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Example: all clubs with league_id = 1 → Premier League (competition_id = 1)
    cur.execute("SELECT id FROM competitions WHERE name = 'Premier League'")
    premier_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM clubs WHERE league_id = 1")
    premier_clubs = [r[0] for r in cur.fetchall()]
    cur.executemany("""
        INSERT INTO clubs_competition (club_id, competition_id)
        VALUES (?, ?)
    """, [(cid, premier_id) for cid in premier_clubs])

    # Championship
    cur.execute("SELECT id FROM competitions WHERE name = 'Championship'")
    champ_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM clubs WHERE league_id = 2")
    champ_clubs = [r[0] for r in cur.fetchall()]
    cur.executemany("""
        INSERT INTO clubs_competition (club_id, competition_id)
        VALUES (?, ?)
    """, [(cid, champ_id) for cid in champ_clubs])

    # FA Cup (just drop both leagues in for now)
    cur.execute("SELECT id FROM competitions WHERE name = 'FA Cup'")
    fa_id = cur.fetchone()[0]

    cur.execute("SELECT id FROM clubs")
    fa_clubs = [r[0] for r in cur.fetchall()]
    cur.executemany("""
        INSERT INTO clubs_competition (club_id, competition_id)
        VALUES (?, ?)
    """, [(cid, fa_id) for cid in fa_clubs])

    conn.commit()
    conn.close()
    print("✅ Clubs linked to competitions")

def resolve_cup_round(conn, competition_id: int, current_round: int):
    cur = conn.cursor()

    # Fetch all matches of this round
    cur.execute("""
        SELECT home_club_id, away_club_id, home_goals, away_goals
        FROM fixtures
        WHERE competition_id = ? AND competition_round = ? AND played = 1
        ORDER BY date
    """, (competition_id, current_round))
    matches = cur.fetchall()

    # Check if all games for this round are finished
    cur.execute("""
        SELECT COUNT(*) FROM fixtures
        WHERE competition_id = ? AND competition_round = ?
    """, (competition_id, current_round))
    total_matches = cur.fetchone()[0]

    if len(matches) < total_matches:
        return []  # round still ongoing

    # --- Aggregate results ---
    ties = {}
    for home, away, hg, ag in matches:
        key = tuple(sorted((home, away)))
        ties.setdefault(key, []).append((home, away, hg, ag))

    winners = []
    for key, legs in ties.items():
        agg = {key[0]: 0, key[1]: 0}
        for home, away, hg, ag in legs:
            agg[home] += hg or 0
            agg[away] += ag or 0

        if agg[key[0]] > agg[key[1]]:
            winner, loser = key[0], key[1]
        elif agg[key[1]] > agg[key[0]]:
            winner, loser = key[1], key[0]
        else:  # tie → penalties
            winner = random.choice(key)
            loser = key[1] if winner == key[0] else key[0]

        winners.append(winner)
        cur.execute("UPDATE clubs_competition SET is_active=0 WHERE club_id=? AND competition_id=?",
                    (loser, competition_id))

    conn.commit()

    # --- Special case: Preliminary Round 0 ---
    if current_round == 0:
        # Collect PL clubs (waiting byes)
        cur.execute("""
            SELECT c.id FROM clubs c
            JOIN clubs_competition cc ON cc.club_id = c.id
            WHERE cc.competition_id=? AND cc.is_active=1 AND cc.round=1
        """, (competition_id,))
        pl = [r[0] for r in cur.fetchall()]

        # Get top 12 Championship by fame
        cur.execute("""
            SELECT c.id FROM clubs c
            JOIN clubs_competition cc ON cc.club_id = c.id
            WHERE cc.competition_id=? AND cc.is_active=1 AND cc.round=0
            ORDER BY c.fame DESC
            LIMIT 12
        """, (competition_id,))
        ch_byes = [r[0] for r in cur.fetchall()]

        # Move Round 0 winners + CH byes → Round 1
        for cid in winners + ch_byes:
            cur.execute("UPDATE clubs_competition SET round=1 WHERE club_id=? AND competition_id=?",
                        (cid, competition_id))

        all_round1 = winners + ch_byes + pl
        assert len(all_round1) == 32, f"Expected 32 teams in Round 1, got {len(all_round1)}"

        # Get last match date of Round 0 to schedule Round 1
        cur.execute("SELECT MAX(date) FROM fixtures WHERE competition_id=? AND competition_round=0",
                    (competition_id,))
        last_date = date.fromisoformat(cur.fetchone()[0])

        # Generate Round 1 fixtures
        next_round = 1
        cup_fixtures, _ = populate_cup_round(competition_id, all_round1, last_date + timedelta(days=7), next_round)
        cur.executemany("""
            INSERT INTO fixtures (date, home_club_id, away_club_id,
                                  competition_id, home_goals, away_goals, played, competition_round)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, cup_fixtures)

        conn.commit()
        print(f"✅ Round 1 created with {len(all_round1)} teams (20 PL + 12 CH)")
        return winners

    # --- Normal rounds (1, 2, …) ---
    next_round = current_round + 1
    if len(winners) < 2:
        if winners:
            cur.execute("SELECT name FROM clubs WHERE id=?", (winners[0],))
            champ = cur.fetchone()[0]
            print(f"🏆 {champ} wins Cup {competition_id}!")
        return winners

    # Schedule next knockout round
    cur.execute("SELECT MAX(date) FROM fixtures WHERE competition_id=? AND competition_round=?",
                (competition_id, current_round))
    last_date = date.fromisoformat(cur.fetchone()[0])

    cup_fixtures, _ = populate_cup_round(competition_id, winners, last_date + timedelta(days=7), next_round)
    cur.executemany("""
        INSERT INTO fixtures (date, home_club_id, away_club_id,
                              competition_id, home_goals, away_goals, played, competition_round)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, cup_fixtures)

    conn.commit()
    print(f"✅ Round {next_round} created with {len(winners)} teams")
    return winners


def advance_cup_round(conn, competition_id: int, start_date: date, current_round: int, winners: list):
    cur = conn.cursor()

    # If fewer than 2 teams, tournament is over
    if len(winners) < 2:
        if winners:
            cur.execute("SELECT name FROM clubs WHERE id=?", (winners[0],))
            champ = cur.fetchone()[0]
            print(f"🏆 {champ} wins Cup {competition_id}!")
        return

    # Move to next round
    next_round = 1 if current_round == 0 else current_round + 1

    # Mark winners in clubs_competition as next_round
    for cid in winners:
        cur.execute("""
            UPDATE clubs_competition
            SET round=?
            WHERE club_id=? AND competition_id=?
        """, (next_round, cid, competition_id))

    # Shuffle and generate fixtures
    random.shuffle(winners)
    cup_fixtures, _ = populate_cup_round(competition_id, winners, start_date, next_round)

    cur.executemany("""
        INSERT INTO fixtures (date, home_club_id, away_club_id,
                              competition_id, home_goals, away_goals, played, competition_round)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, cup_fixtures)

    conn.commit()

    stage = {32: "Round of 32", 16: "Round of 16", 8: "Quarterfinals",
             4: "Semifinals", 2: "Final"}.get(len(winners), f"Round {next_round}")

    print(f"✅ {stage} created for cup {competition_id} with {len(winners)//2} ties")

def seed_cup_round0_two_tier(conn, competition_id: int, season_start_date: date):
    """
    Seed FA Cup:
    - PL clubs (league_id=1) skip to Round 1
    - Championship clubs (league_id=2) play Round 0 prelims (24 → 12 winners)
    """
    cur = conn.cursor()

    # Get clubs entered
    cur.execute("""
        SELECT c.id, c.league_id
        FROM clubs_competition cc
        JOIN clubs c ON c.id = cc.club_id
        WHERE cc.competition_id=? AND cc.is_active=1
    """, (competition_id,))
    rows = cur.fetchall()

    pl = [cid for cid, lid in rows if lid == 1]
    ch = [cid for cid, lid in rows if lid == 2]

    # Sanity check
    assert len(pl) == 20, f"Expected 20 PL clubs, got {len(pl)}"
    assert len(ch) == 24, f"Expected 24 CH clubs, got {len(ch)}"

    # Mark PL → Round 1
    if pl:
        cur.executemany("""
            UPDATE clubs_competition SET round=1
            WHERE competition_id=? AND club_id=?
        """, [(competition_id, cid) for cid in pl])

    # Mark CH → Round 0
    if ch:
        cur.executemany("""
            UPDATE clubs_competition SET round=0
            WHERE competition_id=? AND club_id=?
        """, [(competition_id, cid) for cid in ch])

    # Build Round 0 fixtures (24 clubs → 12 ties, two legs)
    random.shuffle(ch)
    assert len(ch) % 2 == 0, "Championship clubs count must be even to form pairs"
    pairs = [(ch[i], ch[i+1]) for i in range(0, len(ch), 2)]
    assert len(pairs) == 12, f"Expected 12 prelim ties, got {len(pairs)}"

    fixtures = []
    start_monday = season_start_date + timedelta(days=(0 - season_start_date.weekday()) % 7)

    for i, (home, away) in enumerate(pairs):
        tue1 = start_monday + timedelta(days=1)
        wed1 = start_monday + timedelta(days=2)
        leg1 = tue1 if i % 2 == 0 else wed1

        tue2 = start_monday + timedelta(days=15)
        wed2 = start_monday + timedelta(days=16)
        leg2 = tue2 if i % 2 == 0 else wed2

        fixtures.append((leg1.isoformat(), home, away, competition_id, None, None, 0, 0))
        fixtures.append((leg2.isoformat(), away, home, competition_id, None, None, 0, 0))

    cur.executemany("""
        INSERT INTO fixtures (date, home_club_id, away_club_id,
                              competition_id, home_goals, away_goals, played, competition_round)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, fixtures)

    conn.commit()
    print(
        f"🏆 Cup {competition_id} seeded:\n"
        f"   {len(pl)} PL clubs waiting in Round 1\n"
        f"   {len(ch)} CH clubs in Round 0 → {len(pairs)} ties created (two legs each = {len(fixtures)} fixtures)"
    )

def resolve_cup_round(conn, competition_id: int, current_round: int):
    cur = conn.cursor()

    # Fetch played matches
    cur.execute("""
        SELECT home_club_id, away_club_id, home_goals, away_goals
        FROM fixtures
        WHERE competition_id=? AND competition_round=? AND played=1
        ORDER BY date
    """, (competition_id, current_round))
    matches = cur.fetchall()

    # Check if all fixtures are done
    cur.execute("SELECT COUNT(*) FROM fixtures WHERE competition_id=? AND competition_round=?",
                (competition_id, current_round))
    total_matches = cur.fetchone()[0]
    if len(matches) < total_matches:
        return []  # round still in progress

    # Aggregate winners
    ties, winners = {}, []
    for home, away, hg, ag in matches:
        key = tuple(sorted((home, away)))
        ties.setdefault(key, []).append((home, away, hg, ag))

    for key, legs in ties.items():
        agg = {key[0]: 0, key[1]: 0}
        for home, away, hg, ag in legs:
            agg[home] += hg or 0
            agg[away] += ag or 0

        if agg[key[0]] > agg[key[1]]:
            winner, loser = key[0], key[1]
        elif agg[key[1]] > agg[key[0]]:
            winner, loser = key[1], key[0]
        else:  # penalties
            winner = random.choice(key)
            loser = key[1] if winner == key[0] else key[0]

        winners.append(winner)
        cur.execute("UPDATE clubs_competition SET is_active=0 WHERE club_id=? AND competition_id=?",
                    (loser, competition_id))

    winners = list(set(winners))  # deduplicate

    # Special: Round 0 → Round 1
    if current_round == 0:
        assert len(winners) == 12, f"Expected 12 CH winners, got {len(winners)}"

        cur.execute("""
            SELECT c.id FROM clubs c
            JOIN clubs_competition cc ON c.id=cc.club_id
            WHERE cc.competition_id=? AND cc.is_active=1
              AND c.league_id=1
        """, (competition_id,))
        pl = [r[0] for r in cur.fetchall()]
        assert len(pl) == 20, f"Expected 20 PL clubs, got {len(pl)}"

        # Promote winners
        for cid in winners:
            cur.execute("UPDATE clubs_competition SET round=1 WHERE club_id=? AND competition_id=?",
                        (cid, competition_id))

        all_round1 = pl + winners
        assert len(all_round1) == 32, f"Expected 32 in Round 1, got {len(all_round1)}"

        # Schedule Round 1 fixtures
        cur.execute("SELECT MAX(date) FROM fixtures WHERE competition_id=? AND competition_round=0",
                    (competition_id,))
        last_date = date.fromisoformat(cur.fetchone()[0])

        next_round = 1
        cup_fixtures, _ = populate_cup_round(competition_id, all_round1, last_date + timedelta(days=7), next_round)
        cur.executemany("""
            INSERT INTO fixtures (date, home_club_id, away_club_id,
                                  competition_id, home_goals, away_goals, played, competition_round)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, cup_fixtures)

        conn.commit()
        print(f"✅ Round 1 created with {len(all_round1)} teams (20 PL + 12 CH winners)")
        return winners

    # Normal rounds...
    # (unchanged from your current version)


def get_current_cup_round(conn, competition_id: int) -> int:
    cur = conn.cursor()
    cur.execute("""
        SELECT MIN(competition_round)
        FROM fixtures
        WHERE competition_id = ? AND played = 0
    """, (competition_id,))
    row = cur.fetchone()
    if row and row[0] is not None:
        return row[0]  # Hay fixtures pendientes → ronda actual
    return None  # Ninguna ronda pendiente



def cup_manage(competition_id: int):

    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()
    
    
    # Check if there are matches to be played in the cup
    cur.execute("""
        SELECT count(1)
        FROM fixtures
        WHERE competition_id = ? and played = 0
    """, (competition_id,))
    row = cur.fetchone()
    pending_matches = row[0] if row else 0
    print(f"Matches to be played: {pending_matches}")
    
    
    if pending_matches > 0:
        print(f"Pending matches to be played. Waiting to finish them.")
    else:
                
        
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
            FROM fixtures
            WHERE competition_id = ?
        """, (competition_id,))
        row = cur.fetchone()
        next_cup_round = row[0] if row else 0
        print(f"Next cup round to be played: {next_cup_round}")
        
        # We decide the winners from the previous round
        
        if next_cup_round > 1:
            print("Decide winners and create next round")
            
            # Decide winners
            cur.execute("""
                UPDATE clubs_competition SET is_active = FALSE WHERE club_id IN (
                	SELECT 
                	CASE WHEN goals_team1 > goals_team2 THEN team2_id ELSE team1_id END AS loser_team_id
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
                			) AS goals_team2
                		FROM fixtures
                		WHERE competition_id = ?
                		  AND competition_round = ?-1
                		  AND played = 1
                		GROUP BY team1_id, team2_id
                		ORDER BY team1_id
                	)
                )
            """, (competition_id, next_cup_round, ))

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
                )
            """, (next_cup_round, competition_id,next_cup_round,))

            conn.commit()
            
            # Create next round fixtures
            cur.execute("""
                INSERT INTO fixtures(home_club_id, away_club_id, date, competition_id, played, competition_round)
                SELECT home_id, away_id, match_date, ? as competition_id, 0 as played, ? as competition_round FROM (
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
                			THEN date(gv.value_date, 'weekday 2', '+7 days')   -- Tuesday
                		ELSE
                			date(gv.value_date, 'weekday 3', '+7 days')        -- Wednesday
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
            """, (competition_id, next_cup_round, competition_id,next_cup_round,))

            conn.commit()
            print(f"First leg for round {next_cup_round} created")
            
            #Calculate again team number after deciding winners
            cur.execute("""
                SELECT count(1)
                FROM clubs_competition
                WHERE competition_id = ? and is_active
            """, (competition_id,))
            row = cur.fetchone()
            total_clubs = row[0] if row else 0
            print(f"Total clubs: {total_clubs}")
            
            if total_clubs >= 4:
                cur.execute("""
                    INSERT INTO fixtures(home_club_id, away_club_id, date, competition_id, played, competition_round)
                    SELECT f.away_club_id as home_club_id, f.home_club_id as away_club_id, date(f.date, '+7 days'), ? as competition_id, 0 as played, ? as competition_round
                    FROM fixtures f WHERE competition_id = ? and played = 0 and competition_round = ?
                """, (competition_id, next_cup_round, competition_id,next_cup_round,))

                conn.commit()
                print(f"Second leg for round {next_cup_round} created")
            
            
        else:
            
                
            if total_clubs == 1:
                print("Cup already finished")
            elif total_clubs in {2, 4, 8, 16, 32, 64, 128} and total_clubs > 1:
                print("Correct number. Scheduling fixtures")
    
            else: 
                print("NOT correct number. Preliminary round needed")
    
                if total_clubs > 128:
                    print("Adjusting to get 128 teams")
                    round_target=128
                    preliminary_teams=2*(total_clubs-round_target)                
                elif total_clubs > 64:
                    print("Adjusting to get 64 teams")
                    round_target=64
                    preliminary_teams=2*(total_clubs-round_target)
                    print(f"Preliminary_teams: {preliminary_teams}")
                elif total_clubs > 32:
                    print("Adjusting to get 32 teams")
                    round_target=32
                    preliminary_teams=2*(total_clubs-round_target)
                    print(f"Preliminary_teams: {preliminary_teams}")
                    
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
                        )
                    """, (next_cup_round, competition_id,preliminary_teams,))
    
                    conn.commit()
                    
                    #First round
                    cur.execute("""
                        INSERT INTO fixtures(home_club_id, away_club_id, date, competition_id, played, competition_round)
                        SELECT home_id, away_id, match_date, ? as competition_id, 0 as played, ? as competition_round FROM (
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
                        			THEN date(gv.value_date, 'weekday 2', '+7 days')   -- Tuesday
                        		ELSE
                        			date(gv.value_date, 'weekday 3', '+7 days')        -- Wednesday
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
                    """, (competition_id, next_cup_round, competition_id,next_cup_round,))
                    print(f"First leg for round {next_cup_round} created")
                    conn.commit()
                    
                    #Second round, if there are more than 4 teams
                    
                    if total_clubs >= 4:
                        cur.execute("""
                            INSERT INTO fixtures(home_club_id, away_club_id, date, competition_id, played, competition_round)
                            SELECT f.away_club_id as home_club_id, f.home_club_id as away_club_id, date(f.date, '+7 days'), ? as competition_id, 0 as played, ? as competition_round
                            FROM fixtures f WHERE competition_id = ? and played = 0 and competition_round = ?
                        """, (competition_id, next_cup_round, competition_id,next_cup_round,))
        
                        conn.commit()
                        print(f"Second leg for round {next_cup_round} created")
                
               
                elif total_clubs > 16:
                    print("Adjusting to get 16 teams")
                    round_target=16
                    preliminary_teams=2*(total_clubs-round_target)
                elif total_clubs > 8:
                    print("Adjusting to get 8 teams")
                    round_target=8
                    preliminary_teams=2*(total_clubs-round_target)
                elif total_clubs > 4:
                    print("Adjusting to get 4 teams")
                    round_target=4
                    preliminary_teams=2*(total_clubs-round_target)
                elif total_clubs > 2:
                    print("Adjusting to get 2 teams")
                    round_target=2
                    preliminary_teams=2*(total_clubs-round_target)


    conn.close()


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    # Uncomment to hard reset DB schema & base tables:
    init_db()

    # If you reset DB, (optionally) load clubs from CSV:
    depopulate_clubs()
    populate_clubs()
    
    populate_competition_clubs()

    update_game_date_db()           # keep GAME_DATE in DB in sync

    # Fresh players & fixtures each run (like your previous workflow)
    depopulate_players()
    #populate_400_players()
    populate_800_players()

    depopulate_fixtures()
    populate_fixtures(1)
    populate_fixtures(2)
    
    cup_manage(3)
    
    

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
