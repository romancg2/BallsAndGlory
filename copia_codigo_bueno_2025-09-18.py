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
        "at_speed": 0.5, "at_passing": 1, "at_scoring": 0.1, "at_goalkeeping": 5
    },
    "CB": {
        "at_defending": 3, "at_confidence": 2, "at_working": 2,
        "at_speed": 0.7, "at_passing": 1, "at_scoring": 0.3, "at_dribbling": 0.5
    },
    "RB": {
        "at_defending": 2, "at_speed": 2, "at_passing": 1.5,
        "at_dribbling": 1, "at_confidence": 1.5
    },
    "LB": {
        "at_defending": 2, "at_speed": 2, "at_passing": 1.5,
        "at_dribbling": 1, "at_confidence": 1.5
    },
    "CDM": {
        "at_defending": 2.5, "at_passing": 2, "at_working": 2,
        "at_confidence": 1.5, "at_scoring": 0.5, "at_dribbling": 1
    },
    "CM": {
        "at_passing": 3, "at_dribbling": 2, "at_working": 2,
        "at_confidence": 1.5, "at_defending": 1.5, "at_scoring": 1
    },
    "CAM": {
        "at_passing": 2.5, "at_dribbling": 2.5, "at_scoring": 2,
        "at_confidence": 1.5, "at_speed": 1.2
    },
    "RM": {
        "at_speed": 2, "at_dribbling": 2, "at_passing": 1.5,
        "at_confidence": 1.2, "at_scoring": 1.2
    },
    "LM": {
        "at_speed": 2, "at_dribbling": 2, "at_passing": 1.5,
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
        "at_dribbling": 1.5, "at_passing": 0.8, "at_defending": 0.3
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
    DROP TABLE IF EXISTS leagues;
    DROP TABLE IF EXISTS players;
    DROP TABLE IF EXISTS global_val;
    DROP TABLE IF EXISTS match_scorers;
    """)

    cur.executescript("""
     CREATE TABLE global_val (
         var_id INTEGER PRIMARY KEY AUTOINCREMENT,
         var_name TEXT NOT NULL,
         value_text TEXT,
         value_int INTEGER,
         value_date DATE
     );

    CREATE TABLE leagues (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        country TEXT NOT NULL,
        level INTEGER
    );

    CREATE TABLE clubs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        short_name TEXT,
        league_id INTEGER,
        stadium TEXT,
        fame INTEGER,
        FOREIGN KEY (league_id) REFERENCES leagues(id)
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
        home_goals INTEGER,
        away_goals INTEGER,
        played BOOLEAN DEFAULT 0,
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

    leagues = [("Premier League", "England", 1), ("Championship", "England", 2)]
    cur.executemany("INSERT INTO leagues (name, country, level) VALUES (?, ?, ?)", leagues)

    cur.execute("INSERT INTO global_val (var_name, value_date) VALUES (?, ?)", ("GAME_DATE", GAME_DATE.isoformat()))
    conn.commit()
    conn.close()
    print("✅ Database initialized:", DB_PATH)

# -----------------------------
# Utility
# -----------------------------
def clamp(v, lo, hi):
    return max(lo, min(hi, v))

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

def populate_fixtures():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id FROM clubs WHERE league_id = 1")
    club_ids = [r[0] for r in cur.fetchall()]
    if len(club_ids) != 20:
        print("⚠️ Expected 20 Premier League clubs. Found:", len(club_ids))
        conn.close()
        return

    random.shuffle(club_ids)
    first_half = round_robin_rounds(club_ids)
    second_half = [[(a2, h2) for (h2, a2) in rnd] for rnd in first_half]
    all_rounds = first_half + second_half

    first_sat = next_saturday(GAME_DATE)
    fixtures_to_insert = []
    for round_index, rnd in enumerate(all_rounds):
        sat = first_sat + timedelta(weeks=round_index)
        sun = sat + timedelta(days=1)
        matches = list(rnd)
        random.shuffle(matches)
        k = random.choice([3, 4, 5])
        for home, away in matches[:k]:
            fixtures_to_insert.append((sat.isoformat(), home, away, None, None, 0))
        for home, away in matches[k:]:
            fixtures_to_insert.append((sun.isoformat(), home, away, None, None, 0))

    cur.execute("DELETE FROM fixtures")
    cur.execute("DELETE FROM sqlite_sequence WHERE name='fixtures'")
    cur.executemany("""INSERT INTO fixtures (date, home_club_id, away_club_id, home_goals, away_goals, played)
                       VALUES (?, ?, ?, ?, ?, ?)""", fixtures_to_insert)
    conn.commit()
    conn.close()
    print(f"✅ Inserted {len(fixtures_to_insert)} fixtures")

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
        SELECT f.id, f.home_club_id, hc.name, f.away_club_id, ac.name
        FROM fixtures f
        JOIN clubs hc ON hc.id = f.home_club_id
        JOIN clubs ac ON ac.id = f.away_club_id
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

    for fixture_id, home_id, home_name, away_id, away_name in fixtures:
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

        print(f"⚽ {home_name} {home_goals} - {away_goals} {away_name}")
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

        # play fixtures for the day
        simulate_fixtures_for_day(conn, GAME_DATE)

        # season rollover on Aug 31
        if GAME_DATE.month == 8 and GAME_DATE.day == 31:
            print("📅 End of season! Resetting fixtures...")
            depopulate_fixtures()
            populate_fixtures()
            depopulate_match_scorers()
            LEAGUE_ATK_MEAN = None
            LEAGUE_DEF_MEAN = None
            print("✅ New season fixtures generated!")

        # advance one day
        GAME_DATE = advance_game_day(GAME_DATE)
        cur.execute("UPDATE global_val SET value_date=? WHERE var_name='GAME_DATE'", (GAME_DATE.isoformat(),))
        conn.commit()

        # weekly updates (Monday)
        if GAME_DATE.weekday() == 0:
            update_players_in_db(conn, GAME_DATE)

        print(f"Game Date: {GAME_DATE}")

    conn.close()

# -----------------------------
# Quick test util (regen dump)
# -----------------------------
def test_regen_creation(n=3, position="ST"):
    print(f"\n🧪 Testing {n} regen(s) for position {position}...\n")
    for i in range(n):
        player, attrs = generate_player(position=position, club_id=1, club_fame=1000, force_youth=True)
        first, last, dob, nat, pos, club, val, wage, contract = player
        print(f"--- Regen {i+1} ---")
        print(f"Name: {first} {last}, Age: {calculate_age(dob, GAME_DATE)}")
        print(f"Position: {pos}, Nationality: {nat}")
        print(f"Market Value: {val:,}, Wage: {wage:,}")
        print(f"Curr Ability: {attrs[-2]} | Pot Ability: {attrs[-1]}")
    print()

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    # Uncomment to hard reset DB schema & base tables:
    # init_db()

    # If you reset DB, (optionally) load clubs from CSV:
    # depopulate_clubs()
    # populate_clubs()

    update_game_date_db()           # keep GAME_DATE in DB in sync

    # Fresh players & fixtures each run (like your previous workflow)
    depopulate_players()
    populate_400_players()

    depopulate_fixtures()
    populate_fixtures()

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
