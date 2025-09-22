import random
import csv
from tabulate import tabulate
from faker import Faker
from datetime import datetime, timedelta, date
import sqlite3
from dateutil.relativedelta import relativedelta
import datetime
import math

fake = Faker()

# =========================
# POSITION CONFIG
# =========================
positions_map = {
    "GK": 0, "CB": 1, "RB": 2, "LB": 3,
    "CDM": 4, "CM": 5, "CAM": 6, "RM": 7, "LM": 8,
    "RW": 9, "LW": 10, "ST": 11
}

position_groups = {
    "GK": "GK",
    "CB": "DEF", "RB": "DEF", "LB": "DEF",
    "CDM": "MID", "CM": "MID", "CAM": "MID", "RM": "MID", "LM": "MID",
    "RW": "MID", "LW": "MID",
    "ST": "ST"
}

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

# =========================
# ATTRIBUTE WEIGHTS & CAPS
# =========================
position_attribute_weights = {
    "GK": {
        "at_defending": 3, "at_selfcont": 2, "at_confidence": 2,
        "at_speed": 0.5, "at_passing": 1, "at_scoring": 0.1,
        "at_goalkeeping": 5  # GK priority
    },
    "CB": {"at_defending": 3, "at_confidence": 2, "at_working": 2,
           "at_speed": 0.7, "at_passing": 1, "at_scoring": 0.3, "at_dribbling": 0.5},
    "RB": {"at_defending": 2, "at_speed": 2, "at_passing": 1.5,
           "at_dribbling": 1, "at_confidence": 1.5},
    "LB": {"at_defending": 2, "at_speed": 2, "at_passing": 1.5,
           "at_dribbling": 1, "at_confidence": 1.5},
    "CDM": {"at_defending": 2.5, "at_passing": 2, "at_working": 2,
            "at_confidence": 1.5, "at_scoring": 0.5, "at_dribbling": 1},
    "CM": {"at_passing": 3, "at_dribbling": 2, "at_working": 2,
           "at_confidence": 1.5, "at_defending": 1.5, "at_scoring": 1},
    "CAM": {"at_passing": 2.5, "at_dribbling": 2.5, "at_scoring": 2,
            "at_confidence": 1.5, "at_speed": 1.2},
    "RM": {"at_speed": 2, "at_dribbling": 2, "at_passing": 1.5,
           "at_confidence": 1.2, "at_scoring": 1.2},
    "LM": {"at_speed": 2, "at_dribbling": 2, "at_passing": 1.5,
           "at_confidence": 1.2, "at_scoring": 1.2},
    "RW": {"at_speed": 2.5, "at_dribbling": 2.5, "at_scoring": 2,
           "at_confidence": 1.5, "at_passing": 1.2},
    "LW": {"at_speed": 2.5, "at_dribbling": 2.5, "at_scoring": 2,
           "at_confidence": 1.5, "at_passing": 1.2},
    "ST": {"at_scoring": 3, "at_speed": 2, "at_confidence": 1.5,
           "at_dribbling": 1.5, "at_passing": 0.8, "at_defending": 0.3}
}

POSITION_ATTRIBUTE_CAPS = {
    "GK": {"at_scoring": 150, "at_dribbling": 400, "at_passing": 700,
           "at_defending": 400, "at_goalkeeping": 2000},
    "CB": {"at_scoring": 300, "at_dribbling": 600, "at_passing": 900, "at_goalkeeping": 150},
    "RB": {"at_scoring": 400, "at_passing": 1000, "at_goalkeeping": 150},
    "LB": {"at_scoring": 400, "at_passing": 1000, "at_goalkeeping": 150},
    "CDM": {"at_scoring": 400, "at_passing": 1200, "at_defending": 1500, "at_goalkeeping": 150},
    "CM": {"at_scoring": 600, "at_passing": 1500, "at_defending": 1000, "at_goalkeeping": 150},
    "CAM": {"at_scoring": 900, "at_passing": 1600, "at_defending": 800, "at_goalkeeping": 150},
    "RM": {"at_scoring": 700, "at_passing": 1400, "at_defending": 900,
           "at_speed": 1600, "at_goalkeeping": 150},
    "LM": {"at_scoring": 700, "at_passing": 1400, "at_defending": 900,
           "at_speed": 1600, "at_goalkeeping": 150},
    "RW": {"at_scoring": 1000, "at_passing": 1300, "at_defending": 800, "at_goalkeeping": 150},
    "LW": {"at_scoring": 1000, "at_passing": 1300, "at_defending": 800, "at_goalkeeping": 150},
    "ST": {"at_defending": 600, "at_passing": 1000, "at_goalkeeping": 150}
}

# =========================
# GAME CONSTANTS
# =========================
YOUTH_DAMPENING = {"GK": 0.1, "DEF": 0.1, "MID": 0.1, "ST": 0.1}
GAME_DATE = date(2025, 9, 1)

GOAL_SCALING = 6.00
DEFENSE_EXP = 0.65
DEF_SUPPRESS = 0.35
LEAGUE_ATK_MEAN = None
LEAGUE_DEF_MEAN = None

ABILITY_GLOBAL_SCALE = 1.1
ATTR_GLOBAL_SCALE = 1.4
SCORER_BOOST = 1.15


# =========================
# DATABASE INIT
# =========================
def init_db():
    """Create base schema including players and players_attr."""
    conn = sqlite3.connect("fm_database.sqlite")
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

    # Tables creation (continues in next chunk)

    # --- Create tables ---
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

    # Insert sample leagues
    leagues = [
        ("Premier League", "England", 1),
        ("Championship", "England", 2)
    ]
    cur.executemany("INSERT INTO leagues (name, country, level) VALUES (?, ?, ?)", leagues)

    # Insert global variables
    global_val = [("GAME_DATE", GAME_DATE)]
    cur.executemany("INSERT INTO global_val (var_name, value_date) VALUES (?, ?)", global_val)

    conn.commit()
    conn.close()
    print("✅ Database initialized: fm_database.sqlite")


# =========================
# PLAYER UTILS
# =========================
def random_age(position=None):
    """Generate realistic player ages based on position."""
    if position in ("GK",):
        buckets = [(17, 20, 0.05), (21, 24, 0.20), (25, 28, 0.35),
                   (29, 32, 0.30), (33, 38, 0.10)]
    elif position in ("CB", "RB", "LB"):
        buckets = [(17, 20, 0.08), (21, 24, 0.28), (25, 28, 0.37),
                   (29, 32, 0.22), (33, 36, 0.05)]
    elif position in ("CM", "CDM", "CAM"):
        buckets = [(17, 20, 0.12), (21, 24, 0.32), (25, 28, 0.37),
                   (29, 31, 0.16), (32, 35, 0.03)]
    elif position in ("RW", "LW", "ST", "CF", "FW"):
        buckets = [(17, 20, 0.18), (21, 24, 0.37), (25, 28, 0.32),
                   (29, 31, 0.11), (32, 35, 0.02)]
    else:
        buckets = [(17, 20, 0.12), (21, 24, 0.35), (25, 28, 0.37),
                   (29, 31, 0.14), (32, 35, 0.02)]

    lo, hi, _ = random.choices(buckets, weights=[b[2] for b in buckets])[0]
    return random.randint(lo, hi)


def random_potential():
    """Generate player potential (1000–2000)."""
    buckets = [
        (1000, 1400, 0.55),
        (1401, 1700, 0.30),
        (1701, 1850, 0.12),
        (1851, 2000, 0.03)
    ]
    lo, hi, _ = random.choices(buckets, weights=[b[2] for b in buckets])[0]
    return random.randint(lo, hi)


def distribute_attributes(curr_ability, pot_ability, position):
    """Distribute player attributes based on position weights."""
    weights = position_attribute_weights.get(position, {})
    attrs = {}

    for attr in [
        "at_luck", "at_selfcont", "at_honour", "at_crazyness", "at_working",
        "at_sexatract", "at_friendship", "at_speed", "at_dribbling",
        "at_goalkeeping", "at_defending", "at_passing", "at_scoring",
        "at_happiness", "at_confidence", "at_hope"
    ]:
        weight = weights.get(attr, 1.0)
        val = curr_ability * weight
        val *= random.uniform(0.85, 1.15)  # noise
        val = max(200, min(int(val), pot_ability, 2000))
        cap = POSITION_ATTRIBUTE_CAPS.get(position, {}).get(attr, 2000)
        val = min(val, cap)
        attrs[attr] = val

    return attrs


def generate_player(position=None, club_id=None, club_fame=None, force_youth=False):
    """Generate a single player and attributes."""
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

    # --- Fame & Market Value ---
    if club_fame is None:
        club_fame = 1000

    fame_mult = 1 + (club_fame - 1000) / 2000
    base_value = random.randint(100_000, 50_000_000)
    value = int(base_value * fame_mult)

    if age > 31:
        value = int(value * (0.2 if age >= 34 else 0.4))

    wage = max(150_000, min(int(value * 0.05 * random.uniform(0.8, 1.2)), 20_000_000))
    contract_until = faker.date_between(start_date="+1y", end_date="+5y").isoformat()

    # --- Abilities ---
    pot_ability = random_potential()

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

    curr_ability = min(int(pot_ability * curr_pct), pot_ability)

    # --- Attributes ---
    attrs = distribute_attributes(curr_ability, pot_ability, position)

    player_attr = tuple(attrs[a] for a in attrs) + (curr_ability, pot_ability)
    player = (first_name, last_name, date_of_birth, nationality,
              position, club_id, value, wage, contract_until)

    return player, player_attr

# =========================
# POPULATION FUNCTIONS
# =========================
def populate_400_players():
    conn = sqlite3.connect("fm_database.sqlite")
    cur = conn.cursor()

    # Get Premier League clubs
    cur.execute("SELECT id, fame FROM clubs WHERE league_id = 1")
    clubs = cur.fetchall()
    if len(clubs) != 20:
        print(f"⚠️ Expected 20 Premier League clubs, found {len(clubs)}")
        conn.close()
        return

    position_counts = {
        "GK": 2, "CB": 3, "RB": 2, "LB": 2, "CDM": 1,
        "CM": 2, "CAM": 1, "RM": 1, "LM": 1,
        "RW": 1, "LW": 1, "ST": 3,
    }

    players, players_attr = [], []

    for club_id, club_fame in clubs:
        for position, count in position_counts.items():
            for _ in range(count):
                player, attr = generate_player(position=position, club_id=club_id, club_fame=club_fame)
                player = tuple(p.isoformat() if isinstance(p, date) else p for p in player)
                players.append(player)
                players_attr.append(attr)

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
    players_attr_with_ids = [(pid, *attr) for pid, attr in zip(player_ids, players_attr)]

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
    print("✅ 400 players generated and inserted.")


def depopulate_players():
    conn = sqlite3.connect("fm_database.sqlite")
    cur = conn.cursor()
    cur.execute("DELETE FROM players")
    cur.execute("DELETE FROM players_attr")
    cur.execute("DELETE FROM sqlite_sequence WHERE name='players'")
    cur.execute("DELETE FROM sqlite_sequence WHERE name='players_attr'")
    conn.commit()
    conn.close()
    print("✅ Players depopulated.")


def populate_clubs():
    conn = sqlite3.connect("fm_database.sqlite")
    cur = conn.cursor()

    with open('premier_league_clubs.csv', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = [(row['name'], row.get('short_name'),
                 row.get('league_id'), row.get('stadium'), row.get('fame'))
                for row in reader]

    cur.executemany("""
    INSERT INTO clubs (name, short_name, league_id, stadium, fame)
    VALUES (?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    conn.close()
    print("✅ Clubs populated")


def depopulate_clubs():
    conn = sqlite3.connect("fm_database.sqlite")
    cur = conn.cursor()
    cur.execute("DELETE FROM clubs")
    cur.execute("DELETE FROM sqlite_sequence WHERE name='clubs'")
    conn.commit()
    conn.close()
    print("✅ Clubs depopulated.")


def depopulate_fixtures():
    conn = sqlite3.connect("fm_database.sqlite")
    cur = conn.cursor()
    cur.execute("DELETE FROM fixtures")
    cur.execute("DELETE FROM sqlite_sequence WHERE name='fixtures'")
    conn.commit()
    conn.close()
    print("✅ Fixtures depopulated.")


def populate_fixtures():
    conn = sqlite3.connect("fm_database.sqlite")
    cur = conn.cursor()

    cur.execute("SELECT id FROM clubs WHERE league_id = 1")
    club_rows = cur.fetchall()
    club_ids = [r[0] for r in club_rows]

    if len(club_ids) != 20:
        print("⚠️ Expected 20 Premier League clubs. Found:", len(club_ids))
        conn.close()
        return

    random.shuffle(club_ids)
    first_half = round_robin_rounds(club_ids)
    second_half = [[(away, home) for (home, away) in rnd] for rnd in first_half]
    all_rounds = first_half + second_half

    start_datetime = GAME_DATE
    first_sat = next_saturday(start_datetime)

    fixtures_to_insert = []
    for round_index, rnd in enumerate(all_rounds):
        sat = first_sat + timedelta(weeks=round_index)
        sun = sat + timedelta(days=1)
        matches = list(rnd)
        random.shuffle(matches)
        k = random.choice([3, 4, 5])
        sat_matches, sun_matches = matches[:k], matches[k:]

        for home, away in sat_matches:
            fixtures_to_insert.append((sat.isoformat(), home, away, None, None, 0))
        for home, away in sun_matches:
            fixtures_to_insert.append((sun.isoformat(), home, away, None, None, 0))

    print(f"Scheduling {len(all_rounds)} rounds, total fixtures planned: {len(fixtures_to_insert)}")
    if len(fixtures_to_insert) != 380:
        print("⚠️ Unexpected number of fixtures (expected 380).")

    cur.execute("DELETE FROM fixtures")
    cur.execute("DELETE FROM sqlite_sequence WHERE name='fixtures'")

    cur.executemany("""
    INSERT INTO fixtures (date, home_club_id, away_club_id, home_goals, away_goals, played)
    VALUES (?, ?, ?, ?, ?, ?)
    """, fixtures_to_insert)

    conn.commit()
    conn.close()
    print(f"✅ Inserted {len(fixtures_to_insert)} fixtures into database.")


# =========================
# FIXTURE HELPERS
# =========================
def next_saturday(start_dt):
    if isinstance(start_dt, datetime.datetime):
        d = start_dt.date()
    else:
        d = start_dt
    days_ahead = (5 - d.weekday()) % 7
    return d + timedelta(days=days_ahead)


def round_robin_rounds(team_ids):
    """Generate a round robin schedule."""
    teams = list(team_ids)
    n = len(teams)
    if n % 2 == 1:
        teams.append(None)  # bye if odd

    rounds = []
    for r in range(n - 1):
        pairs = []
        for i in range(n // 2):
            a, b = teams[i], teams[n - 1 - i]
            if a is not None and b is not None:
                if r % 2 == 0:
                    pairs.append((a, b))
                else:
                    pairs.append((b, a))
        rounds.append(pairs)
        teams = [teams[0]] + teams[-1:] + teams[1:-1]

    return rounds

def init_db_possib():
    conn = sqlite3.connect("fm_database.sqlite")
    cur = conn.cursor()

    # Drop/create possibility tables
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
        ("Goal scored!", "Goaaaaaaal! The goalkeeper was fooled.", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 2, 1),
        ("Goal scored!", "The ball hits the post and it goes inside!", 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 1, 1),
        ("Penalty stopped. Corner.", "The goalkeeper stops the ball and it goes to corner.", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1),
        ("Penalty stopped. Ball held.", "The goalkeeper stops the ball and secures it.", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1),
        ("Penalty stopped. Rebound goes to defender.", "The goalkeeper stops the ball and it goes to a defender who starts the counterattack.", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -2, -2, -2),
        ("Penalty stopped. Rebound goes to player.", "The goalkeeper stops the ball but the ball is free and the player goes for the rebound.", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    ]
    cur.executemany("""
    INSERT INTO consequences (
        conseq_title, conseq_description,
        conseq_luck, conseq_selfcont, conseq_honour, conseq_crazyness,
        conseq_working, conseq_sexatract, conseq_friendship, conseq_speed,
        conseq_dribbling, conseq_defending, conseq_passing, conseq_scoring,
        conseq_happiness, conseq_confidence, conseq_hope
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, consequences)

    situ_options = [(1,1), (1,2), (1,3), (2,1), (2,2), (2,3)]
    cur.executemany("INSERT INTO situ_options (sit_id, opt_id) VALUES (?, ?)", situ_options)

    options_conseq = [
        (1,1,20), (1,2,15), (1,3,20), (1,4,15), (1,5,15), (1,6,15),
        (2,1,40), (2,2,20), (2,3,10), (2,4,10), (2,5,10), (2,6,10),
        (3,1,30), (3,2,0), (3,3,15), (3,4,30), (3,5,15), (3,6,10)
    ]
    cur.executemany("INSERT INTO options_conseq (opt_id, conseq_id, conseq_probab) VALUES (?, ?, ?)", options_conseq)

    player_situ = [(16,1)]
    cur.executemany("INSERT INTO player_situ (player_id, sit_id) VALUES (?, ?)", player_situ)

    conn.commit()
    conn.close()
    print("✅ Possibility DB initialized.")


# =========================
# GAME LOOP + HELPERS
# =========================
def print_table(table_name):
    conn = sqlite3.connect("fm_database.sqlite")
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    print(f"\n📋 Table: {table_name}")
    print(tabulate(rows, headers=headers, tablefmt="grid"))
    conn.close()


def apply_consequence(player_id, conseq_id, conn):
    cur = conn.cursor()
    cur.execute("SELECT at_curr_ability, at_pot_ability FROM players_attr WHERE player_id=?", (player_id,))
    ability = cur.fetchone()
    if not ability:
        return
    curr_ability, pot_ability = ability
    if curr_ability > pot_ability:
        return

    cur.execute("""
    SELECT COALESCE(conseq_luck,0), COALESCE(conseq_selfcont,0),
           COALESCE(conseq_honour,0), COALESCE(conseq_crazyness,0),
           COALESCE(conseq_working,0), COALESCE(conseq_sexatract,0),
           COALESCE(conseq_friendship,0), COALESCE(conseq_speed,0),
           COALESCE(conseq_dribbling,0), COALESCE(conseq_defending,0),
           COALESCE(conseq_passing,0), COALESCE(conseq_scoring,0),
           COALESCE(conseq_happiness,0), COALESCE(conseq_confidence,0),
           COALESCE(conseq_hope,0)
    FROM consequences WHERE conseq_id=?
    """, (conseq_id,))
    deltas = cur.fetchone()
    if not deltas:
        return

    total_delta = sum(deltas)
    if curr_ability + total_delta > pot_ability:
        return

    cur.execute("""
    UPDATE players_attr SET
        at_luck = MIN(at_luck + ?, 2000),
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
    print("🟢 Attributes updated successfully!")


def run_game(player_id):
    conn = sqlite3.connect("fm_database.sqlite")
    cur = conn.cursor()
    while True:
        cur.execute("SELECT sit_id, sit_title, sit_description FROM situations ORDER BY RANDOM() LIMIT 1")
        sit_id, sit_title, sit_description = cur.fetchone()
        print(f"\n⚽ Situation: {sit_title}")
        print(sit_description)

        cur.execute("""
        SELECT o.opt_id, o.opt_title, o.opt_description
        FROM options o
        JOIN situ_options so ON o.opt_id = so.opt_id
        WHERE so.sit_id = ?
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
            print("❌ Invalid input, try again.")
            continue

        if not any(opt_id == choice for opt_id, _, _ in options):
            print("❌ Option not available, try again.")
            continue

        cur.execute("""
        SELECT c.conseq_id, c.conseq_title, c.conseq_description, oc.conseq_probab
        FROM consequences c
        JOIN options_conseq oc ON c.conseq_id = oc.conseq_id
        WHERE oc.opt_id = ?
        """, (choice,))
        conseqs = cur.fetchall()
        if not conseqs:
            print("⚠️ No consequences found for this option.")
            continue

        conseq_ids = [row[0] for row in conseqs]
        conseq_titles = [row[1] for row in conseqs]
        conseq_descs = [row[2] for row in conseqs]
        conseq_probs = [row[3] for row in conseqs]
        chosen_idx = random.choices(range(len(conseq_ids)), weights=conseq_probs, k=1)[0]
        chosen_conseq_id = conseq_ids[chosen_idx]

        print(f"\n🎲 Result: {conseq_titles[chosen_idx]}")
        print(conseq_descs[chosen_idx])

        cur.execute("""
        INSERT INTO player_situ (player_id, sit_id, conseq_id, player_situ_date)
        VALUES (?, ?, ?, DATE('now'))
        """, (player_id, sit_id, chosen_conseq_id))
        conn.commit()

        cur.execute("""
        SELECT conseq_luck, conseq_selfcont, conseq_honour, conseq_crazyness,
               conseq_working, conseq_sexatract, conseq_friendship, conseq_speed,
               conseq_dribbling, conseq_defending, conseq_passing, conseq_scoring,
               conseq_happiness, conseq_confidence, conseq_hope
        FROM consequences WHERE conseq_id = ?
        """, (chosen_conseq_id,))
        deltas = cur.fetchone()
        if deltas:
            apply_consequence(player_id, chosen_conseq_id, conn)
            print("\n🟢 Player attributes updated!")

        print("\n📜 Game summary:")
        cur.execute("""
        SELECT s.sit_title, c.conseq_title, c.conseq_description, ps.player_situ_date
        FROM player_situ ps
        JOIN situations s ON ps.sit_id = s.sit_id
        JOIN consequences c ON ps.conseq_id = c.conseq_id
        WHERE ps.player_id = ?
        ORDER BY ps.player_situ_date, ps.player_situ_id
        """, (player_id,))
        history = cur.fetchall()
        if history:
            for i, (sit_title, conseq_title, conseq_desc, date) in enumerate(history, 1):
                print(f"\n{i}. [{date}]")
                print(f" ⚽ Situation: {sit_title}")
                print(f" 🎲 Result: {conseq_title}")
                print(f" 📝 {conseq_desc}")
        else:
            print("No history found.")

    conn.close()


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    # init_db()
    update_game_date_db()   # mandatory
    depopulate_players()    # mandatory
    populate_400_players()  # mandatory
    depopulate_fixtures()   # mandatory
    populate_fixtures()     # mandatory
    depopulate_match_scorers()  # mandatory
    # init_db_possib()
    # print_table("players_attr")
    # clean_player_situ()
    # run_game(16)
    game_loop()             # mandatory
    # test_regen_creation(3, position="CB")
    # test_regen_creation(3, position="ST")
    # test_regen_creation(3, position="GK")
