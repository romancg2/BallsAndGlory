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

import decision_making
from decision_making import adjust_board_satisfaction,season_end_board_adjustments

from db_population import init_db, generate_player, populate_all_players, distribute_attributes, calculate_player_fame
from fixture_calculation import simulate_fixtures_for_day

LEAGUE_DEBUGGING = False
CUP_DEBUGGING = False


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


# League scoring tuning (final: slightly reduced totals)

DEFENSE_EXP = 0.65
DEF_SUPPRESS = 0.35

# League baselines (lazy)
LEAGUE_ATK_MEAN = None
LEAGUE_DEF_MEAN = None

SCORER_BOOST = 1.15

# Tables that should have seasonal snapshots
SNAPSHOT_TABLES = (
    "players",
    "players_attr",
    "staff",
    "staff_attr",
    "clubs",
    "clubs_board"  
)

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

FORMATIONS = [
    "4-4-2", "4-3-3", "3-5-2", "4-2-3-1",
    "4-1-4-1", "3-4-3", "5-3-2"
]


# -----------------------------
# DB schema
# -----------------------------


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


def initialize_club_balances():
    """
    Seed club cash with tiered ranges scaled by fame percentile inside each league,
    then apply a global downscale (~2/3) with a small top/bottom skew:
      - richest clubs slightly higher, poorest slightly lower.
      - never below MIN_MONTHS_COVER of wages after scaling.
    """
    import sqlite3, random

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # --- knobs ---
    BASE_SCALE        = 2 / 3       # ~0.667 overall reduction
    EDGE_BOOST        = 0.20        # +20% at top percentile, -20% at bottom
    MIN_MONTHS_COVER  = 2           # ≥ 2 months of wages after scaling
    GLOBAL_CAP        = 180_000_000 # final cap

    # League scaling & floors (before scaling)
    LEAGUE_SCALE      = {1: 1.00, 2: 0.55}  # others get DEFAULT_LG_SCALE
    DEFAULT_LG_SCALE  = 0.35
    FLOOR_BY_LEAGUE   = {1: 8_000_000, 2: 3_000_000}
    DEFAULT_FLOOR     = 1_500_000

    # Pull clubs
    clubs = cur.execute("SELECT id, fame, league_id FROM clubs").fetchall()

    # Build fame lists per league (for percentiles)
    from collections import defaultdict
    famelist_by_league = defaultdict(list)
    for _, fame, lg in clubs:
        famelist_by_league[lg].append(fame)

    # Precompute first-index maps for stable percentile with duplicates
    first_idx_by_lg = {}
    for lg, famelist in famelist_by_league.items():
        sorted_desc = sorted(famelist, reverse=True)
        first_idx = {}
        for i, f in enumerate(sorted_desc):
            if f not in first_idx:
                first_idx[f] = i
        first_idx_by_lg[lg] = (sorted_desc, first_idx)

    def fame_percentile_in_league(league_id: int, fame_val: int) -> float:
        sorted_desc, first_idx = first_idx_by_lg.get(league_id, ([0], {0: 0}))
        n = len(sorted_desc)
        idx = first_idx.get(fame_val, n - 1)
        if n <= 1:
            return 1.0
        # 0..1, where 1.0 is highest fame in league
        return 1.0 - (idx / (n - 1))

    for club_id, fame, league_id in clubs:
        # Annual wages from existing contracts (fallback if empty)
        yearly_wages = cur.execute("""
            SELECT COALESCE(SUM(wage),0)
            FROM players_contract
            WHERE club_id=? AND is_terminated=0
        """, (club_id,)).fetchone()[0]
        monthly_wages = yearly_wages / 12.0
        if monthly_wages <= 0:
            # fallback: rough wage from fame
            monthly_wages = max(75_000, fame * 7_500)

        # Fame percentile within league
        p = fame_percentile_in_league(league_id, fame)

        # Tiered base by percentile (before league scaling)
        if p >= 0.95:
            base_min, base_max = 90_000_000, 140_000_000; months_cover = 8
        elif p >= 0.85:
            base_min, base_max = 60_000_000, 90_000_000;  months_cover = 7
        elif p >= 0.70:
            base_min, base_max = 40_000_000, 60_000_000;  months_cover = 6
        elif p >= 0.50:
            base_min, base_max = 25_000_000, 40_000_000;  months_cover = 5
        elif p >= 0.30:
            base_min, base_max = 12_000_000, 25_000_000;  months_cover = 4
        else:
            base_min, base_max = 3_000_000, 12_000_000;   months_cover = 3

        # League scaling + jitter
        lg_scale  = LEAGUE_SCALE.get(league_id, DEFAULT_LG_SCALE)
        base_pick = int(random.uniform(base_min, base_max) * lg_scale)
        min_by_wages = int(monthly_wages * months_cover)
        floor = FLOOR_BY_LEAGUE.get(league_id, DEFAULT_FLOOR)

        raw = max(base_pick, min_by_wages, floor)
        raw = int(raw * random.uniform(0.95, 1.10))  # small dispersion

        # Percentile-based skew:
        #   p=1.0 -> multiplier = BASE_SCALE * (1 + EDGE_BOOST)
        #   p=0.0 -> multiplier = BASE_SCALE * (1 - EDGE_BOOST)
        scale   = BASE_SCALE * (1 + EDGE_BOOST * (2 * p - 1))
        balance = int(raw * scale)

        # Scale the floor similarly so the bottom can actually be lower
        floor_scaled = int(floor * scale)

        # Ensure minimum wage cushion after scaling
        min_wage_floor = int(monthly_wages * MIN_MONTHS_COVER)

        # Final floors & cap
        balance = max(balance, floor_scaled, min_wage_floor)
        balance = min(balance, GLOBAL_CAP)

        cur.execute("UPDATE clubs SET current_balance_EUR=? WHERE id=?", (balance, club_id))

    conn.commit()
    conn.close()



def process_monthly_finances(conn, game_date):
    from datetime import timedelta
    cur = conn.cursor()

    # Previous month window (run on the 1st)
    first_this_month = game_date.replace(day=1)
    prev_month_end   = first_this_month - timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)
    month_str = prev_month_start.strftime("%Y-%m-01")

    # Idempotent monthly row
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_cme_month_club
        ON clubs_monthly_economy(month_date, club_id)
    """)

    # Snapshot live balances now — this IS the month-end balance (B_end)
    clubs = cur.execute("SELECT id, current_balance_EUR, fame FROM clubs").fetchall()

    for club_id, B_end, fame in clubs:
        
        
        # Balance 2 months ago
        target_month = (GAME_DATE.replace(day=1) - relativedelta(months=2)).strftime("%Y-%m-01")
        row = cur.execute("""
            SELECT balance_before, balance_after
            FROM clubs_monthly_economy
            WHERE club_id=? AND month_date=?""",
            (club_id, target_month)
        ).fetchone()
        
        if row:
            B_start_2_months_ago, B_end_hist_2_months_ago = row  # opening, closing two months ago     
        else: 
            B_start_2_months_ago = B_end
            B_end_hist_2_months_ago = B_end
        
        
        # Wages for PLAYERS previous month (active contracts on prev_month_end)
        yearly_wages = cur.execute("""
            SELECT COALESCE(SUM(wage),0)
            FROM players_contract
            WHERE club_id=? AND is_terminated=0
              AND contract_start<=? AND contract_end>=?
        """, (club_id, prev_month_end, prev_month_end)).fetchone()[0]
        wages_total = yearly_wages // 12

        # Operational income/expense (simple model)
        ops_income = int(wages_total * random.uniform(0.8, 1.2) * (0.8 + fame / 3000))
        ops_exp    = int(wages_total * 0.15)

        # Transfers during the month (reporting only — cash already moved day-of)
        transfer_in = cur.execute("""
            SELECT COALESCE(SUM(fee),0) FROM transfers_log
            WHERE to_club_id=? AND ts BETWEEN ? AND ?
        """, (club_id, prev_month_start.isoformat(), prev_month_end.isoformat())).fetchone()[0]

        transfer_out = cur.execute("""
            SELECT COALESCE(SUM(fee),0) FROM transfers_log
            WHERE from_club_id=? AND ts BETWEEN ? AND ?
        """, (club_id, prev_month_start.isoformat(), prev_month_end.isoformat())).fetchone()[0]

        # Totals shown in the monthly row
        income_total      = ops_income + transfer_out
        expenditure_total = ops_exp    + transfer_in

        # Reconstruct the true month-start balance so the row reconciles:
        # B_end = B_start + ops_income + transfer_in - (wages_total + ops_exp + transfer_out)
        B_start = B_end - ops_income - transfer_in + wages_total + ops_exp + transfer_out

        # Write monthly row: BEFORE = B_start, AFTER = B_end (matches clubs table now)
        cur.execute("""
            INSERT INTO clubs_monthly_economy
              (month_date, club_id, income_total, expenditure_total, wages_total,
               balance_before, balance_after)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (month_str, club_id, income_total, wages_total+expenditure_total, wages_total, B_end_hist_2_months_ago,B_end-wages_total-ops_exp+ops_income ))

        # Roll into the new month with ops only (do NOT re-apply transfers)
        B_next = B_end + ops_income - wages_total - ops_exp
        cur.execute("UPDATE clubs SET current_balance_EUR=? WHERE id=?", (B_end-wages_total-ops_exp+ops_income, club_id))

    conn.commit()





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
    
    
def populate_clubs_board():
    """
    Populate clubs_board with initial values for each club.
    Ranges: 0–2000, influenced by fame and balance.
    """
    import random
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Clean old board records (if rerunning)
    cur.execute("DELETE FROM clubs_board")

    cur.execute("SELECT id, fame, current_balance_EUR FROM clubs")
    clubs = cur.fetchall()

    for club_id, fame, balance in clubs:
        # Normalize inputs
        fame_norm = max(0, min(2000, fame))
        econ_norm = max(0, min(2000, int(balance / 50_000)))  # balance drives economy confidence

        manager_satisf   = int(fame_norm * 0.5 + random.randint(400, 1200))
        squad_satisf     = int(fame_norm * 0.6 + random.randint(300, 1000))
        economic_confid  = int(econ_norm * 0.8 + random.randint(200, 800))
        at_patience      = random.randint(800, 1600)  # boards differ in patience

        # Clamp all values to 0–2000
        manager_satisf   = max(0, min(2000, manager_satisf))
        squad_satisf     = max(0, min(2000, squad_satisf))
        economic_confid  = max(0, min(2000, economic_confid))
        at_patience      = max(0, min(2000, at_patience))

        cur.execute("""
            INSERT INTO clubs_board (manager_satisf, squad_satisf, economic_confid, at_patience, club_id)
            VALUES (?, ?, ?, ?, ?)
        """, (manager_satisf, squad_satisf, economic_confid, at_patience, club_id))

    conn.commit()
    conn.close()
    print(f"✅ Populated clubs_board for {len(clubs)} clubs")


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


def depopulate_transfers_log():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM transfers_log")
    cur.execute("DELETE FROM sqlite_sequence WHERE name='transfers_log'")
    conn.commit()
    conn.close()
    print("✅ transfers_log depopulated.")


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





def fame_bias_for_attributes(club_fame: int) -> float:
    # Moderate fame bias: 0.85 (low fame) → 1.15 (high fame)
    return clamp(0.85 + (club_fame - 1000) / 4000.0, 0.85, 1.15)

def fame_bias_for_potential(club_fame: int) -> float:
    # Slightly stronger on potential to separate big/small clubs
    return clamp(0.85 + (club_fame - 1000) / 3500.0, 0.8, 1.2)



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
        WHERE pc.contract_end = ? AND p.is_retired = 0 and pc.is_terminated = 0
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
        start_year = game_date.year  # since contract_start is 1 Sep
        contract_start = date(start_year, 9, 1)
        contract_end = date(start_year + random.randint(1, 3), 8, 31)

        cur.execute("""
            INSERT INTO players_contract (
                player_id, club_id, contract_type, contract_start, contract_end, wage
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (player_id, club_id, "Renewed Contract",
              contract_start.isoformat(), contract_end.isoformat(), wage))

        #print(f"📝 Renewed contract for player {player_id} at club {club_id}: {wage}€/year until {contract_end}")

    conn.commit()
    
def renew_expired_staff_contracts(conn, game_date):
    """
    Renew staff contracts that end on the given game_date (e.g., 31 Aug).
    """
    cur = conn.cursor()

    cur.execute("""
        SELECT sc.staff_id, sc.club_id, s.role, s.fame
        FROM staff_contract sc
        JOIN staff s ON sc.staff_id = s.id
        WHERE sc.contract_end = ? and is_terminated = 0
    """, (game_date.isoformat(),))

    expired_staff = cur.fetchall()
    if not expired_staff:
        print("📄 No staff contracts to renew.")
        return

    for staff_id, club_id, role, fame in expired_staff:
        # Wage scaling depending on role + fame
        if role == "Physio":
            wage = random.randint(50_000, 200_000)
        elif role == "Medical":
            wage = random.randint(80_000, 250_000)
        elif role == "Scout":
            wage = random.randint(100_000, 400_000)
        elif role == "Assistant Coach":
            wage = random.randint(200_000, 600_000)
        elif role == "Manager":
            fame_factor = (fame / 2000.0) if fame else 1.0
            wage = int(random.randint(500_000, 2_000_000) * fame_factor)
        elif role == "Coach":
            wage = random.randint(100_000, 400_000)
        else:
            wage = random.randint(50_000, 300_000)

        # New contract starts in the current year (1 Sep)
        start_year = game_date.year
        contract_start = date(start_year, 9, 1)
        contract_end = date(start_year + random.randint(1, 3), 8, 31)

        cur.execute("""
            INSERT INTO staff_contract (
                staff_id, club_id, contract_type, contract_start, contract_end, wage
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (staff_id, club_id, "Renewed Contract",
              contract_start.isoformat(), contract_end.isoformat(), wage))

        #print(f"📝 Renewed contract for staff {staff_id} ({role}) at club {club_id}: {wage}€/year until {contract_end}")

    conn.commit()


def calculate_staff_fame(age, club_fame, role):
    # Managers and Assistant Coaches get more fame
    role_mult = {
        "Manager": 1.0,
        "Assistant Coach": 0.8,
        "Coach": 0.6,
        "Physio": 0.4,
        "Medical": 0.4,
        "Scout": 0.5
    }.get(role, 0.5)
    # Age: peak at 45-55
    if age < 30:
        age_mult = 0.5
    elif age < 40:
        age_mult = 0.8
    elif age < 56:
        age_mult = 1.0
    elif age < 65:
        age_mult = 0.7
    else:
        age_mult = 0.4
    # Club fame normalized (0.5–1.0)
    club_mult = 0.5 + (club_fame / 4000.0)
    fame = 2000 * role_mult * age_mult * club_mult
    return int(max(1, min(fame, 2000)))









def maybe_convert_to_staff(conn, player_id):
    cur = conn.cursor()

    cur.execute("""
        SELECT club_id, first_name, last_name, fame, peak_fame,
               position, date_of_birth, nationality, second_nationality
        FROM players WHERE id = ?
    """, (player_id,))
    row = cur.fetchone()
    if not row:
        return
    _club_id, first_name, last_name, fame, peak_fame, pos, dob, nat, nat2 = row

    # Base chance
    chance = 0.30
    if fame > 800 or peak_fame > 1000:
        chance += 0.20
    if random.random() > chance:
        return

    # Role choices
    roles = []
    if pos == "GK":
        roles.append("Goalkeeping Coach")
    elif pos in ("ST", "CF", "FW"):
        roles.append("Attacking Coach")
    elif pos in ("CM", "CDM", "CAM", "RM", "LM"):
        roles.append("Tactical Coach")
    else:
        roles.append("Coach")

    if peak_fame > 700:
        roles.append("Assistant Manager")
    if peak_fame > 500:
        roles.append("Scout")
    if peak_fame > 800:
        roles.append("Manager")

    # Flat 2% legendary chance
    if random.random() < 0.02:
        roles.append("Manager")

    role = random.choice(roles)
    
    # 🎯 Recalculate fame for regen staff based on past fame and role
    if role == "Manager":
        fame = int(random.randint(400, 700) + peak_fame * 0.15)
    elif role in ("Assistant Manager", "Attacking Coach", "Tactical Coach"):
        fame = int(random.randint(350, 600) + peak_fame * 0.1)
    elif role == "Goalkeeping Coach":
        fame = int(random.randint(300, 550) + peak_fame * 0.08)
    else:
        fame = int(random.randint(300, 500) + peak_fame * 0.05)
    
    # Clamp within reasonable limits
    fame = max(300, min(fame, 1500))
    
    preferred_formation = random.choice(FORMATIONS)


    # Insert into staff...
    cur.execute("""
        INSERT INTO staff (first_name, last_name, date_of_birth, nationality,
                           second_nationality, role, fame, club_id, former_player_id, preferred_formation)
        VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
    """, (first_name, last_name, dob, nat, nat2, role, fame, player_id, preferred_formation))
    staff_id = cur.lastrowid

    # Carry over attributes as before...
    cur.execute("SELECT at_goalkeeping, at_defending, at_passing, at_scoring FROM players_attr WHERE player_id = ?", (player_id,))
    gk, tac, pas, sho = cur.fetchone() or (0, 0, 0, 0)
    
    
    # Base staff ability, with realistic spread
    base = random.randint(750, 1250) + (peak_fame // 10)
    base = min(base, 1700)

    if role == "Manager":
        gk_attr = gk // 3
        tac_attr = base + random.randint(50, 150)
        pas_attr = pas // 2
        sho_attr = sho // 2
        at_scouting = base // 2 + random.randint(100, 300)
    elif role == "Assistant Manager":
        gk_attr = gk // 3
        tac_attr = base
        pas_attr = pas // 2
        sho_attr = sho // 2
        at_scouting = base // 3 + random.randint(50, 200)
    elif role == "Scout":
        gk_attr = gk // 4
        tac_attr = tac // 3
        pas_attr = pas // 3
        sho_attr = sho // 3
        at_scouting = base + random.randint(100, 400)
    elif role == "Goalkeeping Coach":
        gk_attr = base + random.randint(50, 200)
        tac_attr = tac // 2
        pas_attr = pas // 2
        sho_attr = sho // 2
        at_scouting = base // 4 + random.randint(20, 100)
    else:  # generic Coach
        gk_attr = gk // 2
        tac_attr = base
        pas_attr = base // 2
        sho_attr = base // 2
        at_scouting = base // 5 + random.randint(20, 80)

    # Curr/pot ability for staff
    curr_ability = max(700, base + random.randint(-100, 150))
    pot_ability  = min(2000, curr_ability + random.randint(300, 700))

    # Insert into staff_attr (with new fields)
    cur.execute("""
        INSERT INTO staff_attr (staff_id, at_goalkeeping, at_tackling, at_passing,
                                at_shooting, at_physio, at_medical, at_scouting,
                                at_curr_ability, at_pot_ability)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        staff_id,
        gk_attr,
        tac_attr,
        pas_attr,
        sho_attr,
        random.randint(400, 900),  # physio
        random.randint(400, 900),  # medical
        at_scouting,
        curr_ability,
        pot_ability
    ))

    conn.commit()
    print(f"👔 {first_name} {last_name} retired and became an UNEMPLOYED {role} (staff_id={staff_id})")



# -----------------------------
# Weekly progression
# -----------------------------

def update_staff_in_db(conn, game_date):
    cur = conn.cursor()
    cur.execute("""
        SELECT s.id, s.date_of_birth, s.role, s.club_id,
               sa.at_curr_ability, sa.at_pot_ability
        FROM staff s
        JOIN staff_attr sa ON s.id = sa.staff_id
        WHERE s.is_retired = 0
    """)
    staff = cur.fetchall()

    for staff_id, birth_date, role, club_id, curr_ability, pot_ability in staff:
        age = calculate_age(birth_date, game_date)

        # 🎲 Retirement chance (fixed so nobody retires before 65)
        retire = False
        if age >= 75:
            retire = True
        elif age >= 70:
            chance = 0.25 if club_id else 0.50  # 25% if employed, 50% if unemployed
            if random.random() < chance:
                retire = True
        elif age >= 65:
            chance = 0.10 if club_id else 0.20  # small chance starts only at 65
            if random.random() < chance:
                retire = True

        if retire:
            cur.execute("""
                UPDATE staff
                SET is_retired = 1, club_id = NULL
                WHERE id = ?
            """, (staff_id,))
            print(f"👴 Staff {staff_id} retired at age {age} ({role})")
            continue

        # --- Otherwise develop/decline ---
        dev_gap = max(0, pot_ability - curr_ability)
        if club_id is None:
            if age < 45:
                growth = dev_gap * 0.001 * random.uniform(0.5, 1.0)
            elif age < 60:
                growth = -curr_ability * 0.002 * random.uniform(0.8, 1.2)
            else:
                growth = -curr_ability * 0.006 * random.uniform(0.8, 1.2)
        else:
            if age < 40:
                growth = dev_gap * 0.005 * random.uniform(0.8, 1.2)
            elif age < 55:
                growth = dev_gap * 0.002 * random.uniform(0.8, 1.2)
            elif age < 65:
                growth = -curr_ability * 0.004 * random.uniform(0.8, 1.2)
            else:
                growth = -curr_ability * 0.01 * random.uniform(0.8, 1.2)

        new_curr_ability = int(clamp(curr_ability + growth, 100, min(pot_ability, 2000)))

        cur.execute("""
            UPDATE staff_attr
            SET at_curr_ability=?
            WHERE staff_id=?
        """, (new_curr_ability, staff_id))

    conn.commit()


def update_players_in_db(conn, game_date):
    cur = conn.cursor()

    if isinstance(game_date, str):
        game_date = datetime.datetime.strptime(game_date, "%Y-%m-%d").date()

    cur.execute("""
        SELECT p.id, p.date_of_birth, p.position, p.club_id,
               pa.at_curr_ability, pa.at_pot_ability,
               pa.at_selfcont, pa.at_honour, pa.at_crazyness, pa.at_working, pa.at_sexatract,
               pa.at_speed, pa.at_dribbling, pa.at_defending,
               pa.at_passing, pa.at_scoring, pa.at_goalkeeping
        FROM players p
        JOIN players_attr pa ON p.id = pa.player_id
        WHERE p.is_retired = 0
    """)
    players = cur.fetchall()

    staff_cache = {}

    def clamp(x, lo=100, hi=2000):
        return int(max(lo, min(hi, round(x))))

    for player in players:
        (player_id, birth_date, pos, club_id, curr_ability, pot_ability,
         selfcont, honour, crazyness, working, sexatract,
         speed, dribbling, defending, passing, scoring, goalkeeping) = player

        age = calculate_age(birth_date, game_date)

        # Retirement
        if age > 35:
            cur.execute("UPDATE players SET is_retired=1, value=0 WHERE id=?", (player_id,))
            maybe_convert_to_staff(conn, player_id)
            continue

        # --- Base growth (slower) ---
        dev_gap = max(0, pot_ability - curr_ability)
        if age < 20:
            growth = dev_gap * 0.010
        elif age < 23:
            growth = dev_gap * 0.008
        elif age < 27:
            growth = dev_gap * 0.004
        elif age < 30:
            growth = dev_gap * 0.001
        else:
            growth = -curr_ability * 0.0015  # gentle decline

        # Staff multipliers (cached)
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

        # --- Cap yearly ability change ---
        raw_new_ca = curr_ability + growth
        raw_new_ca = max(100, min(raw_new_ca, pot_ability))
        ca_delta = raw_new_ca - curr_ability

        # Hard caps by age band to avoid spikes
        if age < 20:
            cap_up = 8
        elif age < 23:
            cap_up = 6
        elif age < 27:
            cap_up = 4
        elif age < 30:
            cap_up = 2
        else:
            cap_up = 0  # no growth after 30 (decline allowed)

        cap_down = -4 if age >= 30 else -2
        ca_delta = max(cap_down, min(ca_delta, cap_up))
        new_curr_ability = int(round(curr_ability + ca_delta))

        # Relative growth factor for attributes
        growth_factor = (new_curr_ability - curr_ability) / max(1, curr_ability)

        # --- Key attributes by position ---
        if pos == "GK":
            key_attrs = {"goalkeeping", "defending"}
        elif pos in ("CB", "LB", "RB", "CDM"):
            key_attrs = {"defending", "passing"}
        elif pos in ("CM", "LM", "RM", "CAM", "AM"):
            key_attrs = {"passing", "dribbling"}
        else:  # forwards
            key_attrs = {"scoring", "dribbling"}

        # --- Mental progression (guaranteed small steps) ---
        # Use % of headroom with min/max step so rounding never kills it.
        def step_up(value, rate, max_step):
            head = 2000 - value
            delta = max(1, min(max_step, head * rate))
            return clamp(value + delta)

        def step_down(value, rate, max_step):
            excess = value - 100
            delta = max(1, min(max_step, excess * rate))
            return clamp(value - delta)

        selfcont   = step_up(selfcont,   0.004, 5)                 # +1..5
        honour     = step_up(honour,     0.003, 4)                 # +1..4
        crazyness  = step_down(crazyness,0.002, 3)                 # -1..3
        working    = step_up(working,    0.004 * mult["def"], 5)   # +1..5, staff helps
        sexatract  = step_down(sexatract,0.003, 5)                 # -1..5

        # --- Technical / physical progression ---
        attr_map = {
            "speed": speed,
            "dribbling": dribbling,
            "defending": defending,
            "passing": passing,
            "scoring": scoring,
            "goalkeeping": goalkeeping
        }

        new_attrs = {}
        for name, value in attr_map.items():
            if name in key_attrs:
                # Follow CA growth but modestly; add small randomness
                delta = value * growth_factor * random.uniform(0.5, 0.8)
                delta = max(-5, min(8, delta))  # hard cap per year
            else:
                # Secondary slower
                delta = value * growth_factor * random.uniform(0.1, 0.3)
                # Aging drag on secondary after 30
                if age > 30:
                    delta -= value * 0.0008 * (age - 30)
                delta = max(-4, min(5, delta))
            new_attrs[name] = clamp(value + delta)

        # Fame / value
        club_fame = cur.execute("SELECT fame FROM clubs WHERE id=?", (club_id,)).fetchone()
        fame_val = club_fame[0] if club_fame else 1000
        value = calculate_player_value(new_curr_ability, pot_ability, age, fame=fame_val)
        fame = calculate_player_fame(age, new_curr_ability, fame_val)

        # Apply updates
        cur.execute("""
            UPDATE players_attr SET
                at_selfcont=?, at_honour=?, at_crazyness=?, at_working=?, at_sexatract=?,
                at_speed=?, at_dribbling=?, at_defending=?, at_passing=?,
                at_scoring=?, at_goalkeeping=?, at_curr_ability=?
            WHERE player_id=?
        """, (
            selfcont, honour, crazyness, working, sexatract,
            new_attrs["speed"], new_attrs["dribbling"], new_attrs["defending"],
            new_attrs["passing"], new_attrs["scoring"], new_attrs["goalkeeping"],
            new_curr_ability, player_id
        ))

        cur.execute("UPDATE players SET value=?, fame=? WHERE id=?", (value, fame, player_id))

    conn.commit()








# -----------------------------
# Match simulation
# -----------------------------
def get_club_fame(cur, club_id):
    cur.execute("SELECT fame FROM clubs WHERE id = ?", (club_id,))
    row = cur.fetchone()
    return row[0] if row else 1000









# def apply_consequence(player_id, conseq_id, conn):
#     cur = conn.cursor()
#     cur.execute("SELECT at_curr_ability, at_pot_ability FROM players_attr WHERE player_id=?", (player_id,))
#     row = cur.fetchone()
#     if not row:
#         return
#     curr_ability, pot_ability = row
#     if curr_ability > pot_ability:
#         return
#     cur.execute("""
#         SELECT COALESCE(conseq_luck,0), COALESCE(conseq_selfcont,0), COALESCE(conseq_honour,0),
#                COALESCE(conseq_crazyness,0), COALESCE(conseq_working,0), COALESCE(conseq_sexatract,0),
#                COALESCE(conseq_friendship,0), COALESCE(conseq_speed,0), COALESCE(conseq_dribbling,0),
#                COALESCE(conseq_defending,0), COALESCE(conseq_passing,0), COALESCE(conseq_scoring,0),
#                COALESCE(conseq_happiness,0), COALESCE(conseq_confidence,0), COALESCE(conseq_hope,0)
#         FROM consequences WHERE conseq_id=?
#     """, (conseq_id,))
#     deltas = cur.fetchone()
#     if not deltas:
#         return
#     total_delta = sum(deltas)
#     if curr_ability + total_delta > pot_ability:
#         return
#     cur.execute("""
#         UPDATE players_attr
#         SET at_luck = MIN(at_luck + ?, 2000),
#             at_selfcont = MIN(at_selfcont + ?, 2000),
#             at_honour = MIN(at_honour + ?, 2000),
#             at_crazyness = MIN(at_crazyness + ?, 2000),
#             at_working = MIN(at_working + ?, 2000),
#             at_sexatract = MIN(at_sexatract + ?, 2000),
#             at_friendship = MIN(at_friendship + ?, 2000),
#             at_speed = MIN(at_speed + ?, 2000),
#             at_dribbling = MIN(at_dribbling + ?, 2000),
#             at_defending = MIN(at_defending + ?, 2000),
#             at_passing = MIN(at_passing + ?, 2000),
#             at_scoring = MIN(at_scoring + ?, 2000),
#             at_happiness = MIN(at_happiness + ?, 2000),
#             at_confidence = MIN(at_confidence + ?, 2000),
#             at_hope = MIN(at_hope + ?, 2000),
#             at_curr_ability = MIN(at_curr_ability + ?, ?)
#         WHERE player_id=?
#     """, (*deltas, total_delta, pot_ability, player_id))
#     conn.commit()

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

    last_season = get_last_season(conn)

    # --- Bottom 3 from League 1 ---
    cur.execute("""
        WITH season_matches AS (
            SELECT home_club_id, away_club_id, home_goals, away_goals, competition_id, season
            FROM fixtures
            WHERE competition_id = 1 AND played = 1
        ),
        club_stats AS (
            SELECT
                c.id AS club_id,
                sm.season,
                SUM(CASE 
                        WHEN sm.home_club_id = c.id AND sm.home_goals > sm.away_goals THEN 1
                        WHEN sm.away_club_id = c.id AND sm.away_goals > sm.home_goals THEN 1
                        ELSE 0 END) AS wins,
                SUM(CASE 
                        WHEN sm.home_goals = sm.away_goals 
                             AND (sm.home_club_id = c.id OR sm.away_club_id = c.id) THEN 1
                        ELSE 0 END) AS draws,
                SUM(CASE 
                        WHEN sm.home_club_id = c.id AND sm.home_goals < sm.away_goals THEN 1
                        WHEN sm.away_club_id = c.id AND sm.away_goals < sm.home_goals THEN 1
                        ELSE 0 END) AS losses,
                SUM(CASE 
                        WHEN sm.home_club_id = c.id AND sm.home_goals > sm.away_goals THEN 3
                        WHEN sm.away_club_id = c.id AND sm.away_goals > sm.home_goals THEN 3
                        WHEN sm.home_goals = sm.away_goals 
                             AND (sm.home_club_id = c.id OR sm.away_club_id = c.id) THEN 1
                        ELSE 0 END) AS points,
                SUM(CASE WHEN sm.home_club_id = c.id THEN sm.home_goals 
                         WHEN sm.away_club_id = c.id THEN sm.away_goals ELSE 0 END) AS goals_for,
                SUM(CASE WHEN sm.home_club_id = c.id THEN sm.away_goals 
                         WHEN sm.away_club_id = c.id THEN sm.home_goals ELSE 0 END) AS goals_against
            FROM clubs c
            JOIN season_matches sm 
              ON sm.home_club_id = c.id OR sm.away_club_id = c.id
            GROUP BY c.id, sm.season
        )
        SELECT c.id, c.name, cs.points, cs.goals_for, cs.goals_against
        FROM club_stats cs
        JOIN clubs c ON c.id = cs.club_id
        WHERE cs.season = ?
        ORDER BY cs.points ASC, (cs.goals_for - cs.goals_against) ASC, cs.goals_for ASC, c.name
        LIMIT 3;
    """, (last_season,))
    relegated = cur.fetchall()

    # --- Top 3 from League 2 ---
    cur.execute("""
        WITH season_matches AS (
            SELECT home_club_id, away_club_id, home_goals, away_goals, competition_id, season
            FROM fixtures
            WHERE competition_id = 2 AND played = 1
        ),
        club_stats AS (
            SELECT
                c.id AS club_id,
                sm.season,
                SUM(CASE 
                        WHEN sm.home_club_id = c.id AND sm.home_goals > sm.away_goals THEN 1
                        WHEN sm.away_club_id = c.id AND sm.away_goals > sm.home_goals THEN 1
                        ELSE 0 END) AS wins,
                SUM(CASE 
                        WHEN sm.home_goals = sm.away_goals 
                             AND (sm.home_club_id = c.id OR sm.away_club_id = c.id) THEN 1
                        ELSE 0 END) AS draws,
                SUM(CASE 
                        WHEN sm.home_club_id = c.id AND sm.home_goals < sm.away_goals THEN 1
                        WHEN sm.away_club_id = c.id AND sm.away_goals < sm.home_goals THEN 1
                        ELSE 0 END) AS losses,
                SUM(CASE 
                        WHEN sm.home_club_id = c.id AND sm.home_goals > sm.away_goals THEN 3
                        WHEN sm.away_club_id = c.id AND sm.away_goals > sm.home_goals THEN 3
                        WHEN sm.home_goals = sm.away_goals 
                             AND (sm.home_club_id = c.id OR sm.away_club_id = c.id) THEN 1
                        ELSE 0 END) AS points,
                SUM(CASE WHEN sm.home_club_id = c.id THEN sm.home_goals 
                         WHEN sm.away_club_id = c.id THEN sm.away_goals ELSE 0 END) AS goals_for,
                SUM(CASE WHEN sm.home_club_id = c.id THEN sm.away_goals 
                         WHEN sm.away_club_id = c.id THEN sm.home_goals ELSE 0 END) AS goals_against
            FROM clubs c
            JOIN season_matches sm 
              ON sm.home_club_id = c.id OR sm.away_club_id = c.id
            GROUP BY c.id, sm.season
        )
        SELECT c.id, c.name, cs.points, cs.goals_for, cs.goals_against
        FROM club_stats cs
        JOIN clubs c ON c.id = cs.club_id
        WHERE cs.season = ?
        ORDER BY cs.points DESC, (cs.goals_for - cs.goals_against) DESC, cs.goals_for DESC, c.name
        LIMIT 3;
    """, (last_season,))
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
    print(f"✅ Promotion/Relegation complete for season {last_season}")


def get_last_season(conn):
    """
    Return the most recent finished season string, e.g. '2025/26'.
    """
    cur = conn.cursor()
    cur.execute("SELECT MAX(season) FROM fixtures WHERE played = 1")
    return cur.fetchone()[0]



def game_loop():
    global GAME_DATE, LEAGUE_ATK_MEAN, LEAGUE_DEF_MEAN
        
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM global_val WHERE var_name='GAME_DATE'")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO global_val (var_name, value_date) VALUES (?, ?)", ("GAME_DATE", GAME_DATE.isoformat()))
        conn.commit()

    print(f"Game started on {GAME_DATE}. Press Enter to tick a day, M for a month, Y for a year, or Q to quit.")
    while True:
        user_input = input("Press Enter (1 day), M (1 month), Y (1 year), or Q to quit: ").strip().lower()
        if user_input == "q":
            print("Quitting the game...")
            break
        elif user_input == "m":

            
          
            
            end_date = advance_game_month(GAME_DATE)
            while GAME_DATE < end_date:
            
                if GAME_DATE.day == 1:
                    process_monthly_finances(conn, GAME_DATE)  
                    
                update_game_date_db()
                
                # Every day we run the decision making for each club
                decision_making.decision_making_func(GAME_DATE)
                
                
                simulate_fixtures_for_day(conn, GAME_DATE)
                if GAME_DATE.month == 8 and GAME_DATE.day == 31:
                    
                    # Screenshot of the tables once a year
                    for table in SNAPSHOT_TABLES:
                        snapshot_table(table, GAME_DATE)
                        
                    # End-of-season board review

                    season_end_board_adjustments(conn, SEASON)
                    
                    handle_promotion_relegation()
                    print("📅 End of season! Resetting fixtures...")
                    populate_fixtures(1)
                    populate_fixtures(2)
                    cup_manage(3)
                    LEAGUE_ATK_MEAN = None
                    LEAGUE_DEF_MEAN = None
                    print("✅ New season fixtures generated!")
                    
                    renew_expired_contracts(conn, GAME_DATE)        # players
                    renew_expired_staff_contracts(conn, GAME_DATE)  # staff
                    
                if GAME_DATE.weekday() == 4:
                    cup_manage(3)

                GAME_DATE = advance_game_day(GAME_DATE)
                cur.execute("UPDATE global_val SET value_date=? WHERE var_name='GAME_DATE'", (GAME_DATE.isoformat(),))
                conn.commit()
                if GAME_DATE.weekday() == 0:
                    update_players_in_db(conn, GAME_DATE)
                    update_staff_in_db(conn, GAME_DATE)
                print(f"Game Date: {GAME_DATE}")
        elif user_input == "y":

            
          
            
            end_date = advance_game_year(GAME_DATE)
            while GAME_DATE < end_date:
                if GAME_DATE.day == 1:
                    process_monthly_finances(conn, GAME_DATE)    
                

                
                update_game_date_db()
                
                # Every day we run the decision making for each club
                decision_making.decision_making_func(GAME_DATE)                

                
                simulate_fixtures_for_day(conn, GAME_DATE)
                if GAME_DATE.month == 8 and GAME_DATE.day == 31:
                    
                    # Screenshot of the tables once a year
                    for table in SNAPSHOT_TABLES:
                        snapshot_table(table, GAME_DATE)
                    
                    # End-of-season board review

                    season_end_board_adjustments(conn, SEASON)
                    
                    handle_promotion_relegation()
                    print("📅 End of season! Resetting fixtures...")
                    populate_fixtures(1)
                    populate_fixtures(2)
                    cup_manage(3)
                    LEAGUE_ATK_MEAN = None
                    LEAGUE_DEF_MEAN = None
                    print("✅ New season fixtures generated!")
                    
                    renew_expired_contracts(conn, GAME_DATE)        # players
                    renew_expired_staff_contracts(conn, GAME_DATE)  # staff
                    
                if GAME_DATE.weekday() == 4:
                    cup_manage(3)
                    
                GAME_DATE = advance_game_day(GAME_DATE)
                cur.execute("UPDATE global_val SET value_date=? WHERE var_name='GAME_DATE'", (GAME_DATE.isoformat(),))
                conn.commit()
                if GAME_DATE.weekday() == 0:
                    update_players_in_db(conn, GAME_DATE)
                    update_staff_in_db(conn, GAME_DATE)
                print(f"Game Date: {GAME_DATE}")
        else:

            if GAME_DATE.day == 1:
                process_monthly_finances(conn, GAME_DATE)              
            
            update_game_date_db()
            
            # Every day we run the decision making for each club
            decision_making.decision_making_func(GAME_DATE)
            
            simulate_fixtures_for_day(conn, GAME_DATE)
            if GAME_DATE.month == 8 and GAME_DATE.day == 31:
                
                # Screenshot of the tables once a year
                for table in SNAPSHOT_TABLES:
                    snapshot_table(table, GAME_DATE)
                
                # End-of-season board review

                season_end_board_adjustments(conn, SEASON)
                
                handle_promotion_relegation()
                print("📅 End of season! Resetting fixtures...")
                populate_fixtures(1)
                populate_fixtures(2)
                cup_manage(3)
                LEAGUE_ATK_MEAN = None
                LEAGUE_DEF_MEAN = None
                print("✅ New season fixtures generated!")
                
                renew_expired_contracts(conn, GAME_DATE)        # players
                renew_expired_staff_contracts(conn, GAME_DATE)  # staff
                
            if GAME_DATE.weekday() == 4:
                cup_manage(3)

            GAME_DATE = advance_game_day(GAME_DATE)
            cur.execute("UPDATE global_val SET value_date=? WHERE var_name='GAME_DATE'", (GAME_DATE.isoformat(),))
            conn.commit()
            if GAME_DATE.weekday() == 0:
                update_players_in_db(conn, GAME_DATE)
                update_staff_in_db(conn, GAME_DATE)
            print(f"Game Date: {GAME_DATE}")


    conn.close()






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

    # --- Clubs ---
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

    base_roles = ["Manager", "Assistant Coach", "Physio", "Medical", "Scout"]

    staff_insert, contracts_insert, attrs_insert = [], [], []

    # --- Staff per club ---
    for club_id, club_fame, club_country in clubs:
        roles = base_roles + ["Goalkeeping Coach"] + ["Coach"] * 2

        for role in roles:
            faker = Faker()
            first_name = faker.first_name_male()
            last_name = faker.last_name()

            # Nationality (90% local, 10% foreign)
            nationality = club_country if random.random() < 0.9 else random.choice(
                ["England", "Argentina", "Spain", "Germany", "Netherlands", "France", "Italy"]
            )
            second_nationality = faker.country() if random.random() < 0.1 else None

            # Age 30–55 (younger than before so they don’t all retire early)
            age = random.randint(30, 55)
            date_of_birth = date(GAME_DATE.year - age,
                                 random.randint(1, 12),
                                 random.randint(1, 28))

            staff_fame = calculate_staff_fame(age, club_fame, role)
            
            preferred_formation = random.choice(FORMATIONS)

            staff_insert.append((
                first_name, last_name, date_of_birth.isoformat(),
                nationality, second_nationality, role, staff_fame, club_id,
                preferred_formation
            ))

            # Contract (Sept 1 – Aug 31)
            start_year = GAME_DATE.year - random.randint(0, 2)
            contract_start = date(start_year, 9, 1)
            end_year = GAME_DATE.year + random.randint(1, 3)
            contract_end = date(end_year, 8, 31)

            wage = random.randint(150_000, 600_000) if role in ("Assistant Coach", "Coach") else \
                   random.randint(500_000, 2_000_000) if role == "Manager" else \
                   random.randint(80_000, 400_000)

            contracts_insert.append((
                club_id, "Professional",
                contract_start.isoformat(), contract_end.isoformat(), wage
            ))

            attrs_insert.append(generate_staff_attributes(role))

    # --- Insert staff ---
    cur.executemany("""
        INSERT INTO staff (
            first_name, last_name, date_of_birth, nationality, second_nationality,
            role, fame, club_id, preferred_formation
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, staff_insert)

    last_rowid = cur.execute("SELECT last_insert_rowid()").fetchone()[0]
    start_id = last_rowid - len(staff_insert) + 1
    staff_ids = list(range(start_id, start_id + len(staff_insert)))

    # Contracts
    contracts_with_ids = [(sid, *c) for sid, c in zip(staff_ids, contracts_insert)]
    cur.executemany("""
        INSERT INTO staff_contract (
            staff_id, club_id, contract_type, contract_start, contract_end, wage
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, contracts_with_ids)

    # Attributes
    attrs_with_ids = [(sid, *a) for sid, a in zip(staff_ids, attrs_insert)]
    cur.executemany("""
        INSERT INTO staff_attr (
            staff_id, at_goalkeeping, at_tackling, at_passing,
            at_shooting, at_physio, at_medical, at_scouting,
            at_curr_ability, at_pot_ability
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, attrs_with_ids)

    print(f"✅ {len(staff_insert)} staff generated for clubs")

    # --- Free agent staff pool (50) ---
    free_staff_insert, free_contracts_insert, free_attrs_insert = [], [], []
    roles_pool = ["Manager", "Assistant Coach", "Goalkeeping Coach", "Coach", "Physio", "Medical", "Scout"]

    for _ in range(50):
        role = random.choice(roles_pool)
        faker = Faker()
        first_name = faker.first_name_male()
        last_name = faker.last_name()

        nationality = faker.country()
        second_nationality = faker.country() if random.random() < 0.1 else None

        age = random.randint(28, 55)
        date_of_birth = date(GAME_DATE.year - age,
                             random.randint(1, 12),
                             random.randint(1, 28))

        staff_fame = random.randint(100, 800)
        
        preferred_formation = random.choice(FORMATIONS)


        free_staff_insert.append((
            first_name, last_name, date_of_birth.isoformat(),
            nationality, second_nationality, role, staff_fame, None, preferred_formation   
        ))

        free_contracts_insert.append((
            None, "Unemployed",
            GAME_DATE.isoformat(), None, 0
        ))

        free_attrs_insert.append(generate_staff_attributes(role))
        


    cur.executemany("""
        INSERT INTO staff (
            first_name, last_name, date_of_birth, nationality, second_nationality,
            role, fame, club_id, preferred_formation
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, free_staff_insert)

    last_rowid = cur.execute("SELECT last_insert_rowid()").fetchone()[0]
    start_id = last_rowid - len(free_staff_insert) + 1
    free_staff_ids = list(range(start_id, start_id + len(free_staff_insert)))


    # Attributes
    free_attrs_with_ids = [(sid, *a) for sid, a in zip(free_staff_ids, free_attrs_insert)]
    cur.executemany("""
        INSERT INTO staff_attr (
            staff_id, at_goalkeeping, at_tackling, at_passing,
            at_shooting, at_physio, at_medical, at_scouting,
            at_curr_ability, at_pot_ability
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, free_attrs_with_ids)

    conn.commit()
    conn.close()
    print(f"✅ {len(free_staff_insert)} free agent staff generated")




def generate_staff_attributes(role):
    """
    Generate realistic staff attributes (0–2000 scale),
    with role-specific strengths and higher potentials.
    """
    # Base scale
    #curr_ability = random.randint(800, 1400)
    #pot_ability = min(2000, curr_ability + random.randint(400, 900))
    
    base = random.randint(750, 1250)
    curr_ability = max(700, base + random.randint(-100, 150))
    pot_ability  = min(2000, curr_ability + random.randint(300, 700))

    if role == "Manager":
        at_goalkeeping = random.randint(200, 600)
        at_tackling = random.randint(800, 1400)
        at_passing = random.randint(800, 1400)
        at_shooting = random.randint(500, 1000)
        at_physio = random.randint(300, 700)
        at_medical = random.randint(300, 700)
        at_scouting = random.randint(900, 1600)
    elif role == "Assistant Coach":
        at_goalkeeping = random.randint(200, 700)
        at_tackling = random.randint(700, 1200)
        at_passing = random.randint(700, 1200)
        at_shooting = random.randint(500, 1000)
        at_physio = random.randint(300, 800)
        at_medical = random.randint(300, 800)
        at_scouting = random.randint(700, 1200)
    elif role == "Coach":
        at_goalkeeping = random.randint(300, 1000)
        at_tackling = random.randint(600, 1200)
        at_passing = random.randint(600, 1200)
        at_shooting = random.randint(600, 1200)
        at_physio = random.randint(300, 700)
        at_medical = random.randint(300, 700)
        at_scouting = random.randint(500, 1000)
    elif role == "Scout":
        at_goalkeeping = random.randint(100, 400)
        at_tackling = random.randint(200, 600)
        at_passing = random.randint(200, 600)
        at_shooting = random.randint(200, 600)
        at_physio = random.randint(200, 600)
        at_medical = random.randint(200, 600)
        at_scouting = random.randint(1000, 1800)  # ⭐ Scouts excel
    elif role == "Physio":
        at_goalkeeping = random.randint(100, 400)
        at_tackling = random.randint(100, 400)
        at_passing = random.randint(100, 400)
        at_shooting = random.randint(100, 400)
        at_physio = random.randint(1000, 1800)   # ⭐ Physio focus
        at_medical = random.randint(700, 1400)
        at_scouting = random.randint(200, 700)
    elif role == "Medical":
        at_goalkeeping = random.randint(100, 400)
        at_tackling = random.randint(100, 400)
        at_passing = random.randint(100, 400)
        at_shooting = random.randint(100, 400)
        at_physio = random.randint(700, 1400)
        at_medical = random.randint(1000, 1800)  # ⭐ Medical focus
        at_scouting = random.randint(200, 700)
    elif role == "Goalkeeping Coach":
        at_goalkeeping = random.randint(1200, 1800)   # ⭐ Specialization
        at_tackling    = random.randint(300, 800)
        at_passing     = random.randint(300, 800)
        at_shooting    = random.randint(300, 800)
        at_physio      = random.randint(300, 700)
        at_medical     = random.randint(300, 700)
        at_scouting    = random.randint(200, 600)
    else:  # fallback
        at_goalkeeping = random.randint(200, 800)
        at_tackling = random.randint(200, 800)
        at_passing = random.randint(200, 800)
        at_shooting = random.randint(200, 800)
        at_physio = random.randint(200, 800)
        at_medical = random.randint(200, 800)
        at_scouting = random.randint(200, 800)
        
    return (
        at_goalkeeping, at_tackling, at_passing, at_shooting,
        at_physio, at_medical, at_scouting,
        curr_ability, pot_ability
    )


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

def advance_game_month(current_date):
    # Advance to the same day next month, or last day if not possible
    try:
        return current_date + relativedelta(months=1)
    except Exception:
        # fallback: go to last day of next month
        next_month = current_date.month % 12 + 1
        year = current_date.year + (current_date.month // 12)
        last_day = (date(year, next_month % 12 + 1, 1) - timedelta(days=1)).day
        return date(year, next_month, last_day)


def advance_game_year(current_date):
    # Advance to the same day next year, or last day if not possible
    try:
        return current_date + relativedelta(years=1)
    except Exception:
        # fallback: go to last day of next year
        year = current_date.year + 1
        month = current_date.month
        last_day = (date(year, month % 12 + 1, 1) - timedelta(days=1)).day
        return date(year, month, last_day)


def create_histo_table(base_table, db_path=DB_PATH):
    """
    Create a _histo table with same columns as base_table
    plus screenshot_day DATE. Drops existing only at game start.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    histo_table = f"{base_table}_histo"
    cur.execute(f"DROP TABLE IF EXISTS {histo_table}")

    # Copy column definitions
    cur.execute(f"PRAGMA table_info({base_table})")
    col_defs = [f"{row[1]} {row[2]}" for row in cur.fetchall()]
    col_defs.append("screenshot_day DATE")

    ddl = f"CREATE TABLE {histo_table} ({', '.join(col_defs)})"
    cur.execute(ddl)

    conn.commit()
    conn.close()


def snapshot_table(base_table, game_date, db_path=DB_PATH):
    """
    Append rows from base_table into base_table_histo
    with GAME_DATE stamped into screenshot_day.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    histo_table = f"{base_table}_histo"

    # Get column names dynamically
    cur.execute(f"PRAGMA table_info({base_table})")
    cols = [row[1] for row in cur.fetchall()]
    col_list = ", ".join(cols)

    cur.execute(f"SELECT {col_list} FROM {base_table}")
    rows = cur.fetchall()
    rows_with_date = [tuple(row) + (game_date,) for row in rows]

    placeholders = ", ".join(["?"] * (len(cols) + 1))
    insert_sql = f"INSERT INTO {histo_table} ({col_list}, screenshot_day) VALUES ({placeholders})"
    cur.executemany(insert_sql, rows_with_date)

    conn.commit()
    conn.close()


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    # Uncomment to hard reset DB schema & base tables:
    init_db(DB_PATH, GAME_DATE)

    # If you reset DB, (optionally) load clubs from CSV:
    #depopulate_clubs()
    populate_clubs()
    initialize_club_balances()
    populate_clubs_board()

    populate_competition_clubs()

    update_game_date_db()           # keep GAME_DATE in DB in sync

    # Fresh players & fixtures each run (like your previous workflow)
    #depopulate_players()
    #populate_400_players()
    populate_all_players(DB_PATH, GAME_DATE, fakers)

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
    depopulate_transfers_log()

    # Optional: mini-situations system
    # init_db_possib()
    # clean_player_situ()
    # run_game(16)
    
    
    # Create historical tables
    for table in SNAPSHOT_TABLES:
        create_histo_table(table)   # reset fresh
        snapshot_table(table, GAME_DATE)  # first snapshot
    

    

    # Kick off loop
    game_loop()

    # Debug helpers:
    # print_table("players_attr")
    # test_regen_creation(3, position="GK")
