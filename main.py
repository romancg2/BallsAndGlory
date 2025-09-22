# main.py (a.k.a. fm_database_create_fixed.py)
import random
from tabulate import tabulate
from faker import Faker
from datetime import date
import sqlite3
from dateutil.relativedelta import relativedelta
import datetime

# ✅ import the whole config module to keep GAME_DATE live
import BallsAndGlory.config as config
from BallsAndGlory.config import  LEAGUE_ATK_MEAN, LEAGUE_DEF_MEAN

# DB/schema + population helpers
from BallsAndGlory.db.schema import init_db
from BallsAndGlory.db.population import (
    populate_clubs,
    populate_400_players,
    depopulate_players,
    depopulate_fixtures,
    populate_fixtures,
    generate_player,
    distribute_attributes,
    update_players_in_db,        # <-- now imported
    depopulate_match_scorers     # <-- now imported
)

fake = Faker()

# --- Fix for Python 3.12 date handling ---
sqlite3.register_adapter(datetime.date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime.datetime, lambda dt: dt.isoformat(" "))
sqlite3.register_converter("DATE", lambda s: datetime.date.fromisoformat(s.decode("utf-8")))
sqlite3.register_converter("DATETIME", lambda s: datetime.datetime.fromisoformat(s.decode("utf-8")))



def print_table(table_name):
    conn = sqlite3.connect(config.DB_PATH)
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
    print("🟢 Attributes updated successfully!")


def run_game(player_id):
    conn = sqlite3.connect(config.DB_PATH)
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
        for i, (sit_title, conseq_title, conseq_desc, dt) in enumerate(history, 1):
            print(f"\n{i}. [{dt}]")
            print(f"   ⚽ Situation: {sit_title}")
            print(f"   🎲 Result: {conseq_title}")
            print(f"   📝 {conseq_desc}")
    else:
        print("No history found.")

    conn.close()


def clean_player_situ():
    conn = sqlite3.connect(config.DB_PATH)
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


def advance_game_month(current_date):
    return current_date + relativedelta(months=1)

def advance_game_day(current_date):
    return current_date + relativedelta(days=1)

def calculate_age(birth_date, game_date):
    if isinstance(birth_date, str):
        birth_date = datetime.datetime.strptime(birth_date, "%Y-%m-%d").date()
    elif isinstance(birth_date, datetime.datetime):
        birth_date = birth_date.date()
    elif not isinstance(birth_date, datetime.date):
        raise TypeError("birth_date must be a string or date/datetime object")

    if isinstance(game_date, str):
        game_date = datetime.datetime.strptime(game_date, "%Y-%m-%d").date()
    elif isinstance(game_date, datetime.datetime):
        game_date = game_date.date()
    elif not isinstance(game_date, datetime.date):
        raise TypeError("game_date must be a string or date/datetime object")

    age = game_date.year - birth_date.year
    if (game_date.month, game_date.day) < (birth_date.month, birth_date.day):
        age -= 1
    return age


def update_game_date_db():
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "UPDATE global_val SET value_date = ? WHERE var_name = 'GAME_DATE'",
        (config.GAME_DATE.isoformat(),)
    )
    conn.commit()


def game_loop():
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM global_val WHERE var_name = 'GAME_DATE'")
    if cur.fetchone()[0] == 0:
        cur.execute(
            "INSERT INTO global_val (var_name, value_date) VALUES (?, ?)", 
            ("GAME_DATE", config.GAME_DATE.isoformat())
        )
        conn.commit()
        
    print(f"Game started on {config.GAME_DATE}. Press Enter to advance by one day, or Q to quit.")

    while True:
        user_input = input("Press Enter to advance the game by one day, or Q to quit: ")
        if user_input.lower() == "q":
            print("Quitting the game...")
            break

        # Play today's fixtures before advancing date
        simulate_fixtures_for_day(conn, config.GAME_DATE)

        # End of season rollover
        if config.GAME_DATE.month == 8 and config.GAME_DATE.day == 31:
            print("📅 End of season reached! Resetting fixtures for new season...")
            depopulate_fixtures()
            populate_fixtures()
            depopulate_match_scorers()
            print("✅ New season fixtures generated!")

        # Advance date (live in config)
        config.GAME_DATE = advance_game_day(config.GAME_DATE)

        cur.execute(
            "UPDATE global_val SET value_date = ? WHERE var_name = 'GAME_DATE'",
            (config.GAME_DATE.isoformat(),)
        )
        conn.commit()
        
        # Weekly attribute updates on Mondays
        if config.GAME_DATE.weekday() == 0:
            update_players_in_db(conn, config.GAME_DATE)

        print(f"Game Date: {config.GAME_DATE}")

    conn.close()


if __name__ == "__main__":
    # init_db()  # run once to (re)create schema if needed
    update_game_date_db()

    # Populate fresh season data
    # populate_clubs()  # run once; keep commented if clubs already inserted

    depopulate_players()
    populate_400_players()

    depopulate_fixtures()
    populate_fixtures()

    depopulate_match_scorers()

    # run_game(16)  # optional: interactive mini-game

    game_loop()
