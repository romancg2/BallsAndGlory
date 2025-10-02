import os
import random
import sqlite3
from datetime import date

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "db")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "fm_database.sqlite")


def decision_making_func(GAME_DATE):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT id, name, fame, current_balance_EUR FROM clubs")
    clubs = cur.fetchall()

    cur.execute("""
        SELECT p.id, p.first_name, p.last_name, p.position, p.value, p.fame
        FROM players p
        WHERE p.club_id IS NULL AND p.is_retired = 0
    """)
    free_players = cur.fetchall()

    cur.execute("""
        SELECT s.id, s.first_name, s.last_name, s.role, s.fame
        FROM staff s
        WHERE s.club_id IS NULL AND s.is_retired = 0
    """)
    free_staff = cur.fetchall()

    for club_id, club_name, club_fame, balance in clubs:
        #print(f"\n🏟️ {club_name} balance={balance}")

        # --- Players in club
        cur.execute("""
            SELECT position, COUNT(*)
            FROM players
            WHERE club_id=? AND is_retired=0
            GROUP BY position
        """, (club_id,))
        squad_counts = dict(cur.fetchall())
        #print("   Current squad:", squad_counts)

        # --- Staff in club
        cur.execute("""
            SELECT role, COUNT(*)
            FROM staff
            WHERE club_id=? AND is_retired=0
            GROUP BY role
        """, (club_id,))
        staff_counts = dict(cur.fetchall())
        #print("   Current staff:", staff_counts)

        # -----------------
        # PLAYER SIGNING
        # -----------------
        needs_player = []
        if squad_counts.get("GK", 0) < 3:
            needs_player.append("GK")
        if squad_counts.get("ST", 0) < 4:
            needs_player.append("ST")
        if squad_counts.get("RB", 0) < 4:
            needs_player.append("ST")
        if squad_counts.get("CM", 0) < 4:
            needs_player.append("ST")
                

        if needs_player:
            pos = random.choice(needs_player)
            candidates = [fp for fp in free_players if fp[3] == pos]
            #print(f"   Needs {pos}, candidates={len(candidates)}")

            if candidates and balance > 200_000:  # lower threshold
                pid, fn, ln, position, value, fame = random.choice(candidates)
                wage = max(100_000, int(value * 0.02))  # cheaper wage formula
                if wage < balance * 0.5:  # allow up to 50% of balance
                    contract_start = GAME_DATE
                    contract_end = date(GAME_DATE.year + 2, 8, 31)

                    cur.execute("UPDATE players SET club_id=? WHERE id=?", (club_id, pid))
                    cur.execute("""
                        INSERT INTO players_contract (player_id, club_id, contract_type, contract_start, contract_end, wage)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (pid, club_id, "Professional",
                          contract_start.isoformat(), contract_end.isoformat(), wage))

                    new_balance = balance - wage
                    cur.execute("UPDATE clubs SET current_balance_EUR=? WHERE id=?", (new_balance, club_id))

                    print(f"   ✅ Signed player {fn} {ln} ({position}), wage={wage}")
                else:
                    print(f"   ❌ Cannot afford player (wage {wage} > balance {balance})")

        # -----------------
        # STAFF SIGNING
        # -----------------
        staff_needs = []
        for role in ["Manager", "Physio", "Medical", "Scout", "Goalkeeping Coach"]:
            if staff_counts.get(role, 0) < 1:
                staff_needs.append(role)

        if staff_needs:
            role = random.choice(staff_needs)
            candidates = [fs for fs in free_staff if fs[3] == role]
            print(f"   Needs staff {role}, candidates={len(candidates)}")

            if candidates and balance > 100_000:
                sid, fn, ln, role, fame = random.choice(candidates)
                wage = random.randint(50_000, 150_000)
                if wage < balance * 0.3:  # allow up to 30% of balance
                    contract_start = GAME_DATE
                    contract_end = date(GAME_DATE.year + 2, 8, 31)

                    cur.execute("UPDATE staff SET club_id=? WHERE id=?", (club_id, sid))
                    cur.execute("""
                        INSERT INTO staff_contract (staff_id, club_id, contract_type, contract_start, contract_end, wage)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (sid, club_id, "Professional",
                          contract_start.isoformat(), contract_end.isoformat(), wage))

                    new_balance = balance - wage
                    cur.execute("UPDATE clubs SET current_balance_EUR=? WHERE id=?", (new_balance, club_id))

                    print(f"   ✅ Hired staff {fn} {ln} ({role}), wage={wage}")
                else:
                    print(f"   ❌ Cannot afford staff (wage {wage} > balance {balance})")

    conn.commit()
    conn.close()
