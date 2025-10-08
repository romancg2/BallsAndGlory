import os
import random
import sqlite3
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "db")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "fm_database.sqlite")

FAME_TOLERANCE = 0  # set >0 if you want to allow small downgrades (e.g., 50)
MIN_SQUAD = 18
MAX_SQUAD = 23




def decision_making_func(GAME_DATE):
    """
    Transfers + Staff:
      - COOLDOWN_DAYS=180 via transfers_log check
      - unique (player_id, ts) index
      - seller re-check right before move
      - commit after each successful transfer
    """


    COOLDOWN_DAYS = 180
    cutoff_date = (GAME_DATE - timedelta(days=COOLDOWN_DAYS)).isoformat()


    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()


    def club_balance(cid: int) -> int:
        row = cur.execute("SELECT current_balance_EUR FROM clubs WHERE id=?", (cid,)).fetchone()
        return row[0] if row else 0
    # -----------------------
    # Schema safety
    # -----------------------
    def ensure_transfers_log():
        cur.execute("""
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
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_transfers_log_ts ON transfers_log(ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_transfers_log_player ON transfers_log(player_id)")
        # prevent two moves for same player on the same day
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_transfers_log_player_day ON transfers_log(player_id, ts)")

    def ensure_players_last_transfer():
        cur.execute("PRAGMA table_info(players)")
        cols = {r[1] for r in cur.fetchall()}
        if "last_transfer_ts" not in cols:
            cur.execute("ALTER TABLE players ADD COLUMN last_transfer_ts DATE")

    ensure_transfers_log()
    ensure_players_last_transfer()

    # -----------------------
    # Helpers / config
    # -----------------------
    def is_window(d: date) -> bool:
        return d.month in (1, 7, 8)

    REQUIRED = {"GK": 3, "ST": 4, "RB": 2, "CM": 4}

    # How many "starters" per position to compare against (depth to beat)
    STARTERS = {"GK": 1, "RB": 1, "LB": 1, "CB": 2, "CM": 2, "RM": 1, "LM": 1, "ST": 2}
    DEFAULT_STARTERS = 1
    
    # Improvement thresholds (on 100–2000 scale)
    IMPROVE_ABS_DELTA = 35         # absolute points better than last starter
    IMPROVE_RATIO     = 1.03       # or ≥3% better    
    
    
    def key_attrs_for_pos(pos: str):
        """Return the list of key attribute column names for a position."""
        if pos == "GK":
            return ["at_goalkeeping", "at_defending"]
        if pos in ("CB", "RB", "LB"):
            return ["at_defending", "at_passing"]
        if pos in ("CM", "RM", "LM"):
            return ["at_passing", "at_dribbling"]
        # forwards / wingers
        return ["at_scoring", "at_dribbling", "at_speed"]
    
    def player_pos_score(pid: int, pos: str) -> int:
        """Role score = 60% Current Ability + 40% avg(key attributes)."""
        row = cur.execute(f"""
            SELECT pa.at_curr_ability, {", ".join(key_attrs_for_pos(pos))}
            FROM players_attr pa
            WHERE pa.player_id=?
        """, (pid,)).fetchone()
        if not row:
            return 0
        ca = row[0]
        keys = row[1:]
        key_avg = sum(keys) / len(keys) if keys else 0
        score = 0.60 * ca + 0.40 * key_avg
        return int(score)
    
    def club_pos_scores(club_id: int, pos: str):
        """Return [(player_id, score)] for club’s players in pos, sorted desc."""
        rows = cur.execute("""
            SELECT p.id
            FROM players p
            WHERE p.club_id=? AND p.position=? AND p.is_retired=0
        """, (club_id, pos)).fetchall()
        scores = [(pid, player_pos_score(pid, pos)) for (pid,) in rows]
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores
    
    def improves_team(club_id: int, pos: str, cand_pid: int) -> bool:
        """True if candidate clearly improves over the last starter in that position."""
        cand_score = player_pos_score(cand_pid, pos)
        starters_n = STARTERS.get(pos, DEFAULT_STARTERS)
        depth = club_pos_scores(club_id, pos)
        # If the club lacks enough players to fill starters, any decent candidate helps
        if len(depth) < starters_n:
            return True
        # Compare vs last starter (the lowest score among current starters)
        baseline = depth[min(starters_n, len(depth)) - 1][1]
        return cand_score >= baseline + IMPROVE_ABS_DELTA or cand_score >= baseline * IMPROVE_RATIO    

    def active_contract_end(pid: int):
        row = cur.execute("""
            SELECT contract_end
            FROM players_contract
            WHERE player_id=? AND is_terminated=0
            ORDER BY COALESCE(contract_end,'9999-12-31') DESC
            LIMIT 1
        """, (pid,)).fetchone()
        return row[0] if row else None

    def years_left(pid: int, today: date) -> float:
        end_s = active_contract_end(pid)
        if not end_s: return 0.0
        y, m, d = map(int, end_s.split("-"))
        end_d = date(y, m, d)
        return max(0.0, (end_d - today).days / 365.0)

    def ask_fee(value: int, y_left: float) -> int:
        return int(value * (1.00 + min(0.50, 0.25 * y_left)))  # 1.00..1.50× value

    def can_sell(seller_id: int, pos: str, keep_at_least: int) -> bool:
        (cnt,) = cur.execute("""
            SELECT COUNT(*) FROM players
            WHERE club_id=? AND position=? AND is_retired=0
        """, (seller_id, pos)).fetchone()
        return cnt > keep_at_least

    def recently_moved(pid: int) -> bool:
        # check log
        if cur.execute(
            "SELECT 1 FROM transfers_log WHERE player_id=? AND ts>=? LIMIT 1",
            (pid, cutoff_date)
        ).fetchone():
            return True
        # check players.last_transfer_ts
        row = cur.execute("SELECT last_transfer_ts FROM players WHERE id=?", (pid,)).fetchone()
        if row and row[0] and row[0] >= cutoff_date:
            return True
        return False
    
    def club_squad_count(cid: int) -> int:
        return cur.execute(
            "SELECT COUNT(*) FROM players WHERE club_id=? AND is_retired=0", (cid,)
        ).fetchone()[0]
    
    def safe_assign_to_club(pid: int, new_cid: int) -> bool:
        """
        Move player only if the target club is still under MAX_SQUAD *at update time*.
        Returns True if the row was updated (moved), False if capacity blocked it.
        """
        cur.execute("""
            UPDATE players
               SET club_id=?
             WHERE id=?
               AND (SELECT COUNT(*) FROM players WHERE club_id=? AND is_retired=0) < ?
        """, (new_cid, pid, new_cid, MAX_SQUAD))
        return cur.rowcount == 1    

    def mark_transfer(pid: int):
        cur.execute("UPDATE players SET last_transfer_ts=? WHERE id=?", (GAME_DATE.isoformat(), pid))

    # -----------------------
    # Player operations
    # -----------------------
    def sign_free_agent(club_id: int, club_name: str, pos: str, balance: int, today: date, moved_today: set) -> int:
        
        
        
        buyer_total = cur.execute(
            "SELECT COUNT(*) FROM players WHERE club_id=? AND is_retired=0", (club_id,)
        ).fetchone()[0]
        if buyer_total >= MAX_SQUAD:
            return balance
           
        
        # exclude cooldown + moved_today using both log and last_transfer_ts
        candidates = cur.execute("""
            SELECT p.id, p.first_name, p.last_name, p.position, p.value, pa.at_curr_ability
            FROM players p
            JOIN players_attr pa ON pa.player_id = p.id
            WHERE p.is_retired = 0
              AND p.club_id IS NULL
              AND p.position = ?
              AND COALESCE(
                    (SELECT MAX(tl.ts) FROM transfers_log tl WHERE tl.player_id = p.id),
                    '1900-01-01'
                  ) < ?
            ORDER BY pa.at_curr_ability DESC
            LIMIT 16
        """, (pos, cutoff_date)).fetchall()



        # Filter out moved-today / cooldown
        candidates = [row for row in candidates if row[0] not in moved_today]
        if not candidates or balance <= 200_000:
            return balance
        
        # Sort by role score (best first)
        scored = []
        for pid, fn, ln, position, value, ca in candidates:
            scored.append((player_pos_score(pid, position), pid, fn, ln, position, value, ca))
        scored.sort(reverse=True)
        
        # Check current depth
        buyer_total = cur.execute(
            "SELECT COUNT(*) FROM players WHERE club_id=? AND is_retired=0", (club_id,)
        ).fetchone()[0]
        pos_count = cur.execute(
            "SELECT COUNT(*) FROM players WHERE club_id=? AND position=? AND is_retired=0",
            (club_id, pos)
        ).fetchone()[0]
        need_pos = pos_count < REQUIRED.get(pos, 0)
        need_total = buyer_total < MIN_SQUAD
        
        # Try the best few that actually improve or fill a hard need
        for _, pid, fn, ln, position, value, ca in scored[:8]:
            if pid in moved_today or recently_moved(pid):
                continue
            
            # cur.execute("UPDATE players SET club_id=? WHERE id=?", (club_id, pid))
            if not safe_assign_to_club(pid, club_id):
                continue  # capacity filled between check and assignment, try next candidate                    
        
            wage = max(100_000, int(value * 0.02))
            has_active = cur.execute("""
                SELECT 1 FROM players_contract
                WHERE player_id=? AND is_terminated=0
                  AND (contract_end IS NULL OR contract_end >= ?)
            """, (pid, today.isoformat())).fetchone()
            if has_active or wage >= balance * 0.5:
                continue
        
            # Only sign if (we're short) OR (he improves starters in that position)
            if not (need_pos or need_total or improves_team(club_id, position, pid)):
                continue
        
            if buyer_total + 1 > MAX_SQUAD:
                continue
        
            # Do the move
            cur.execute("UPDATE players SET club_id=? WHERE id=?", (club_id, pid))
            end = date(today.year + 2, 8, 31)
            cur.execute("""
                INSERT INTO players_contract (player_id, club_id, contract_type, contract_start, contract_end, wage, is_terminated)
                VALUES (?, ?, 'Professional', ?, ?, ?, 0)
            """, (pid, club_id, today.isoformat(), end.isoformat(), wage))
            cur.execute("""
                INSERT OR IGNORE INTO transfers_log (ts, type, from_club_id, to_club_id, player_id, fee, wage, contract_end)
                VALUES (?, 'free', NULL, ?, ?, 0, ?, ?)
            """, (today.isoformat(), club_id, pid, wage, end.isoformat()))
            mark_transfer(pid)
        
            moved_today.add(pid)
            conn.commit()
            print(f"[{club_name}] Signed FREE {fn} {ln} ({position}) wage={wage}")
            return balance  # signed one this call
        
        # no suitable improvement
        return balance

    
    
    def buy_player(club_id: int, club_name: str, pos: str, balance: int, today: date) -> int:
        if not is_window(today) or balance <= 400_000:
            return balance
    
    
        # 🚫 Buyer already full?
        buyer_total = cur.execute(
            "SELECT COUNT(*) FROM players WHERE club_id=? AND is_retired=0", (club_id,)
        ).fetchone()[0]
        if buyer_total >= MAX_SQUAD:
            return balance    
    
        # Buyer's fame (used in SQL filter and final guard)
        buyer_fame = cur.execute("SELECT fame FROM clubs WHERE id=?", (club_id,)).fetchone()[0]
        
        pool = cur.execute("""
            SELECT p.id, p.club_id, p.first_name, p.last_name, p.position, p.value,
                   pa.at_curr_ability, c.fame AS seller_fame
            FROM players p
            JOIN players_attr pa ON pa.player_id = p.id
            JOIN clubs c ON c.id = p.club_id
            WHERE p.is_retired = 0
              AND p.club_id IS NOT NULL
              AND p.club_id != ?
              AND p.position = ?
              AND COALESCE(
                    (SELECT MAX(tl.ts) FROM transfers_log tl WHERE tl.player_id = p.id),
                    '1900-01-01'
                  ) < ?
              AND (c.fame + ?) <= ?
            ORDER BY pa.at_curr_ability DESC
            LIMIT 24
        """, (club_id, pos, cutoff_date, FAME_TOLERANCE, buyer_fame)).fetchall()


    
        random.shuffle(pool)
        for pid, seller_id, fn, ln, position, value, ca, seller_fame in pool:
            # Final fame guard (belt & suspenders)
            if seller_fame + FAME_TOLERANCE > buyer_fame:
                continue
    
            # Re-check current owner + cooldown (no lock columns)
            row = cur.execute("SELECT club_id FROM players WHERE id=?", (pid,)).fetchone()
            if not row:
                continue
            current_owner = row[0]
            if current_owner != seller_id:
                continue
            
            # Cooldown guard: skip if moved within last COOLDOWN_DAYS
            if cur.execute(
                "SELECT 1 FROM transfers_log WHERE player_id=? AND ts >= ? LIMIT 1",
                (pid, cutoff_date)
            ).fetchone():
                continue
            
            # Same-day guard (belt & suspenders)
            if cur.execute(
                "SELECT 1 FROM transfers_log WHERE player_id=? AND ts = ? LIMIT 1",
                (pid, GAME_DATE.isoformat())
            ).fetchone():
                continue

            live_balance = club_balance(club_id)
    
            # pricing + affordability
            y_left = years_left(pid, today)
            fee = ask_fee(value, y_left)
            wage = max(150_000, int(value * 0.05))
            
            if fee > live_balance:
                continue
            
            # Seller must have enough overall squad size after the sale
            seller_total = cur.execute(
                "SELECT COUNT(*) FROM players WHERE club_id=? AND is_retired=0", (seller_id,)
            ).fetchone()[0]
            if seller_total - 1 < MIN_SQUAD:
                continue  # 🚫 would drop seller below min 18            
                
            # Buyer must not exceed max after purchase
            if buyer_total + 1 > MAX_SQUAD:
                continue  # 🚫 would exceed 23                
    
    
            # ... after affordability & squad-size checks ...
            
            # Require clear improvement unless the buyer is short in this position
            pos_count = cur.execute(
                "SELECT COUNT(*) FROM players WHERE club_id=? AND position=? AND is_retired=0",
                (club_id, position)
            ).fetchone()[0]
            need_pos = pos_count < REQUIRED.get(position, 0)
            
            if not need_pos and not improves_team(club_id, position, pid):
                continue

    
    
    
            # accept?
            if random.random() < 0.60:

                if not safe_assign_to_club(pid, club_id):
                    continue

                # seller receives fee (relative add)
                cur.execute("UPDATE clubs SET current_balance_EUR = current_balance_EUR + ? WHERE id=?", (fee, seller_id))
                # buyer pays fee (relative subtract)
                cur.execute("UPDATE clubs SET current_balance_EUR = current_balance_EUR - ? WHERE id=?", (fee, club_id))
                # (optional) refresh local for subsequent checks this day
                live_balance = club_balance(club_id)

                
                    
                cur.execute("""
                    UPDATE players_contract
                       SET is_terminated=1
                     WHERE player_id=? AND is_terminated=0
                       AND (contract_end IS NULL OR contract_end >= ?)
                """, (pid, today.isoformat()))
    
                cur.execute("UPDATE players SET club_id=? WHERE id=?", (club_id, pid))
                end = date(today.year + random.randint(2,4), 8, 31)
                cur.execute("""
                    INSERT INTO players_contract (player_id, club_id, contract_type, contract_start, contract_end, wage, is_terminated)
                    VALUES (?, ?, 'Transfer', ?, ?, ?, 0)
                """, (pid, club_id, today.isoformat(), end.isoformat(), wage))
    
                cur.execute("""
                    INSERT OR IGNORE INTO transfers_log (ts, type, from_club_id, to_club_id, player_id, fee, wage, contract_end)
                    VALUES (?, 'transfer', ?, ?, ?, ?, ?, ?)
                """, (today.isoformat(), seller_id, club_id, pid, fee, wage, end.isoformat()))
                mark_transfer(pid)
    
                conn.commit()
                print(f"[{club_name}] Bought {fn} {ln} ({position}) fee={fee} wage={wage}")
                break
    
        return balance


    # -----------------------
    # Execution
    # -----------------------
    # Preload free staff once per day
    free_staff = cur.execute("""
        SELECT s.id, s.first_name, s.last_name, s.role, s.fame
        FROM staff s
        WHERE s.club_id IS NULL AND s.is_retired = 0
    """).fetchall()

    clubs = cur.execute("SELECT id, name, fame, current_balance_EUR FROM clubs").fetchall()
    moved_players_today = set()

    for club_id, club_name, club_fame, balance in clubs:
        # count squad
        counts = dict(cur.execute("""
            SELECT position, COUNT(*) FROM players
            WHERE club_id=? AND is_retired=0 GROUP BY position
        """, (club_id,)).fetchall())

        # 1) Fill gaps with free agents
        needed = [pos for pos, need in REQUIRED.items() if counts.get(pos, 0) < need]
        if needed:
            pos_try = random.choice(needed)
            balance = sign_free_agent(club_id, club_name, pos_try, balance, GAME_DATE, moved_players_today)

        # 2) If still needs and in window, try a paid buy
        counts = dict(cur.execute("""
            SELECT position, COUNT(*) FROM players
            WHERE club_id=? AND is_retired=0 GROUP BY position
        """, (club_id,)).fetchall())
        needed = [pos for pos, need in REQUIRED.items() if counts.get(pos, 0) < need]
        if needed:
            pos_try = random.choice(needed)
            balance = buy_player(club_id, club_name, pos_try, balance, GAME_DATE)

        # 3) STAFF SIGNING (same as before)
        staff_counts = dict(cur.execute("""
            SELECT role, COUNT(*) FROM staff
            WHERE club_id=? AND is_retired=0 GROUP BY role
        """, (club_id,)).fetchall())

        staff_needs = []
        for role in ["Manager", "Physio", "Medical", "Scout", "Goalkeeping Coach"]:
            if staff_counts.get(role, 0) < 1:
                staff_needs.append(role)

        if staff_needs and balance > 100_000:
            need_role = random.choice(staff_needs)
            candidates = [fs for fs in free_staff if fs[3] == need_role]

            if candidates:
                sid, fn, ln, role, fame = random.choice(candidates)
                wage = random.randint(50_000, 150_000)

                already_signed = cur.execute("""
                    SELECT 1 FROM staff_contract
                    WHERE staff_id=? AND contract_end >= ? AND is_terminated=0
                """, (sid, GAME_DATE.isoformat())).fetchone()

                already_worked_here = cur.execute("""
                    SELECT 1 FROM staff_contract
                    WHERE staff_id=? AND club_id=?
                """, (sid, club_id)).fetchone()

                if not already_signed and not already_worked_here and wage < balance * 0.30:
                    contract_start = GAME_DATE
                    contract_end = date(GAME_DATE.year + 2, 8, 31)

                    cur.execute("UPDATE staff SET club_id=? WHERE id=?", (club_id, sid))
                    cur.execute("""
                        INSERT INTO staff_contract (staff_id, club_id, contract_type, contract_start, contract_end, wage, is_terminated)
                        VALUES (?, ?, 'Professional', ?, ?, ?, 0)
                    """, (sid, club_id, contract_start.isoformat(), contract_end.isoformat(), wage))

                    #cur.execute("UPDATE clubs SET current_balance_EUR = current_balance_EUR - ? WHERE id=?", (wage, club_id))
                    balance = club_balance(club_id)  # refresh local

                    # manager confidence reset
                    if role == "Manager":
                        row = cur.execute("SELECT manager_satisf FROM clubs_board WHERE club_id=?", (club_id,)).fetchone()
                        prev_conf = row[0] if row else 1000
                        new_confidence = max(1500, prev_conf)
                        cur.execute("""
                            UPDATE clubs_board
                            SET manager_satisf = ?,
                                last_manager_change = ?
                            WHERE club_id=?
                        """, (new_confidence, GAME_DATE.isoformat(), club_id))

                    free_staff = [fs for fs in free_staff if fs[0] != sid]
                    conn.commit()
                    print(f"[{club_name}] Hired staff {fn} {ln} ({role}) wage={wage}")

    # no bulk commit needed; we committed after each successful op
    conn.close()




def get_expected_table_position(cur, club_id, league_id):
    cur.execute("SELECT id, fame FROM clubs WHERE league_id=? ORDER BY fame DESC", (league_id,))
    clubs = cur.fetchall()
    for idx, (cid, fame) in enumerate(clubs):
        if cid == club_id:
            return idx + 1, len(clubs)
    return None, len(clubs)

def get_actual_table_position(cur, club_id, league_id):
    cur.execute("""
        SELECT c.id,
               COALESCE(SUM(
                   CASE
                       WHEN f.home_club_id = c.id AND f.home_goals > f.away_goals THEN 3
                       WHEN f.away_club_id = c.id AND f.away_goals > f.home_goals THEN 3
                       WHEN f.home_goals = f.away_goals AND (f.home_club_id = c.id OR f.away_club_id = c.id) THEN 1
                       ELSE 0 END
               ), 0) as points,
               COALESCE(SUM(CASE WHEN f.home_club_id = c.id THEN f.home_goals ELSE f.away_goals END), 0) as goals_for,
               COALESCE(SUM(CASE WHEN f.home_club_id = c.id THEN f.away_goals ELSE f.home_goals END), 0) as goals_against
        FROM clubs c
        LEFT JOIN fixtures f ON (f.home_club_id = c.id OR f.away_club_id = c.id)
        WHERE c.league_id = ?
        GROUP BY c.id
        ORDER BY points DESC, (goals_for - goals_against) DESC
    """, (league_id,))
    table = cur.fetchall()
    for idx, (cid, *_rest) in enumerate(table):
        if cid == club_id:
            return idx + 1  # 1-based
    return None

def board_satisfaction_and_firing(conn, GAME_DATE, min_matches=10, max_matches=15):
    cur = conn.cursor()
    cur.execute("SELECT id, name, league_id FROM clubs WHERE league_id IN (1,2)")
    for club_id, club_name, league_id in cur.fetchall():
        
        cur.execute("SELECT last_manager_change FROM clubs_board WHERE club_id=?", (club_id,))
        row = cur.fetchone()
        if row and row[0]:
            last_change = date.fromisoformat(row[0])
            if (GAME_DATE - last_change).days < 90:  # grace period
                continue  # ✅ Skip firing check
        
        cur.execute("SELECT manager_satisf, at_patience FROM clubs_board WHERE club_id=?", (club_id,))
        row = cur.fetchone()
        if row:
            board_manager, board_patience = row
        else:
            board_manager, board_patience = (1000, 1000)
            
        # ✅ Fire chance depends strongly on confidence
        if board_manager > 1200 and board_patience > 1200:
            fire_chance = 0.05
        elif board_manager > 1000:
            fire_chance = 0.2
        else:
            fire_chance = 0.5

        if board_manager < 800:
            fire_chance += 0.2
        if board_patience < 800:
            fire_chance += 0.2
        
        # Count league matches played this season
        cur.execute("""
            SELECT COUNT(*) FROM fixtures
            WHERE (home_club_id=? OR away_club_id=?) AND competition_id=? AND played=1
        """, (club_id, club_id, league_id))
        matches_played = cur.fetchone()[0]

        if matches_played < min_matches:
            continue
        if matches_played > max_matches:
            continue

        expected_pos, total_clubs = get_expected_table_position(cur, club_id, league_id)
        actual_pos = get_actual_table_position(cur, club_id, league_id)
        if expected_pos is None or actual_pos is None:
            continue

        allowed_pos = expected_pos + 2  # Leeway: 2 places below expected
        
        if actual_pos > allowed_pos:
            if random.random() < fire_chance:
                cur.execute("SELECT id, first_name, last_name FROM staff WHERE club_id=? AND role='Manager'", (club_id,))
                manager = cur.fetchone()
                if manager:
                    manager_id, fn, ln = manager
                    print(f"[{club_name}] 🚨 Board fires {fn} {ln} (expected ≤ {allowed_pos}, actual {actual_pos})")

                    # ✅ Fire manager (set him free)
                    cur.execute("UPDATE staff SET club_id=NULL WHERE id=?", (manager_id,))
                    
                    # ✅ Terminate his active contract
                    cur.execute("""
                        UPDATE staff_contract
                        SET is_terminated=1, contract_end=?
                        WHERE staff_id=? AND club_id=? AND is_terminated=0
                    """, (GAME_DATE, manager_id, club_id))
            else:
                cur.execute("SELECT first_name, last_name FROM staff WHERE club_id=? AND role='Manager'", (club_id,))
                manager = cur.fetchone()
                if manager:
                    fn, ln = manager
                    print(f"[{club_name}] ⚠️ Board considered firing {fn} {ln}, but gave another chance (expected ≤ {allowed_pos}, actual {actual_pos})")
    conn.commit()





def adjust_board_satisfaction(cur, club_id, result):
    # --- Get fame ---
    cur.execute("SELECT fame, league_id FROM clubs WHERE id=?", (club_id,))
    row = cur.fetchone()
    if not row:
        return
    fame, league_id = row
    fame_factor = max(0.7, min(1.3, fame / 1000.0))  # gentle fame scaling

    # --- Expected vs actual position ---
    expected_pos, total_clubs = get_expected_table_position(cur, club_id, league_id)
    actual_pos = get_actual_table_position(cur, club_id, league_id)

    if expected_pos is None or actual_pos is None:
        pos_factor = 1.0  # fallback neutral
    else:
        # If doing better than expected → >1.0, worse → <1.0
        diff = expected_pos - actual_pos  # positive if overperforming
        pos_factor = 1.0 + (diff / total_clubs) * 0.5  # cap influence
        pos_factor = max(0.6, min(1.4, pos_factor))

    # --- Base deltas ---
    if result == "win":
        delta_mgr = random.randint(45, 75)
        delta_squad = random.randint(35, 65)
    elif result == "loss":
        delta_mgr = random.randint(-60, -30)
        delta_squad = random.randint(-45, -20)
    else:  # draw
        delta_mgr = random.randint(-5, 12)
        delta_squad = random.randint(-4, 10)

    # --- Adjust by fame (expectations) ---
    if fame_factor > 1.1:  # big clubs stricter
        delta_mgr = int(delta_mgr * (0.9 if delta_mgr > 0 else 1.2))
        delta_squad = int(delta_squad * (0.95 if delta_squad > 0 else 1.1))
    elif fame_factor < 0.9:  # small clubs more forgiving
        delta_mgr = int(delta_mgr * (1.15 if delta_mgr > 0 else 0.85))
        delta_squad = int(delta_squad * (1.1 if delta_squad > 0 else 0.9))

    # --- Adjust by current league position vs expectations ---
    delta_mgr = int(delta_mgr * pos_factor)
    delta_squad = int(delta_squad * pos_factor)

    # --- Apply to DB ---
    cur.execute("SELECT manager_satisf, squad_satisf FROM clubs_board WHERE club_id=?", (club_id,))
    row = cur.fetchone()
    if not row:
        return
    m, s = row

    new_mgr = max(0, min(2000, m + delta_mgr))
    new_squad = max(0, min(2000, s + delta_squad))

    cur.execute("""
        UPDATE clubs_board
        SET manager_satisf=?, squad_satisf=?
        WHERE club_id=?
    """, (new_mgr, new_squad, club_id))


def season_end_board_adjustments(conn, season):
    cur = conn.cursor()
    cur.execute("SELECT id, name, league_id FROM clubs WHERE league_id IN (1,2)")
    clubs = cur.fetchall()

    for club_id, club_name, league_id in clubs:
        expected_pos, total_clubs = get_expected_table_position(cur, club_id, league_id)
        actual_pos = get_actual_table_position(cur, club_id, league_id)

        if expected_pos is None or actual_pos is None:
            continue

        # Overperformance factor
        diff = expected_pos - actual_pos  # positive if better
        pos_factor = diff / total_clubs

        # Base end-season deltas
        if actual_pos == 1:  # Champions
            delta = +300
        elif actual_pos <= expected_pos:  # Better than expected
            delta = +150
        elif actual_pos > total_clubs * 0.9:  # Relegation zone
            delta = -300
        elif actual_pos > expected_pos + 4:  # Way below expectations
            delta = -150
        else:
            delta = 0

        # Apply scaling
        delta = int(delta * (1.0 + pos_factor))

        cur.execute("SELECT manager_satisf, squad_satisf FROM clubs_board WHERE club_id=?", (club_id,))
        row = cur.fetchone()
        if not row:
            continue
        m, s = row

        new_mgr = max(0, min(2000, m + delta))
        new_squad = max(0, min(2000, s + delta))

        cur.execute("""
            UPDATE clubs_board
            SET manager_satisf=?, squad_satisf=?
            WHERE club_id=?
        """, (new_mgr, new_squad, club_id))

        if delta > 0:
            print(f"[{club_name}] 🏆 Board delighted (Δ {delta}) — end of season review")
        elif delta < 0:
            print(f"[{club_name}] 😡 Board disappointed (Δ {delta}) — end of season review")

    conn.commit()


