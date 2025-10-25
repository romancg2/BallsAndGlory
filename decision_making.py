import os
import random
import sqlite3
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
#from db_population import gen_logs_insert

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "db")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "fm_database.sqlite")

FAME_TOLERANCE = 0  # set >0 if you want to allow small downgrades (e.g., 50)
MIN_SQUAD = 18
MAX_SQUAD = 23

TRANSFER_COOLDOWN_DAYS = 180     # 6 months; set 365 if you prefer 1 year
RETURN_BLOCK_DAYS       = 365    # block going back to last seller for 1 year
MAX_TRANSFERS_PER_PLAYER = 3     # lifetime cap (moves of any type)

GEN_LOG_ACTIVATED = 0





def decision_making_func(GAME_DATE):
    """
    Transfers + Staff:
      - COOLDOWN_DAYS=180 via transfers_log check
      - unique (player_id, ts) index
      - seller re-check right before move
      - commit after each successful transfer
    """

    if GEN_LOG_ACTIVATED:
        from db_population import gen_logs_insert

    COOLDOWN_DAYS = 180
    cutoff_date = (GAME_DATE - timedelta(days=COOLDOWN_DAYS)).isoformat()


    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()


    def formation_to_starters_map(form_str: str) -> dict:
        """
        Map '4-4-2', '4-3-3', '3-5-2', '4-2-3-1', etc. to per-position starters
        using your roles (GK, RB, LB, CB, CM, RM, LM, ST).
        AM/DM/WB are folded into CM/RM/LM heuristically.
        """
        f = (form_str or "").strip()
        if not f:
            f = "4-4-2"
    
        presets = {
            "4-4-2":   {"GK":1,"RB":1,"LB":1,"CB":2,"CM":2,"RM":1,"LM":1,"ST":2},
            "4-3-3":   {"GK":1,"RB":1,"LB":1,"CB":2,"CM":3,"RM":1,"LM":1,"ST":1},
            "3-5-2":   {"GK":1,"RB":0,"LB":0,"CB":3,"CM":3,"RM":1,"LM":1,"ST":2},
            "5-3-2":   {"GK":1,"RB":1,"LB":1,"CB":3,"CM":3,"RM":0,"LM":0,"ST":2},
            "4-2-3-1": {"GK":1,"RB":1,"LB":1,"CB":2,"CM":2,"RM":1,"LM":1,"ST":1},
            "4-1-4-1": {"GK":1,"RB":1,"LB":1,"CB":2,"CM":3,"RM":1,"LM":1,"ST":1},
            "4-5-1":   {"GK":1,"RB":1,"LB":1,"CB":2,"CM":4,"RM":0,"LM":0,"ST":1},
            "3-4-3":   {"GK":1,"RB":0,"LB":0,"CB":3,"CM":2,"RM":1,"LM":1,"ST":1},
        }
    
        if f in presets:
            m = presets[f].copy()
        else:
            # Heuristic fallback for unknown strings (e.g., "4-4-1-1" ≈ 4-4-2)
            if f.startswith("4-"):
                m = presets["4-4-2"].copy()
            elif f.startswith("3-"):
                m = presets["3-5-2"].copy()
            elif f.startswith("5-"):
                m = presets["5-3-2"].copy()
            else:
                m = presets["4-4-2"].copy()
    
        # Ensure all keys exist
        for k in ("GK","RB","LB","CB","CM","RM","LM","ST"):
            m.setdefault(k, 0)
        return m
    
    
    def get_club_starters_map(club_id: int) -> dict:
        """
        Read the manager's staff.preferred_formation for this club and return starters map.
        Falls back to '4-4-2' if no manager or no preference set.
        """
        row = cur.execute("""
            SELECT preferred_formation
            FROM staff
            WHERE club_id=? AND role='Manager' AND is_retired=0
            ORDER BY id DESC
            LIMIT 1
        """, (club_id,)).fetchone()
    
        form = row[0] if row and row[0] else "4-4-2"
        return formation_to_starters_map(form)
    
    
    def starters_for_pos(club_id: int, pos: str, default_starters: int = 1) -> int:
        return get_club_starters_map(club_id).get(pos, default_starters)


    def count_players_in_pos(club_id: int, pos: str) -> int:
        (cnt,) = cur.execute("""
            SELECT COUNT(DISTINCT p.id)
            FROM players p
            LEFT JOIN players_positions pp ON pp.player_id = p.id
            WHERE p.club_id=? AND p.is_retired=0
              AND COALESCE(pp.position, p.position) = ?
        """, (club_id, pos)).fetchone()
        return cnt


    def last_transfer(pid: int):
        """
        Returns (ts_str, from_club_id, to_club_id) of the last move, or (None, None, None).
        """
        row = cur.execute("""
            SELECT ts, from_club_id, to_club_id
            FROM transfers_log
            WHERE player_id=?
            ORDER BY ts DESC
            LIMIT 1
        """, (pid,)).fetchone()
        return row if row else (None, None, None)
    
    def lifetime_transfers(pid: int) -> int:
        (cnt,) = cur.execute("SELECT COUNT(*) FROM transfers_log WHERE player_id=?", (pid,)).fetchone()
        return cnt
    
    def days_since(ts_str: str, today: date) -> int:
        if not ts_str: return 10_000
        y, m, d = map(int, ts_str.split("-"))
        return (today - date(y, m, d)).days
    
    def can_move_player(pid: int, new_club_id: int, today: date) -> (bool, str):
        # 3) lifetime cap
        if lifetime_transfers(pid) >= MAX_TRANSFERS_PER_PLAYER:
            return False, "lifetime-cap"
    
        # last move info
        last_ts, last_from, last_to = last_transfer(pid)
    
        # 2) cooldown between any two moves
        if days_since(last_ts, today) < TRANSFER_COOLDOWN_DAYS:
            return False, "cooldown"
    
        # 1) no return to the previous seller (bounce-back) for RETURN_BLOCK_DAYS
        # If the last move was FROM club X TO club Y, forbid moving back TO X for the block window
        if last_from is not None and new_club_id == last_from and days_since(last_ts, today) < RETURN_BLOCK_DAYS:
            return False, "return-block"
    
        return True, "ok"


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

    REQUIRED = {
        "GK": 2,
        "RB": 2, "LB": 2, "CB": 4,
        "RM": 2, "LM": 2, "CM": 4,
        "ST": 3
    }
    ALL_POSITIONS = tuple(REQUIRED.keys())


    # How many "starters" per position to compare against (depth to beat)
    STARTERS = {"GK": 1, "RB": 1, "LB": 1, "CB": 2, "CM": 2, "RM": 1, "LM": 1, "ST": 2}
    DEFAULT_STARTERS = 1
    
    # Improvement thresholds (on 100–2000 scale)
    IMPROVE_ABS_DELTA = 35         # absolute points better than last starter
    IMPROVE_RATIO     = 1.03       # or ≥3% better    
    
    
    def deficit_positions(club_id: int):
        """
        Returns a list of (pos, lacking) with lacking>0, sorted by how short the club is.
        Example: [('CB', 2), ('GK', 1), ...]
        """
        
        counts = dict(cur.execute("""
            SELECT pp.position, COUNT(*)
            FROM players p
            JOIN players_positions pp ON pp.player_id = p.id
            WHERE p.club_id=? AND p.is_retired=0
            GROUP BY pp.position
        """, (club_id,)).fetchall())
    
        lacks = []
        for pos in ALL_POSITIONS:
            have = counts.get(pos, 0)
            need = REQUIRED.get(pos, 0)
            if have < need:
                lacks.append((pos, need - have))
        # most urgent first
        lacks.sort(key=lambda x: x[1], reverse=True)
        return lacks

    
    
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
        rows = cur.execute("""
            SELECT DISTINCT p.id
            FROM players p
            LEFT JOIN players_positions pp ON pp.player_id = p.id
            WHERE p.club_id=? AND p.is_retired=0
              AND COALESCE(pp.position, p.position) = ?
        """, (club_id, pos)).fetchall()
        scores = [(pid, player_pos_score(pid, pos)) for (pid,) in rows]
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores

    
    def improves_team(club_id: int, pos: str, cand_pid: int) -> bool:
        cand_score  = player_pos_score(cand_pid, pos)
        starters_n  = starters_for_pos(club_id, pos, DEFAULT_STARTERS)
        if starters_n <= 0:
            return False  # formation doesn’t use this role; no “upgrade” needed
    
        depth = club_pos_scores(club_id, pos)
    
        # if we don’t have enough players to fill the starters, any good candidate helps
        if len(depth) < starters_n:
            return True
    
        baseline_idx = starters_n - 1   # safe because len(depth) >= starters_n here
        baseline     = depth[baseline_idx][1]
        return (
            cand_score >= baseline + IMPROVE_ABS_DELTA
            or cand_score >= baseline * IMPROVE_RATIO
        )


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

    def ask_fee(value: int, y_left: float, age: int) -> int:
        """
        Softer valuation:
          - start from 'value'
          - modest contract premium: up to +18% for 3+ years left
          - age discount: -20% if 30+, -10% if 27–29; slight +5% if <=23
          - global cap: 0.6×..1.15× of 'value'
        """
        # modest contract leverage (0..+18%)
        contract_factor = 1.0 + 0.06 * min(3.0, max(0.0, y_left))   # 0..+18%
    
        # age effect
        if age >= 30:
            age_factor = 0.80
        elif age >= 27:
            age_factor = 0.90
        elif age <= 23:
            age_factor = 1.05
        else:
            age_factor = 1.00
    
        raw = value * contract_factor * age_factor
        # bound the final ask around the internal 'value'
        low_cap, hi_cap = 0.60 * value, 1.15 * value
        return int(max(low_cap, min(hi_cap, raw)))


    def can_sell(seller_id: int, pos: str, keep_at_least: int) -> bool:
        cnt = count_players_in_pos(seller_id, pos)
        return cnt > keep_at_least


    
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
    def sign_free_agent(club_id: int, club_name: str, pos: str, balance_unused: int, today: date, moved_today: set) -> bool:
        """
        Try to sign ONE free agent for 'pos'. Returns True if someone was signed.
        Uses live club balance; if squad below MIN_SQUAD, skip the 'improves_team' gate.
        """
        buyer_total = cur.execute(
            "SELECT COUNT(*) FROM players WHERE club_id=? AND is_retired=0", (club_id,)
        ).fetchone()[0]
        if buyer_total >= MAX_SQUAD:
            return False
    
        # pull a decent pool of free agents for this pos
        candidates = cur.execute("""
            SELECT DISTINCT p.id, p.first_name, p.last_name,
                   COALESCE(pp.position, p.position) AS position, p.value
            FROM players p
            LEFT JOIN players_positions pp ON pp.player_id = p.id
            WHERE p.is_retired=0
              AND p.club_id IS NULL
              AND COALESCE(pp.position, p.position) = ?
            ORDER BY (SELECT pa.at_curr_ability FROM players_attr pa WHERE pa.player_id=p.id) DESC
            LIMIT 40
        """, (pos,)).fetchall()
    
        if not candidates:
            return False
    
        # score and iterate best-first
        scored = []
        for pid, fn, ln, position, value in candidates:
            if pid in moved_today:
                continue
            # no cooldown check needed for true free agents
            role_score = player_pos_score(pid, position)
            scored.append((role_score, pid, fn, ln, position, value))
        scored.sort(reverse=True)
    
        need_total = buyer_total < MIN_SQUAD
        for _, pid, fn, ln, position, value in scored:
            # capacity guard
            if cur.execute(
                "SELECT COUNT(*) FROM players WHERE club_id=? AND is_retired=0", (club_id,)
            ).fetchone()[0] >= MAX_SQUAD:
                return False
    
            live_balance = club_balance(club_id)
            wage = max(80_000, int(value * 0.02))  # keep wages modest for FAs
            if wage > live_balance * 0.6:           # don’t blow >60% of cash monthly
                continue
    
            # if we’re short overall OR short in this pos, relax the “improves_team” rule
            pos_have = count_players_in_pos(club_id, position)
            pos_need = starters_for_pos(club_id, position, DEFAULT_STARTERS)
            need_pos = pos_have < pos_need
            if not (need_total or need_pos or improves_team(club_id, position, pid)):
                continue
    
            # assign to club
            if not safe_assign_to_club(pid, club_id):
                continue
    
            # 2-year contract to Aug 31
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
            return True
    
        return False


    def calc_aggression(balance: int, fame: int) -> float:
        """
        0..0.95 scale. More money => more eager to buy.
        Small clubs get a small eagerness bump to help them spend windfalls.
        """
        a = 0.15
        a += min(0.65, balance / 50_000_000)     # +0..0.65 as balance grows to 50M
        a += max(0.0, (800 - fame) / 4000.0)     # +0..0.20 for very small clubs
        return min(0.95, a)
    

    def almost_improves_team(club_id: int, pos: str, cand_pid: int, slack: int) -> bool:
        """
        Near-upgrade allowed when aggressive, again using dynamic starters.
        """
        cand = player_pos_score(cand_pid, pos)
        starters_n = starters_for_pos(club_id, pos, DEFAULT_STARTERS)
        depth = club_pos_scores(club_id, pos)
    
        if len(depth) < starters_n:
            return True
    
        baseline = depth[min(starters_n, len(depth)) - 1][1]
        return cand >= baseline - slack

    
    def buy_player(club_id: int, club_name: str, pos: str, balance: int, today: date) -> int:
        if not is_window(today) or balance <= 400_000:
            if GEN_LOG_ACTIVATED and balance <= 400_000: 
                gen_logs_insert(DB_PATH, GAME_DATE, f'[{club_name}] not buying (buy_player filter)', f'Balance too low [{balance}]  ')
            return balance
    


    

        # 🚫 Buyer already full?
        buyer_total = cur.execute(
            "SELECT COUNT(*) FROM players WHERE club_id=? AND is_retired=0", (club_id,)
        ).fetchone()[0]
        if buyer_total >= MAX_SQUAD:
            if GEN_LOG_ACTIVATED: 
                gen_logs_insert(DB_PATH, GAME_DATE, f'[{club_name}] not buying (buy_player filter)', f'Buyer already full [{buyer_total} > {MAX_SQUAD}]  ')
            return balance    
    
        # Buyer's fame (used in SQL filter and final guard)
        buyer_fame = cur.execute("SELECT fame FROM clubs WHERE id=?", (club_id,)).fetchone()[0]
        
        # before the pool query:
        agg = calc_aggression(balance, buyer_fame)
        # let small/rich clubs buy from somewhat more famous sellers
        # e.g., base +400 fame, plus up to +1200 more with aggression
        fame_gap = int(400 + agg * 1200)
        
        pool = cur.execute("""
            SELECT p.id, p.club_id, p.first_name, p.last_name, pp.position, p.value,
                   pa.at_curr_ability, c.fame AS seller_fame, p.date_of_birth
            FROM players p
            JOIN players_attr pa ON pa.player_id = p.id
            JOIN clubs c ON c.id = p.club_id
            JOIN players_positions pp ON pp.player_id = p.id
            WHERE p.is_retired = 0
              AND p.club_id IS NOT NULL
              AND p.club_id != ?
              AND pp.position = ?
              AND COALESCE(
                    (SELECT MAX(tl.ts) FROM transfers_log tl WHERE tl.player_id = p.id),
                    '1900-01-01'
                  ) < ?
              AND c.fame <= ? + ?
            ORDER BY pa.at_curr_ability DESC
            LIMIT 24
        """, (club_id, pos, cutoff_date, buyer_fame, fame_gap)).fetchall()

        


    
        random.shuffle(pool)
        for pid, seller_id, fn, ln, position, value, ca, seller_fame, dob in pool:
            y, m, d = map(int, dob.split("-"))
            age = (today - date(y, m, d)).days // 365
            
            
            ok, reason = can_move_player(pid, club_id, today)
            if not ok:
                if GEN_LOG_ACTIVATED:
                    gen_logs_insert(DB_PATH, GAME_DATE, f"[{club_name}] transfer blocked", f"pid={pid} reason={reason}")
                continue
            
            # Final fame guard (belt & suspenders)
            if seller_fame > buyer_fame + fame_gap:
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
            fee = ask_fee(value, y_left, age)
            
            # make wages less punchy (3% of value, with sensible floor)
            wage = max(120_000, int(value * 0.03))
            
            # dynamic cap: spend less if not a clear need/upgrade
            pos_count = count_players_in_pos(club_id, position)
            need_pos = pos_count < REQUIRED.get(position, 0)
            is_rich = balance >= 80_000_000
            
            # how much of the balance we allow for a single fee
            if need_pos:
                cap_frac = 0.28 if not is_rich else 0.40
            else:
                cap_frac = 0.12 if not is_rich else 0.20
            
            buyer_cap = int(cap_frac * balance)
            fee = min(fee, buyer_cap)            
            
            
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
            pos_count = count_players_in_pos(club_id, position)
            need_pos = pos_count < REQUIRED.get(position, 0)
                
            # 💰 Rich club logic
            is_rich = balance >= 100_000_000  # rich threshold
            
            # Require clear improvement unless the buyer is short or rich
            pos_count = count_players_in_pos(club_id, position)
            need_pos = pos_count < REQUIRED.get(position, 0)
            
            # Rich clubs act more aggressively in the market
            if is_rich:
                # 70% chance to attempt signing even without real need
                if random.random() < 0.7:
                    print(f"[{club_name}] 💸 Big-money club acting aggressively on {position}")
                else:
                    # fallback to normal rule
                    if not (need_pos or improves_team(club_id, position, pid)):
                        continue
            else:
                # Normal clubs: only buy if needed or clear upgrade
                if not (need_pos or improves_team(club_id, position, pid)):
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

        # A) Hard safety: if below MIN_SQUAD, fill multiple times this tick
        total_now = cur.execute(
            "SELECT COUNT(*) FROM players WHERE club_id=? AND is_retired=0", (club_id,)
        ).fetchone()[0]
        
        attempts = 0
        while total_now < MIN_SQUAD and attempts < 6:  # cap per tick to avoid runaway
            lacks = deficit_positions(club_id)
            signed_any = False
            
            # try most deficient roles first
            for pos, _missing in lacks[:3]:  # look at up to 3 most-needed roles
                #print(f"[{club_name}] lacks players, position: {pos}")
                if sign_free_agent(club_id, club_name, pos, 0, GAME_DATE, moved_players_today):
                    signed_any = True
                    break  # re-check totals and recompute deficits
        
            if not signed_any:
                # fallback: try any position if we’re desperate
                for pos in ALL_POSITIONS:
                    if sign_free_agent(club_id, club_name, pos, 0, GAME_DATE, moved_players_today):
                        signed_any = True
                        break
        
            if not signed_any:
                break  # nothing to sign this tick
        
            total_now = cur.execute(
                "SELECT COUNT(*) FROM players WHERE club_id=? AND is_retired=0", (club_id,)
            ).fetchone()[0]
            attempts += 1
        
        # B) Normal one-off “need” logic (your existing flow) …
        counts = dict(cur.execute("""
            SELECT position, COUNT(*) FROM players
            WHERE club_id=? AND is_retired=0 GROUP BY position
        """, (club_id,)).fetchall())
        needed = [pos for pos, need in REQUIRED.items() if counts.get(pos, 0) < need]
        if needed:
            pos_try = random.choice(needed)
            _ = sign_free_agent(club_id, club_name, pos_try, 0, GAME_DATE, moved_players_today)
        
        # …then your existing paid buy and staff logic


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


