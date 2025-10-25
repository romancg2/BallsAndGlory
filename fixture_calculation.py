
import random
import math
from decision_making import adjust_board_satisfaction,season_end_board_adjustments


GOAL_SCALING = 1.0 

# League baselines (lazy)
LEAGUE_ATK_MEAN = None
LEAGUE_DEF_MEAN = None

LEAGUE_DEBUGGING = False
CUP_DEBUGGING = False

HOME_ADV = 1.08



def pick_scorers(cur, club_id, goals, fixture_id, team_name, eligible_ids=None):
    """
    Picks scorers from the players who actually played minutes.
    Guarantees scorers exist for both home and away.
    """
    if goals == 0:
        return [], []

    cur.execute("""
        SELECT
          p.id,
          p.first_name,
          p.last_name,
          COALESCE(
            (SELECT pp.position
             FROM players_positions pp
             WHERE pp.player_id = p.id
             ORDER BY pp.position
             LIMIT 1),
            p.position,
            'CM'
          ) AS position,
          pa.at_curr_ability, pa.at_scoring, pa.at_speed
        FROM players p
        JOIN players_attr pa ON pa.player_id = p.id
        WHERE p.club_id = ? AND p.is_retired = 0



    """, (club_id,))
    players = cur.fetchall()

    # ✅ Only allow players who actually played minutes
    if eligible_ids is not None:
        players = [row for row in players if row[0] in eligible_ids]

    if not players:
        return [], []

    # Fame
    cur.execute("SELECT fame FROM clubs WHERE id=?", (club_id,))
    fame_row = cur.fetchone()
    fame = fame_row[0] if fame_row else 1000
    fame_mult = 0.9 + (fame / 2000.0) * 0.2

    weighted_pool = []
    for pid, fn, ln, pos, ability, scoring, speed in players:
        attr_score = scoring * 1.5 + speed * 0.5 + ability * 0.3
        if pos in ("ST", "CF", "FW"):
            attr_score *= 2.5
        elif pos in ("LW", "RW", "AM", "CAM"):
            attr_score *= 1.5
        elif pos in ("CM", "RM", "LM"):
            attr_score *= 0.8
        elif pos in ("CB", "LB", "RB"):
            attr_score *= 0.3
        else:
            attr_score *= 0.1
        weight = attr_score * fame_mult
        weighted_pool.append((pid, fn, ln, weight))

    scorers, names = [], []
    for _ in range(goals):
        scorer = random.choices(
            weighted_pool, weights=[w for _, _, _, w in weighted_pool], k=1
        )[0]
        pid, fn, ln, _ = scorer
        scorers.append((pid, fixture_id))
        names.append(f"{fn} {ln} ({team_name})")

    return scorers, names


def update_fame_after_match(cur, club_id, result, fame_delta=10):
    # Get club fame for scaling
    cur.execute("SELECT fame FROM clubs WHERE id=?", (club_id,))
    row = cur.fetchone()
    club_fame = row[0] if row else 1000
    # Scale: 1.0 at 1000 fame, 2.0 at 2000 fame
    fame_scale = 1.0 + ((club_fame - 1000) / 1000.0)
    fame_scale = max(1.0, min(fame_scale, 2.0))

    if result == "win":
        player_delta = fame_delta
        staff_delta = fame_delta
        manager_delta = fame_delta * 2
    elif result == "loss":
        player_delta = -fame_delta
        staff_delta = -fame_delta
        manager_delta = int(-fame_delta * 2 * fame_scale)
    else:  # draw
        player_delta = 0
        staff_delta = 0
        manager_delta = 0

    # Update players
    cur.execute("SELECT id, fame FROM players WHERE club_id=? AND is_retired=0", (club_id,))
    for pid, fame in cur.fetchall():
        new_fame = max(1, min(fame + player_delta, 2000))
        cur.execute("UPDATE players SET fame=? WHERE id=?", (new_fame, pid))

    # Update staff (except manager)
    cur.execute("SELECT id, fame FROM staff WHERE club_id=? AND role != 'Manager'", (club_id,))
    for sid, fame in cur.fetchall():
        new_fame = max(1, min(fame + staff_delta, 2000))
        cur.execute("UPDATE staff SET fame=? WHERE id=?", (new_fame, sid))

    # Update manager
    cur.execute("SELECT id, fame FROM staff WHERE club_id=? AND role = 'Manager'", (club_id,))
    for sid, fame in cur.fetchall():
        new_fame = max(1, min(fame + manager_delta, 2000))
        cur.execute("UPDATE staff SET fame=? WHERE id=?", (new_fame, sid))

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

def get_club_fame(cur, club_id):
    cur.execute("SELECT fame FROM clubs WHERE id = ?", (club_id,))
    row = cur.fetchone()
    return row[0] if row else 1000


def formation_modifiers(cur, club_id):
    """
    Returns (attack_mult, defense_mult) based on manager's preferred formation.
    Example: 3-5-2 → (1.08, 0.90)
    """
    cur.execute("""
        SELECT preferred_formation
        FROM staff
        WHERE club_id = ? AND role = 'Manager' AND is_retired = 0
        ORDER BY fame DESC LIMIT 1
    """, (club_id,))
    row = cur.fetchone()
    if not row or not row[0]:
        return 1.0, 1.0  # neutral

    formation = row[0].replace(" ", "")
    parts = formation.split("-")

    # --- Defense bias (first number)
    try:
        defenders = int(parts[0])
    except:
        defenders = 4

    # --- Attack bias (last number)
    try:
        strikers = int(parts[-1])
    except:
        strikers = 2

    atk_mult = 1.0
    def_mult = 1.0

    # Defense side
    if defenders >= 5:
        def_mult *= 1.10
        atk_mult *= 0.95
    elif defenders == 3:
        def_mult *= 0.90
        atk_mult *= 1.05

    # Attack side
    if strikers >= 3:
        atk_mult *= 1.08
        def_mult *= 0.95
    elif strikers == 1:
        atk_mult *= 0.92
        def_mult *= 1.05

    # Mild normalization to avoid extremes
    return clamp(atk_mult, 0.85, 1.15), clamp(def_mult, 0.85, 1.15)


def team_strengths(cur, club_id):
    """
    Calculate attack & defense strength from players.
    Attack = heavily scoring + speed, with some passing/dribbling.
    Defense = defending + goalkeeping + discipline.
    Fame is applied later in the match simulation.
    """
    cur.execute("""
        SELECT
          pa.at_scoring, pa.at_speed, pa.at_passing, pa.at_dribbling,
          pa.at_defending, pa.at_goalkeeping, pa.at_selfcont,
          COALESCE(
            (SELECT pp.position
             FROM players_positions pp
             WHERE pp.player_id = p.id
             ORDER BY pp.position
             LIMIT 1),
            p.position,
            'CM'
          ) AS position
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

def compute_comp_strength_baselines(conn, competition_id):
    cur = conn.cursor()
    cur.execute("""
        SELECT c.id
        FROM clubs c
        JOIN clubs_competition cc ON cc.club_id = c.id
        WHERE cc.competition_id = ? AND cc.is_active = 1
    """, (competition_id,))
    club_ids = [r[0] for r in cur.fetchall()]
    if not club_ids:
        return 1500.0, 1500.0
    atk_vals, def_vals = [], []
    for cid in club_ids:
        a, d = team_strengths(cur, cid)
        atk_vals.append(a); def_vals.append(d)
    return sum(atk_vals)/len(atk_vals), sum(def_vals)/len(def_vals)

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

def clamp(v, lo, hi):
    return max(lo, min(hi, v))



def assign_realistic_minutes(home_players, away_players):
    """
    Assign realistic minutes for one match, synchronized for both teams:
      - Both teams share same full match duration (90–94)
      - 11 starters per team, 1 GK always full-time
      - Up to 3 substitutions (no GK subs)
      - Sub pairs share same match length (starter + sub = full match)
      - Bench not used = 0 minutes
      - Total minutes per team = 11 × full_match
    """
    if not home_players or not away_players:
        return {}, {}

    # --- Common full match duration ---
    full_match = random.randint(90, 94)

    def assign_team_minutes(players):
        if len(players) < 11:
            return {pid: full_match for pid, *_ in players}

        starters = players[:11]
        bench = players[11:]

        # Identify GK
        gk_starters = [pid for pid, pos, *_ in starters if pos == "GK"]
        gk_id = gk_starters[0] if gk_starters else None

        minutes = {pid: full_match for pid, *_ in starters}

        # --- Select up to 3 outfield subs ---
        valid_bench = [p for p in bench if p[1] != "GK"]
        subs = random.sample(valid_bench, k=min(3, len(valid_bench)))
        sub_candidates = [p for p in starters if p[1] != "GK"]
        replaced = random.sample(sub_candidates, k=len(subs))

        # --- Apply substitutions ---
        for (sub_pid, *_), (out_pid, *__) in zip(subs, replaced):
            sub_minutes = random.randint(10, 25)
            minutes[out_pid] = full_match - sub_minutes
            minutes[sub_pid] = sub_minutes

        # --- Non-used bench = 0 ---
        for pid, *_ in bench:
            if pid not in minutes:
                minutes[pid] = 0

        # --- GK plays full match ---
        if gk_id is not None:
            minutes[gk_id] = full_match

        return minutes

    home_minutes = assign_team_minutes(home_players)
    away_minutes = assign_team_minutes(away_players)

    # ✅ Ensure both have same total (optional small correction)
    expected_total = 11 * full_match
    for minutes_dict in (home_minutes, away_minutes):
        total = sum(minutes_dict.values())
        if abs(total - expected_total) > 5:
            correction = (expected_total - total) // 11
            for pid in minutes_dict:
                minutes_dict[pid] = max(0, minutes_dict[pid] + correction)

    return home_minutes, away_minutes





def get_realistic_squad(cur, club_id):
    """
    Returns up to 22 players (11 starters + bench) based on the manager's preferred formation.
    Example formations supported: "4-3-3", "4-2-3-1", "3-5-2", "5-3-2", etc.
    """
    # 1️⃣ Get manager's preferred formation
    cur.execute("""
        SELECT preferred_formation FROM staff
        WHERE club_id = ? AND role = 'Manager' AND is_retired = 0
        ORDER BY fame DESC LIMIT 1
    """, (club_id,))
    row = cur.fetchone()
    formation = row[0] if row and row[0] else "4-3-3"

    # 2️⃣ Parse formation into defender–midfielder–forward numbers
    try:
        parts = [int(x) for x in formation.replace(" ", "").split("-")]
        if len(parts) == 2:  # e.g. "4-4"
            defs, mids, fwds = parts[0], parts[1], 2
        elif len(parts) == 3:  # e.g. "4-3-3"
            defs, mids, fwds = parts
        elif len(parts) == 4:  # e.g. "4-2-3-1"
            defs, mids, fwds = parts[0], parts[1] + parts[2], parts[3]
        else:
            defs, mids, fwds = 4, 3, 3
    except Exception:
        defs, mids, fwds = 4, 3, 3  # fallback

    total = defs + mids + fwds
    if total != 10:  # GK + 10 outfielders
        # Normalize proportions to 10
        scale = 10 / total
        defs = round(defs * scale)
        mids = round(mids * scale)
        fwds = 10 - defs - mids

    # 3️⃣ Fetch players
    cur.execute("""
        SELECT
          p.id,
          COALESCE(
            (SELECT pp.position
             FROM players_positions pp
             WHERE pp.player_id = p.id
             ORDER BY pp.position
             LIMIT 1),
            p.position,
            'CM'
          ) AS position,
          pa.at_defending, pa.at_passing, pa.at_scoring,
          pa.at_goalkeeping, pa.at_speed, pa.at_curr_ability
        FROM players p
        JOIN players_attr pa ON pa.player_id = p.id
        WHERE p.club_id = ? AND p.is_retired = 0
        ORDER BY pa.at_curr_ability DESC



    """, (club_id,))
    all_players = cur.fetchall()
    if not all_players:
        return []

    # Split by position group
    gks  = [p for p in all_players if p[1] == "GK"]
    defs_pool = [p for p in all_players if p[1] in ("CB", "LB", "RB", "CDM")]
    mids_pool = [p for p in all_players if p[1] in ("CM", "RM", "LM", "CAM", "AM")]
    fwds_pool = [p for p in all_players if p[1] in ("ST", "CF", "LW", "RW")]

    # 4️⃣ Build starting XI
    starters = []
    if gks:
        starters.append(gks[0])  # 1 GK
    starters += defs_pool[:defs]
    starters += mids_pool[:mids]
    starters += fwds_pool[:fwds]

    # Fill if short
    if len(starters) < 11:
        extra = [p for p in all_players if p not in starters]
        starters += extra[: 11 - len(starters)]

    # 5️⃣ Build bench (1 GK + 9 random mix)
    remaining = [p for p in all_players if p not in starters]
    bench = []
    if len(gks) > 1:
        bench.append(gks[1])
    # add mix of roles
    pool_for_bench = [p for p in remaining if p not in bench]
    bench += random.sample(pool_for_bench, k=min(9, len(pool_for_bench)))

    return starters + bench


def draw_goals(lmbda):
    """
    Stable Poisson draw with soft bias to prevent 6–0 or 7–1 blowouts.
    """
    lmbda = max(0.05, min(lmbda, 1.8))
    probs = [math.exp(-lmbda) * (lmbda ** k) / math.factorial(k) for k in range(8)]
    probs[-1] = max(0.0, 1.0 - sum(probs[:-1]))
    goals = random.choices(range(8), weights=probs, k=1)[0]

    # Softly cap extremes
    if goals > 4:
        if random.random() < 0.6:
            goals = 4
        elif goals > 5:
            goals = 5
    return goals



# --- Build playing ranges ---
def get_playing_ranges(minutes_dict):
    # starters: 0 to minutes_played
    # subs: (full_match - sub_minutes) to full_match
    # (We assume full_match ≈ max(minutes_dict.values()))
    full_match = max(minutes_dict.values(), default=90)
    playing_ranges = {}
    for pid, mins in minutes_dict.items():
        if mins == 0:
            continue
        if mins >= full_match - 10:  # starter (played most of match)
            start, end = 0, mins
        else:  # substitute
            start, end = full_match - mins, full_match
        playing_ranges[pid] = (start, end)
    return playing_ranges



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
    
    
    
    def expected_goals_local(attack, opp_defense, fame_mult, form_mult, home_side, atk_mean, def_mean):
        base = 1.05 if home_side else 0.95
        atk_n = attack / max(1.0, atk_mean)
        def_n = opp_defense / max(1.0, def_mean)
        ratio = (atk_n ** 1.0) / (def_n ** 1.0)
        fame_adj = 1.0 + (fame_mult - 1.0) * 0.3
        form_adj = 1.0 + (form_mult - 1.0) * 0.25
        lam = base * ratio * fame_adj * form_adj
        if home_side:
            lam *= 1.05
        lam *= random.uniform(0.97, 1.03)
        return clamp(lam, 0.3, 1.8)
    
        


    def poisson_draw(lmbda, kmax=6):
        lmbda = max(0.05, lmbda)
        probs = [math.exp(-lmbda) * (lmbda ** k) / math.factorial(k) for k in range(kmax)]
        probs[-1] = max(0.0, 1.0 - sum(probs[:-1]))
        return random.choices(range(kmax), weights=probs, k=1)[0]
    
    
    def negbinom_poisson(lmbda, kmax=8, phi=0.25):
        """
        Draw goals from a Negative Binomial by mixing a Gamma over the Poisson rate:
        - lmbda: mean goals
        - phi: overdispersion (0 -> Poisson). 0.2–0.4 is a nice realistic band.
        """
        lmbda = max(0.03, lmbda)
        # Gamma(shape, scale) with mean=λ, var=λ + φλ^2
        shape = 1.0 / max(1e-6, phi)
        scale = lmbda / shape
        lam_ = random.gammavariate(shape, scale)
        # Poisson draw with soft cap probability mass
        probs = [math.exp(-lam_) * (lam_ ** k) / math.factorial(k) for k in range(kmax)]
        probs[-1] = max(0.0, 1.0 - sum(probs[:-1]))
        return random.choices(range(kmax), weights=probs, k=1)[0]


    # Group fixtures by competition to compute baselines once per comp per day
    by_comp = {}
    for row in fixtures:
        _fid, _h, _hn, _a, _an, comp_id, *_ = row
        by_comp.setdefault(comp_id, []).append(row)

    baselines = {}
    for comp_id in by_comp:
        baselines[comp_id] = compute_comp_strength_baselines(conn, comp_id)


    for fixture_id, home_id, home_name, away_id, away_name, league_id, league_name, is_cup, competition_round in fixtures:
        
        scorers, names = [], []  # ✅ initialize before any conditional use
        
        home_attack, home_defense = team_strengths(cur, home_id)
        away_attack, away_defense = team_strengths(cur, away_id)
        
        # ⚙️ Formation tactical effect
        home_atk_mult, home_def_mult = formation_modifiers(cur, home_id)
        away_atk_mult, away_def_mult = formation_modifiers(cur, away_id)
        
        home_attack *= home_atk_mult
        home_defense *= home_def_mult
        away_attack *= away_atk_mult
        away_defense *= away_def_mult

        home_fm = fame_effect(get_club_fame(cur, home_id))
        away_fm = fame_effect(get_club_fame(cur, away_id))
        home_form = get_team_form(cur, home_id)
        away_form = get_team_form(cur, away_id)


        # after computing baselines = {comp_id: (atk_mean, def_mean)}
        atk_mean, def_mean = baselines.get(league_id, (LEAGUE_ATK_MEAN or 1500.0, LEAGUE_DEF_MEAN or 1500.0))
        
        home_lambda = expected_goals_local(home_attack, away_defense, home_fm, home_form, True,  atk_mean, def_mean)
        away_lambda = expected_goals_local(away_attack, home_defense, away_fm, away_form, False, atk_mean, def_mean)


        home_goals = draw_goals(home_lambda)
        away_goals = draw_goals(away_lambda)
        
        
        # --- Players involved ---
        cur.execute("SELECT id FROM players WHERE club_id = ? AND is_retired = 0", (home_id,))
        home_players = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT id FROM players WHERE club_id = ? AND is_retired = 0", (away_id,))
        away_players = [r[0] for r in cur.fetchall()]
        
        
        # Effect on fame for players and staff after win/lose
        if home_goals > away_goals:
            update_fame_after_match(cur, home_id, "win")
            update_fame_after_match(cur, away_id, "loss")
            adjust_board_satisfaction(cur, home_id, "win")
            adjust_board_satisfaction(cur, away_id, "loss")
        elif home_goals < away_goals:
            update_fame_after_match(cur, home_id, "loss")
            update_fame_after_match(cur, away_id, "win")
            adjust_board_satisfaction(cur, home_id, "loss")
            adjust_board_satisfaction(cur, away_id, "win")
        else:
            update_fame_after_match(cur, home_id, "draw")
            update_fame_after_match(cur, away_id, "draw")
            adjust_board_satisfaction(cur, home_id, "draw")
            adjust_board_satisfaction(cur, away_id, "draw")






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

            if LEAGUE_DEBUGGING:
                print(f"⚽ [{league_name}] {home_name} {home_goals} - {away_goals} {away_name}")

            
            
 
            
        # --- PLAYER MATCH STATS (realistic minutes & participation) ---
        player_stats = []

        def get_team_players(club_id):
            cur.execute("""
                SELECT
                  p.id,
                  COALESCE(
                    (SELECT pp.position
                     FROM players_positions pp
                     WHERE pp.player_id = p.id
                     ORDER BY pp.position
                     LIMIT 1),
                    p.position,
                    'CM'
                  ) AS position,
                  pa.at_defending, pa.at_passing, pa.at_scoring,
                  pa.at_goalkeeping, pa.at_speed, pa.at_curr_ability
                FROM players p
                JOIN players_attr pa ON pa.player_id = p.id
                WHERE p.club_id = ? AND p.is_retired = 0
                ORDER BY pa.at_curr_ability DESC
                LIMIT 22



            """, (club_id,))
            return cur.fetchall()

        home_players = get_team_players(home_id)
        away_players = get_team_players(away_id)

        # Top 11 start, next 4 are potential subs
        def assign_minutes(players):
            starters = players[:11]
            subs = random.sample(players[11:], k=min(len(players[11:]), 3))
            active = starters + subs

            # minute distribution
            minutes = {}
            for pid, pos, *_ in players:
                if (pid, pos, *_ ) in starters:
                    minutes[pid] = random.randint(80, 95)
                elif (pid, pos, *_ ) in subs:
                    minutes[pid] = random.randint(10, 30)
                else:
                    minutes[pid] = 0
            return minutes

        home_players = get_realistic_squad(cur, home_id)
        away_players = get_realistic_squad(cur, away_id)
        
        # Get the club_id for each player
        home_pids = [pid for pid, *_ in home_players]
        away_pids = [pid for pid, *_ in away_players]        
        
        
        pid_to_club = {pid: home_id for pid in home_pids}
        pid_to_club.update({pid: away_id for pid in away_pids})
        
        home_minutes, away_minutes = assign_realistic_minutes(home_players, away_players)


        # ✅ Now that we know who played, pick scorers from actual participants
        home_eligible = [pid for pid, *_ in home_players if home_minutes.get(pid, 0) > 0]
        away_eligible = [pid for pid, *_ in away_players if away_minutes.get(pid, 0) > 0]

        hs, hn = pick_scorers(cur, home_id, home_goals, fixture_id, home_name, home_eligible)
        as_, an = pick_scorers(cur, away_id, away_goals, fixture_id, away_name, away_eligible)

        scorers = hs + as_
        names = hn + an
        
        
        home_ranges = get_playing_ranges(home_minutes)
        away_ranges = get_playing_ranges(away_minutes)

        # ✅ Handle goal scorers (insert + realistic timing)
        if scorers:
            scorer_minutes = []
            for (pid, fix_id), name in zip(scorers, names):
                # Pick a random valid minute within player’s actual time range
                if pid in home_ranges:
                    start, end = home_ranges[pid]
                elif pid in away_ranges:
                    start, end = away_ranges[pid]
                else:
                    start, end = (1, 90)  # fallback
            
                minute = random.randint(max(1, int(start)), min(94, int(end)))
                scorer_minutes.append((pid, fix_id, minute))

            # Sort all by time for consistency
            scorer_minutes.sort(key=lambda x: x[2])

            # Debug print if enabled
            if LEAGUE_DEBUGGING or CUP_DEBUGGING:
                pretty = ", ".join([
                    f"{nm} {m if m < 91 else '90+' + str(m-90)}'"
                    for (pid, fix_id, m), nm in zip(scorer_minutes, names)
                ])
                print("   Scorers:", pretty)

            # Insert into DB
            cur.executemany("""
                INSERT INTO match_scorers (player_id, fixture_id, goal_minute)
                VALUES (?, ?, ?)
            """, scorer_minutes)



        # Merge all
        all_players = home_players + away_players
        all_minutes = {**home_minutes, **away_minutes}

        # Build goal map for scorers
        goal_map = {}
        for pid, fix_id in scorers:
            goal_map[pid] = goal_map.get(pid, 0) + 1

        def stat_from_attr(attr, factor, minutes_played, base_factor=0.01, spread=0.2):
            """Scales production by minutes played and ability."""
            if minutes_played <= 0:
                return 0
            base = attr / 1000.0 * factor * (minutes_played / 90.0)
            return max(0, int(base * random.uniform(1 - spread, 1 + spread)))

        def make_stats(pid, pos, defending, passing, scoring, goalkeeping, speed, ability):
            minutes_played = all_minutes.get(pid, 0)   # players not in today’s XI get 0
            if minutes_played == 0:
                return None  # skip creating a row for DNPs (or keep it if you want 0-min rows)
            if minutes_played == 0:
                return None
        
            if pos in ("GK",):
                tackles_a = stat_from_attr(defending, 2, minutes_played)
                tackles_c = int(tackles_a * random.uniform(0.7, 0.95))
                passes_a = stat_from_attr(passing, 25, minutes_played)
                passes_c = int(passes_a * random.uniform(0.85, 0.99))
                shoots_a = shoots_t = 0
            elif pos in ("CB", "RB", "LB", "CDM"):
                tackles_a = stat_from_attr(defending, 6, minutes_played)
                tackles_c = int(tackles_a * random.uniform(0.6, 0.9))
                passes_a = stat_from_attr(passing, 35, minutes_played)
                passes_c = int(passes_a * random.uniform(0.85, 0.99))
                shoots_a = stat_from_attr(scoring, 1, minutes_played)
                shoots_t = int(shoots_a * random.uniform(0.4, 0.7))
            elif pos in ("CM", "RM", "LM", "CAM"):
                tackles_a = stat_from_attr(defending, 4, minutes_played)
                tackles_c = int(tackles_a * random.uniform(0.6, 0.9))
                passes_a = stat_from_attr(passing, 60, minutes_played)
                passes_c = int(passes_a * random.uniform(0.85, 1.0))
                shoots_a = stat_from_attr(scoring, 3, minutes_played)
                shoots_t = int(shoots_a * random.uniform(0.4, 0.7))
            else:
                tackles_a = stat_from_attr(defending, 2, minutes_played)
                tackles_c = int(tackles_a * random.uniform(0.5, 0.8))
                passes_a = stat_from_attr(passing, 30, minutes_played)
                passes_c = int(passes_a * random.uniform(0.8, 0.95))
                shoots_a = stat_from_attr(scoring, 5, minutes_played)
                shoots_t = int(shoots_a * random.uniform(0.4, 0.7))
        
            yellow = 1 if random.random() < (0.06 if pos in ("CB", "CDM", "RB", "LB") else 0.02) else 0
            red = 1 if yellow and random.random() < 0.1 else 0
            goals = goal_map.get(pid, 0)
        
            # 🧠 Ensure scorers always have at least one shot and one on target
            if goals > 0:
                shoots_a = max(shoots_a, goals)
                shoots_t = max(shoots_t, goals)
        
            return (
                pid, fixture_id, pid_to_club.get(pid), minutes_played,
                tackles_a, tackles_c,
                passes_a, passes_c,
                shoots_a, shoots_t,
                goals, yellow, red
            )



        for pid, pos, defending, passing, scoring, gk, speed, ability in all_players:
            record = make_stats(pid, pos, defending, passing, scoring, gk, speed, ability)
            if record:
                player_stats.append(record)

        cur.executemany("""
            INSERT INTO players_stats (
                player_id, fixture_id, club_id,
                minutes_played, tackles_attempted, tackles_comp,
                passes_attempted, passes_comp,
                shoots_attempted, shoots_target,
                goals_scored, yellow_cards, red_cards
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, player_stats)





    conn.commit()