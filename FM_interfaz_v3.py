import pygame
import sys
from dataclasses import dataclass
from typing import List, Tuple, Optional
import datetime
import sqlite3

pygame.init()

# --- Config & Theme ---
WIDTH, HEIGHT = 1500, 800
SCREEN = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption("Football Manager–style UI (PyGame Demo)")

FONT = pygame.font.SysFont("Segoe UI", 18)
FONT_BOLD = pygame.font.SysFont("Segoe UI", 18, bold=True)
FONT_SMALL = pygame.font.SysFont("Segoe UI", 14)

BG = pygame.Color(20, 24, 28)
TOPBAR = pygame.Color(25, 30, 36)
SIDEBAR = pygame.Color(24, 28, 34)
CARD = pygame.Color(30, 36, 44)
ACCENT = pygame.Color(0, 120, 215)
ACCENT_DIM = pygame.Color(0, 90, 170)
TEXT = pygame.Color(220, 228, 235)
TEXT_DIM = pygame.Color(170, 180, 190)
GRID = pygame.Color(50, 58, 68)
ROW_ALT = pygame.Color(34, 40, 48)
HOVER = pygame.Color(50, 60, 72)
WARNING = pygame.Color(210, 80, 80)
SUCCESS = pygame.Color(60, 165, 90)
DANGER = (200, 50, 50)

# Sidebar: keep earlier indices; append League Table
LEFT_TAB_LIST = [
    "Overview", "Squad", "Tactics", "Training", "Transfers",
    "Inbox", "Finances", "Fixtures", "League Table"
]

CLOCK = pygame.time.Clock()

# Load pitch image if available
try:
    FIELD_IMG = pygame.image.load("Field.png")
    FIELD_IMG = pygame.transform.smoothscale(FIELD_IMG, (800, 500))
except Exception:
    FIELD_IMG = pygame.Surface((800, 500))
    FIELD_IMG.fill((20, 110, 20))
    pygame.draw.rect(FIELD_IMG, (255, 255, 255), FIELD_IMG.get_rect(), 4)

COMPETITION_ID = 1
DB_PATH = "db/fm_database.sqlite"

POSITION_ORDER = {
    "GK": 0, "CB": 1, "RB": 2, "LB": 3, "CDM": 4, "CM": 5, "CAM": 6,
    "RM": 7, "LM": 8, "RW": 9, "LW": 10, "ST": 11
}

def set_rows(self, rows: List[List[str]]):
    self.rows = rows
    self.apply_sort()
    # Reset scroll to avoid overscrolling after table gets shorter
    self.scroll_y = 0

def draw_text_centered(surface, text, font, color, center):
    s = font.render(text, True, color)
    r = s.get_rect(center=center)
    surface.blit(s, r)

def build_labels(formation_str):
    lines = [int(x) for x in formation_str.split("-")]
    labels = ["GK"]
    for idx, num in enumerate(lines):
        if idx == 0:  # defenders
            if num == 4:
                labels += ["LB", "LCB", "RCB", "RB"]
            elif num == 3:
                labels += ["LCB", "CB", "RCB"]
            elif num == 5:
                labels += ["LWB", "LCB", "CB", "RCB", "RWB"]
            elif num == 6:
                labels += ["LWB", "LCB", "CB", "CB", "RWB"]
            else:
                labels += [f"DF{i+1}" for i in range(num)]
        elif idx == len(lines) - 1:  # attackers
            if num == 2:
                labels += ["LS", "RS"]
            elif num == 3:
                labels += ["LW", "ST", "RW"]
            elif num == 4:
                labels += ["LW", "LS", "RS", "RW"]
            elif num == 1:
                labels += ["ST"]
            else:
                labels += [f"AT{i+1}" for i in range(num)]
        else:  # midfielders
            if num == 4:
                labels += ["LM", "LCM", "RCM", "RM"]
            elif num == 3:
                labels += ["LCM", "CM", "RCM"]
            elif num == 2:
                labels += ["LCM", "RCM"]
            elif num == 5:
                labels += ["LM", "LCM", "DM", "RCM", "RM"]
            elif num == 6:
                labels += ["LM", "LCM", "DM", "DM", "RCM", "RM"]
            else:
                labels += [f"MF{i+1}" for i in range(num)]
    return labels

def role_layer(role):
    if role == "GK": return 0
    if role in ["LB", "LCB", "RCB", "RB", "CB"]: return 1
    if role in ["LWB", "RWB", "DM", "DM1", "DM2"]: return 1.5
    if role in ["LM", "LCM", "CM", "RCM", "RM", "MF1", "MF2", "MF3", "MF4"]: return 2
    if role in ["LWF", "RWF", "LW", "RW"]: return 2.5
    if role in ["ST", "CF", "LS", "RS", "AT1", "AT2"]: return 3
    return 2

def generate_positions_dynamic(labels, field_rect):
    positions = []
    left_margin, right_margin, top_margin, bottom_margin = 40, 40, 40, 40
    usable_width = field_rect.width - left_margin - right_margin
    usable_height = field_rect.height - top_margin - bottom_margin
    max_layer = 3

    layers = {}
    for role in labels:
        layer = role_layer(role)
        layers.setdefault(layer, []).append(role)

    for layer, roles in layers.items():
        x = field_rect.left + left_margin + int((usable_width / max_layer) * layer)
        if layer != 0:
            x -= 120
        y_positions = []
        n = len(roles)
        for i, role in enumerate(roles):
            if role in ["LW", "LWF", "LWB"]:
                y = field_rect.top + top_margin + int(usable_height * 0.1)
            elif role in ["RW", "RWF", "RWB"]:
                y = field_rect.top + top_margin + int(usable_height * 0.9)
            else:
                spacing = usable_height / (n + 1)
                y = field_rect.top + top_margin + spacing * (i + 1)
            y_positions.append(y)
        for role, y in zip(roles, y_positions):
            positions.append((role, (int(x), int(y))))
    return positions

def draw_tactic_dynamic(surf, field_rect, formation_str):
    labels = build_labels(formation_str)
    positions = generate_positions_dynamic(labels, field_rect)
    for role, (x, y) in positions:
        pygame.draw.circle(surf, (30, 144, 255), (x, y), 18)
        pygame.draw.circle(surf, (255, 255, 255), (x, y), 18, 2)
        draw_text_centered(surf, role, FONT, (255, 255, 255), (x, y))

def draw_text(surface, text, font, color, pos, center_y=False, bold=False):
    f = FONT_BOLD if bold else font
    s = f.render(text, True, color)
    r = s.get_rect(topleft=pos)
    if center_y:
        r.y = pos[1] - r.h // 2
    surface.blit(s, r)
    return r

# --- Data Layer ---
@dataclass
class Player:
    first_name: str
    last_name: str
    age: int
    nationality: str
    position: str
    club_name: str
    value: int
    wage: int
    contract_until: str

def load_players_from_db() -> List[Player]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT p.first_name, p.last_name, p.date_of_birth, p.position, c.name as club_name
        FROM players p
        JOIN clubs c ON p.club_id = c.id
        WHERE p.is_retired = 0
    """)
    rows = cur.fetchall()
    conn.close()

    today = datetime.date.today()
    players = []
    for first, last, dob, position, club_name in rows:
        try:
            dob_date = datetime.date.fromisoformat(dob) if dob else None
        except Exception:
            dob_date = None
        if dob_date:
            age = today.year - dob_date.year - ((today.month, today.day) < (dob_date.month, dob_date.day))
        else:
            age = 0
        players.append(Player(
            first_name=first,
            last_name=last,
            age=age,
            nationality="?",   # extend if stored
            position=position,
            club_name=club_name,
            value=0,           # extend if stored
            wage=0,            # extend if stored
            contract_until="-" # extend if stored
        ))
    return players

SAMPLE_PLAYERS = load_players_from_db()

@dataclass
class Fixture:
    date: str
    competition: str
    home_team: str
    away_team: str
    result: Optional[str]
    stadium: str

def load_fixtures_from_db(season: Optional[str] = None) -> List[Fixture]:
    """
    Load fixtures optionally filtered by computed season string 'YYYY-YYYY'.
    Season boundaries: Aug 1 – Dec 31 belong to start year; Jan 1 – Jul 31 belong to end year.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    base_sql = """
        SELECT 
            f.fixture_date,
            co.name AS competition,
            hc.name,
            ac.name,
            f.home_goals,
            f.away_goals,
            hc.stadium
        FROM fixtures f
        JOIN clubs hc ON f.home_club_id = hc.id
        JOIN clubs ac ON f.away_club_id = ac.id
        JOIN competitions co ON co.id = f.competition_id
    """
    season_case = """
        CASE 
            WHEN strftime('%m-%d', f.fixture_date) BETWEEN '08-01' AND '12-31'
                THEN strftime('%Y', f.fixture_date) || '-' || (CAST(strftime('%Y', f.fixture_date) AS INTEGER) + 1)
            ELSE (CAST(strftime('%Y', f.fixture_date) AS INTEGER) - 1) || '-' || strftime('%Y', f.fixture_date)
        END
    """
    params = []
    if season:
        sql = base_sql + f" WHERE {season_case} = ? ORDER BY f.fixture_date ASC"
        params = [season]
    else:
        sql = base_sql + " ORDER BY f.fixture_date ASC"

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    fixtures = []
    for date, comp, home, away, home_goals, away_goals, stadium in rows:
        result = f"{home_goals}-{away_goals}" if home_goals is not None and away_goals is not None else "-"
        fixtures.append(Fixture(date, comp, home, away, result, stadium or "-"))
    return fixtures

def load_seasons_for_fixtures() -> List[str]:
    """Distinct seasons present in fixtures across all competitions."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT season FROM (
            SELECT CASE 
                WHEN strftime('%m-%d', f.fixture_date) BETWEEN '08-01' AND '12-31'
                    THEN strftime('%Y', f.fixture_date) || '-' || (CAST(strftime('%Y', f.fixture_date) AS INTEGER) + 1)
                ELSE (CAST(strftime('%Y', f.fixture_date) AS INTEGER) - 1) || '-' || strftime('%Y', f.fixture_date)
            END AS season
            FROM fixtures f
        ) s
        ORDER BY season ASC
    """)
    seasons = [r[0] for r in cur.fetchall()]
    conn.close()
    return seasons

def load_seasons_from_db(competition_id: int = COMPETITION_ID) -> List[str]:
    """Distinct seasons for a specific competition (used by league table)."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT season FROM (
            SELECT CASE 
                WHEN strftime('%m-%d', f.fixture_date) BETWEEN '08-01' AND '12-31'
                    THEN strftime('%Y', f.fixture_date) || '-' || (CAST(strftime('%Y', f.fixture_date) AS INTEGER) + 1)
                ELSE (CAST(strftime('%Y', f.fixture_date) AS INTEGER) - 1) || '-' || strftime('%Y', f.fixture_date)
            END AS season
            FROM fixtures f
            WHERE f.competition_id = ?
        ) t
        ORDER BY season ASC
    """, (competition_id,))
    seasons = [r[0] for r in cur.fetchall()]
    conn.close()
    return seasons

def load_league_table_for_season(season: str, competition_id: int = COMPETITION_ID) -> List[List]:
    """
    Build league table rows for a given season.
    Returns list of [#, Club, MP, W, D, L, GF, GA, GD, Pts].
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    WITH season_matches AS (
        SELECT 
            f.home_club_id,
            f.away_club_id,
            f.home_goals,
            f.away_goals,
            CASE 
                WHEN strftime('%m-%d', f.fixture_date) BETWEEN '08-01' AND '12-31'
                    THEN strftime('%Y', f.fixture_date) || '-' || (CAST(strftime('%Y', f.fixture_date) AS INTEGER) + 1)
                ELSE (CAST(strftime('%Y', f.fixture_date) AS INTEGER) - 1) || '-' || strftime('%Y', f.fixture_date)
            END AS season
        FROM fixtures f
        JOIN competitions comp ON comp.id = f.competition_id
        WHERE f.competition_id = ?
          AND comp.is_league = 1
          AND comp.is_cup = 0
          AND f.played = 1
    ),
    club_stats AS (
        SELECT
            cc.club_id,
            SUM(
                CASE 
                  WHEN (sm.home_club_id = cc.club_id AND sm.home_goals > sm.away_goals) OR
                       (sm.away_club_id = cc.club_id AND sm.away_goals > sm.home_goals)
                  THEN 1 ELSE 0 END
            ) AS wins,
            SUM(
                CASE 
                  WHEN sm.home_goals = sm.away_goals AND (sm.home_club_id = cc.club_id OR sm.away_club_id = cc.club_id)
                  THEN 1 ELSE 0 END
            ) AS draws,
            SUM(
                CASE 
                  WHEN (sm.home_club_id = cc.club_id AND sm.home_goals < sm.away_goals) OR
                       (sm.away_club_id = cc.club_id AND sm.away_goals < sm.home_goals)
                  THEN 1 ELSE 0 END
            ) AS losses,
            SUM(
                CASE 
                  WHEN (sm.home_club_id = cc.club_id AND sm.home_goals > sm.away_goals) OR
                       (sm.away_club_id = cc.club_id AND sm.away_goals > sm.home_goals)
                  THEN 3
                  WHEN sm.home_goals = sm.away_goals AND (sm.home_club_id = cc.club_id OR sm.away_club_id = cc.club_id)
                  THEN 1
                  ELSE 0 END
            ) AS points,
            SUM(CASE WHEN sm.home_club_id = cc.club_id THEN sm.home_goals
                     WHEN sm.away_club_id = cc.club_id THEN sm.away_goals ELSE 0 END) AS goals_for,
            SUM(CASE WHEN sm.home_club_id = cc.club_id THEN sm.away_goals
                     WHEN sm.away_club_id = cc.club_id THEN sm.home_goals ELSE 0 END) AS goals_against,
            SUM(CASE WHEN sm.home_club_id = cc.club_id OR sm.away_club_id = cc.club_id THEN 1 ELSE 0 END) AS matches_played
        FROM clubs_competition cc
        JOIN competitions comp ON comp.id = cc.competition_id
        LEFT JOIN season_matches sm 
               ON sm.season = ?
              AND (sm.home_club_id = cc.club_id OR sm.away_club_id = cc.club_id)
        WHERE cc.competition_id = ?
          AND cc.is_active = 1
          AND comp.is_league = 1
          AND comp.is_cup = 0
        GROUP BY cc.club_id
    )
    SELECT c.name AS club,
           COALESCE(cs.matches_played,0) AS MP,
           COALESCE(cs.wins,0) AS W,
           COALESCE(cs.draws,0) AS D,
           COALESCE(cs.losses,0) AS L,
           COALESCE(cs.goals_for,0) AS GF,
           COALESCE(cs.goals_against,0) AS GA,
           COALESCE(cs.points,0) AS Pts
    FROM club_stats cs
    JOIN clubs c ON c.id = cs.club_id
    ORDER BY Pts DESC, (GF-GA) DESC, GF DESC, c.name ASC
    """, (competition_id, season, competition_id))
    rows = cur.fetchall()
    conn.close()

    out = []
    for idx, (club, mp, w, d, l, gf, ga, pts) in enumerate(rows, start=1):
        out.append([idx, club, mp, w, d, l, gf, ga, gf - ga, pts])
    return out

# --- UI Components ---
class Button:
    def __init__(self, rect: pygame.Rect, text: str, on_click=None, variant="primary"):
        self.rect = pygame.Rect(rect)
        self.text = text
        self.on_click = on_click
        self.variant = variant  # "primary"=accent, "danger"=red, "ghost"=neutral
        self.hovered = False

    def draw(self, surf):
        if self.variant == "primary":
            base = ACCENT_DIM if self.hovered else ACCENT
        elif self.variant == "danger":
            base = (180, 40, 40) if self.hovered else DANGER
        else:
            base = CARD
        rect_to_draw = self.rect.copy()
        if self.hovered:
            rect_to_draw.inflate_ip(6, 4)
        pygame.draw.rect(surf, base, rect_to_draw, border_radius=10)
        pygame.draw.rect(surf, GRID, rect_to_draw, 2, border_radius=10)
        text_surf = FONT_BOLD.render(self.text, True, TEXT)
        text_rect = text_surf.get_rect(center=rect_to_draw.center)
        surf.blit(text_surf, text_rect)

    def handle(self, event):
        if event.type == pygame.MOUSEMOTION:
            self.hovered = self.rect.collidepoint(event.pos)
        elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
            if self.rect.collidepoint(event.pos) and self.on_click:
                self.on_click()

class TabBar:
    def __init__(self, rect: pygame.Rect, tabs: List[str], on_change):
        self.rect = pygame.Rect(rect)
        self.tabs = tabs
        self.active = 0
        self.on_change = on_change

    def draw(self, surf):
        pygame.draw.rect(surf, CARD, self.rect, border_radius=12)
        x = self.rect.x + 8
        y = self.rect.y + 6
        for i, t in enumerate(self.tabs):
            pad = 14
            label = FONT_BOLD.render(t, True, TEXT if i == self.active else TEXT_DIM)
            lw, lh = label.get_size()
            tab_rect = pygame.Rect(x, y, lw + pad * 2, self.rect.h - 12)
            if i == self.active:
                pygame.draw.rect(surf, ACCENT, tab_rect, border_radius=10)
            surf.blit(label, (x + pad, y + (tab_rect.h - lh) // 2))
            x += tab_rect.w + 6

    def handle(self, event):
        if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
            x = self.rect.x + 8
            y = self.rect.y + 6
            for i, t in enumerate(self.tabs):
                lw, lh = FONT_BOLD.size(t)
                tab_rect = pygame.Rect(x, y, lw + 28, self.rect.h - 12)
                if tab_rect.collidepoint(event.pos):
                    self.active = i
                    self.on_change(i)
                    break
                x += tab_rect.w + 6

class Sidebar:
    def __init__(self, rect: pygame.Rect, items: List[str], on_change):
        self.rect = pygame.Rect(rect)
        self.items = items
        self.active = 0
        self.on_change = on_change
        self.item_h = 40

    def draw(self, surf):
        pygame.draw.rect(surf, SIDEBAR, self.rect)
        pygame.draw.line(surf, GRID, (self.rect.right, self.rect.y), (self.rect.right, self.rect.bottom))
        y = self.rect.y + 80
        draw_text(surf, "⚽ Club 24/25", FONT_BOLD, TEXT, (self.rect.x + 16, self.rect.y + 20))
        for i, it in enumerate(self.items):
            r = pygame.Rect(self.rect.x + 8, y, self.rect.w - 16, self.item_h)
            if i == self.active:
                pygame.draw.rect(surf, ACCENT, r, border_radius=8)
            else:
                pygame.draw.rect(surf, CARD, r, border_radius=8)
            draw_text(surf, it, FONT, TEXT if i == self.active else TEXT_DIM, (r.x + 12, r.y + 10))
            y += self.item_h + 8

    def handle(self, event):
        if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
            y = self.rect.y + 80
            for i, _ in enumerate(self.items):
                r = pygame.Rect(self.rect.x + 8, y, self.rect.w - 16, self.item_h)
                if r.collidepoint(event.pos):
                    self.active = i
                    self.on_change(i)
                    break
                y += self.item_h + 8

class Modal:
    def __init__(self, rect: pygame.Rect, title: str, content: List[str], on_close=None):
        self.rect = pygame.Rect(rect)
        self.title = title
        self.content = content
        self.on_close = on_close
        self.close_btn = Button(pygame.Rect(self.rect.right - 110, self.rect.y + 16, 95, 36), "Close", self.close)

    def close(self):
        if self.on_close:
            self.on_close()

    def draw(self, surf):
        overlay = pygame.Surface(surf.get_size(), pygame.SRCALPHA)
        overlay.fill((0, 0, 0, 150))
        surf.blit(overlay, (0, 0))
        pygame.draw.rect(surf, CARD, self.rect, border_radius=16)
        pygame.draw.rect(surf, GRID, self.rect, 2, border_radius=16)
        draw_text(surf, self.title, FONT_BOLD, TEXT, (self.rect.x + 16, self.rect.y + 20))
        y = self.rect.y + 70
        for line in self.content:
            draw_text(surf, line, FONT, TEXT, (self.rect.x + 16, y))
            y += 26
        self.close_btn.draw(surf)

    def handle(self, event):
        self.close_btn.handle(event)

class Table:
    def __init__(self, rect: pygame.Rect, headers: List[Tuple[str, int]], row_h=36):
        self.rect = pygame.Rect(rect)
        self.headers = headers
        self.row_h = row_h
        self.scroll_y = 0
        self.sort_keys = [(0, False)]
        self.rows: List[List[str]] = []
        self.hover_row: Optional[int] = None
        self.dragging = False
        self.drag_offset = 0

    def set_rows(self, rows: List[List[str]]):
        self.rows = rows
        self.apply_sort()

    def apply_sort(self):
        for idx, reverse in reversed(self.sort_keys):
            def key(row):
                if self.headers[idx][0] == "Position":
                    return POSITION_ORDER.get(row[idx], 99)
                try:
                    return float(row[idx])
                except (ValueError, TypeError):
                    return str(row[idx])
            self.rows.sort(key=key, reverse=reverse)

    def draw(self, surf):
        pygame.draw.rect(surf, CARD, self.rect, border_radius=12)
        pygame.draw.rect(surf, GRID, self.rect, 2, border_radius=12)
        x = self.rect.x + 12
        col_rects = []
        for i, (h, w) in enumerate(self.headers):
            hr = pygame.Rect(x, self.rect.y + 8, w, self.row_h)
            tag = ""
            if self.sort_keys and self.sort_keys[0][0] == i:
                tag = " ↓" if self.sort_keys[0][1] else " ↑"
            draw_text(surf, h + tag, FONT_BOLD, TEXT, (hr.x + 6, hr.y + 8))
            col_rects.append(hr)
            x += w
        pygame.draw.line(surf, GRID, (self.rect.x, self.rect.y + self.row_h + 12), (self.rect.right, self.rect.y + self.row_h + 12))
        clip = surf.get_clip()
        body_rect = pygame.Rect(self.rect.x + 6, self.rect.y + self.row_h + 16, self.rect.w - 12, self.rect.h - self.row_h - 22)
        surf.set_clip(body_rect)
        visible_start = max(0, self.scroll_y // self.row_h)
        visible_count = (body_rect.h // self.row_h) + 2
        for i in range(visible_start, min(len(self.rows), visible_start + visible_count)):
            row = self.rows[i]
            ry = body_rect.y + (i * self.row_h) - self.scroll_y
            rr = pygame.Rect(body_rect.x, ry, body_rect.w, self.row_h)
            if i % 2 == 0:
                pygame.draw.rect(surf, ROW_ALT, rr)
            if self.hover_row == i:
                pygame.draw.rect(surf, HOVER, rr)
            x = self.rect.x + 12
            for c, (_, w) in enumerate(self.headers):
                draw_text(surf, str(row[c]), FONT, TEXT, (x + 6, ry + 8))
                x += w
            pygame.draw.line(surf, GRID, (rr.x, rr.bottom - 1), (rr.right, rr.bottom - 1))
        total_h = max(1, len(self.rows) * self.row_h)
        if total_h > body_rect.h:
            thumb_h = max(48, int(body_rect.h * (body_rect.h / total_h)))
            max_scroll = max(0, total_h - body_rect.h)   # <-- FIX: use body_rect.h, not body_h
            y_ratio = (self.scroll_y / max_scroll) if max_scroll else 0
            thumb_y = body_rect.y + int((body_rect.h - thumb_h) * y_ratio)
            self.scrollbar_rect = pygame.Rect(body_rect.right - 7, thumb_y, 6, thumb_h)
            pygame.draw.rect(surf, GRID, (body_rect.right - 6, body_rect.y, 4, body_rect.h), border_radius=2)
            pygame.draw.rect(surf, TEXT_DIM, (body_rect.right - 12, thumb_y, 12, thumb_h), border_radius=5)
        else:
            self.scrollbar_rect = None
        surf.set_clip(clip)
        return col_rects



    def handle(self, event, header_rects=None):
        body_top = self.rect.y + self.row_h + 16
        body_h = self.rect.h - self.row_h - 22
        total_h = max(1, len(self.rows) * self.row_h)
        max_scroll = total_h - body_h

        if event.type == pygame.MOUSEWHEEL:
            self.scroll_y = max(0, min(max_scroll, self.scroll_y - event.y * 60))
        elif event.type == pygame.MOUSEMOTION:
            _, y = event.pos
            if self.rect.collidepoint(event.pos) and y >= body_top:
                idx = (y - body_top + self.scroll_y) // self.row_h
                self.hover_row = int(idx) if 0 <= idx < len(self.rows) else None
            else:
                self.hover_row = None
            if self.dragging and self.scrollbar_rect:
                rel_y = y - self.drag_offset - body_top
                ratio = max(0, min(1, rel_y / (body_h - self.scrollbar_rect.h)))
                self.scroll_y = int(ratio * max_scroll)
        elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
            if header_rects:
                for i, hr in enumerate(header_rects):
                    if hr.collidepoint(event.pos):
                        if self.sort_keys and self.sort_keys[0][0] == i:
                            current_reverse = self.sort_keys[0][1]
                            self.sort_keys[0] = (i, not current_reverse)
                        else:
                            self.sort_keys = [sk for sk in self.sort_keys if sk[0] != i]
                            self.sort_keys.insert(0, (i, False))
                        self.apply_sort()
                        break
            if self.scrollbar_rect and self.scrollbar_rect.collidepoint(event.pos):
                self.dragging = True
                self.drag_offset = event.pos[1] - self.scrollbar_rect.y
            elif self.scrollbar_rect:
                if event.pos[1] < self.scrollbar_rect.y:
                    self.scroll_y = max(0, self.scroll_y - body_h)
                elif event.pos[1] > self.scrollbar_rect.bottom:
                    self.scroll_y = min(max_scroll, self.scroll_y + body_h)
        elif event.type == pygame.MOUSEBUTTONUP and event.button == 1:
            self.dragging = False

# --- Screen ---
class ManagerScreen:
    def __init__(self, stop_callback):
        self.stop_callback = stop_callback
        self.current_date = datetime.date.today()

        self.sidebar = Sidebar(pygame.Rect(0, 0, 220, HEIGHT),
                               LEFT_TAB_LIST,
                               self.on_sidebar_change)
        self.active_section = 0

        # Squad tabbar
        self.tabbar = TabBar(pygame.Rect(236, 82, WIDTH - 252, 46),
                             ["Squad List", "Depth Chart", "Stats"],
                             self.on_tab_change)
        self.active_tab = 0

        # Squad table
        headers = [
            ("First Name", 140), ("Last Name", 140), ("Age", 50),
            ("Nationality", 120), ("Position", 80), ("Club", 220),
            ("Value", 80), ("Wage", 80), ("Contract Until", 120)
        ]
        self.table = Table(pygame.Rect(236, 138, WIDTH - 252, HEIGHT - 160), headers)
        self.table.set_rows([
            [p.first_name, p.last_name, p.age, p.nationality, p.position, p.club_name, p.value, p.wage, p.contract_until]
            for p in SAMPLE_PLAYERS
        ])

        # --- Fixtures screen (with season selector) ---
        self.fixtures_seasons = load_seasons_for_fixtures()
        self.selected_fixtures_season_idx = len(self.fixtures_seasons) - 1 if self.fixtures_seasons else -1
        self.selected_fixtures_season = self.fixtures_seasons[self.selected_fixtures_season_idx] if self.selected_fixtures_season_idx >= 0 else None

        self.btn_prev_fixt_season = Button(pygame.Rect(236, 92, 40, 32), "◀", on_click=self.prev_fixtures_season, variant="ghost")
        self.btn_next_fixt_season = Button(pygame.Rect(540, 92, 40, 32), "▶", on_click=self.next_fixtures_season, variant="ghost")

        fixtures = load_fixtures_from_db(self.selected_fixtures_season)
        self.fixtures_table = Table(
            pygame.Rect(236, 138, WIDTH - 252, HEIGHT - 160),
            [("Date", 140), ("Competition", 240), ("Home", 240), ("Away", 240), ("Result", 100), ("Stadium", 160)]
        )
        self.fixtures_table.set_rows([
            [f.date, f.competition, f.home_team, f.away_team, f.result, f.stadium] for f in fixtures
        ])

        # --- League Table screen (with season selector) ---
        self.seasons = load_seasons_from_db(COMPETITION_ID) or []
        self.selected_season_idx = len(self.seasons) - 1 if self.seasons else -1
        self.selected_season = self.seasons[self.selected_season_idx] if self.selected_season_idx >= 0 else None

        self.btn_prev_season = Button(pygame.Rect(236, 92, 40, 32), "◀", on_click=self.prev_season, variant="ghost")
        self.btn_next_season = Button(pygame.Rect(540, 92, 40, 32), "▶", on_click=self.next_season, variant="ghost")

        self.league_table = Table(
            pygame.Rect(236, 138, WIDTH - 252, HEIGHT - 160),
            [("#", 50), ("Club", 300), ("MP", 70), ("W", 60), ("D", 60), ("L", 60),
             ("GF", 60), ("GA", 60), ("GD", 70), ("Pts", 80)]
        )
        self.refresh_league_table()

        # Topbar buttons
        self.btn_save = Button(pygame.Rect(WIDTH - 310, 22, 100, 36), "Save")
        self.btn_continue = Button(pygame.Rect(WIDTH - 210, 22, 100, 36), "Continue", self.on_continue)
        self.btn_quit = Button(pygame.Rect(WIDTH - 110, 22, 100, 36), "Quit", on_click=self.stop_callback, variant="danger")

        # Tactics
        self.current_tactic = "4-4-2"
        self.tactic_list = ["4-4-2", "4-3-3", "4-5-1", "3-5-2", "4-2-4"]
        self.btn_tactic = Button(pygame.Rect(1260, 120, 120, 32), self.current_tactic, on_click=self.cycle_tactic)

        self.show_modal = False
        self.modal = Modal(pygame.Rect(WIDTH//2 - 260, HEIGHT//2 - 160, 520, 320),
                           "Transfer Offer",
                           ["Club: Union Bergstadt", "Player: R. Campos", "Fee: €12.5M", "Wage: €52k/wk"],
                           on_close=self.toggle_modal)

    # --- Fixtures helpers ---
    def refresh_fixtures_table(self):
        fixtures = load_fixtures_from_db(self.selected_fixtures_season)
        self.fixtures_table.set_rows([
            [f.date, f.competition, f.home_team, f.away_team, f.result, f.stadium] for f in fixtures
        ])

    def prev_fixtures_season(self):
        if not self.fixtures_seasons:
            return
        self.selected_fixtures_season_idx = (self.selected_fixtures_season_idx - 1) % len(self.fixtures_seasons)
        self.selected_fixtures_season = self.fixtures_seasons[self.selected_fixtures_season_idx]
        self.refresh_fixtures_table()

    def next_fixtures_season(self):
        if not self.fixtures_seasons:
            return
        self.selected_fixtures_season_idx = (self.selected_fixtures_season_idx + 1) % len(self.fixtures_seasons)
        self.selected_fixtures_season = self.fixtures_seasons[self.selected_fixtures_season_idx]
        self.refresh_fixtures_table()

    # --- League Table helpers ---
    def refresh_league_table(self):
        if self.selected_season:
            rows = load_league_table_for_season(self.selected_season, COMPETITION_ID)
        else:
            rows = []
        self.league_table.set_rows(rows)

    def prev_season(self):
        if not self.seasons:
            return
        self.selected_season_idx = (self.selected_season_idx - 1) % len(self.seasons)
        self.selected_season = self.seasons[self.selected_season_idx]
        self.refresh_league_table()

    def next_season(self):
        if not self.seasons:
            return
        self.selected_season_idx = (self.selected_season_idx + 1) % len(self.seasons)
        self.selected_season = self.seasons[self.selected_season_idx]
        self.refresh_league_table()

    # --- Other screen handlers ---
    def on_continue(self):
        self.current_date += datetime.timedelta(days=1)

    def toggle_modal(self):
        self.show_modal = not self.show_modal

    def on_sidebar_change(self, idx: int):
        self.active_section = idx

    def on_tab_change(self, idx: int):
        self.active_tab = idx

    def cycle_tactic(self):
        i = self.tactic_list.index(self.current_tactic)
        i = (i + 1) % len(self.tactic_list)
        self.current_tactic = self.tactic_list[i]
        self.btn_tactic.text = self.current_tactic

    def handle(self, event):
        self.sidebar.handle(event)
        if self.active_section == 1:
            self.tabbar.handle(event)
            header_rects = self.table.draw(SCREEN)
            self.table.handle(event, header_rects)
        if self.active_section == 2:
            self.btn_tactic.handle(event)

        if self.active_section == 7:  # Fixtures
            self.btn_prev_fixt_season.handle(event)
            self.btn_next_fixt_season.handle(event)
            header_rects = self.fixtures_table.draw(SCREEN)
            self.fixtures_table.handle(event, header_rects)

        if self.active_section == 8:  # League Table
            self.btn_prev_season.handle(event)
            self.btn_next_season.handle(event)
            header_rects = self.league_table.draw(SCREEN)
            self.league_table.handle(event, header_rects)

        self.btn_save.handle(event)
        self.btn_continue.handle(event)
        self.btn_quit.handle(event)

        if self.show_modal:
            self.modal.handle(event)

    def draw_topbar(self, surf):
        r = pygame.Rect(0, 0, WIDTH, 70)
        pygame.draw.rect(surf, TOPBAR, r)
        pygame.draw.line(surf, GRID, (0, r.bottom), (WIDTH, r.bottom))
        draw_text(surf, "Manager Demo", FONT_BOLD, TEXT, (900, 22))
        date_str = self.current_date.strftime("%a %d %b %Y")
        draw_text(surf, date_str, FONT, TEXT_DIM, (320, 26))
        self.btn_save.draw(surf)
        self.btn_continue.draw(surf)
        self.btn_quit.draw(surf)

    def draw(self, surf):
        surf.fill(BG)
        self.draw_topbar(surf)
        self.sidebar.draw(surf)
        draw_text(surf, LEFT_TAB_LIST[self.active_section], FONT_BOLD, TEXT, (236, 50))

        if self.active_section == 1:  # Squad
            self.tabbar.draw(surf)
            if self.active_tab == 0:
                self.table.draw(surf)
            else:
                c = pygame.Rect(236, 138, WIDTH - 252, HEIGHT - 160)
                pygame.draw.rect(surf, CARD, c, border_radius=12)
                draw_text(surf, "Coming soon...", FONT, TEXT_DIM, (c.x + 16, c.y + 16))

        elif self.active_section == 2:  # Tactics
            c = pygame.Rect(236, 100, WIDTH - 252, HEIGHT - 120)
            pygame.draw.rect(surf, CARD, c, border_radius=12)
            pygame.draw.rect(surf, GRID, c, 2, border_radius=12)
            field_rect = FIELD_IMG.get_rect(center=c.center)
            surf.blit(FIELD_IMG, field_rect)
            draw_tactic_dynamic(surf, field_rect, self.current_tactic)
            self.btn_tactic.draw(surf)

        elif self.active_section == 7:  # Fixtures
            draw_text(surf, "Season:", FONT_BOLD, TEXT, (236, 68))
            self.btn_prev_fixt_season.draw(surf)
            self.btn_next_fixt_season.draw(surf)
            fixt_season_label = self.selected_fixtures_season if self.selected_fixtures_season else "No seasons"
            draw_text(surf, fixt_season_label, FONT_BOLD, TEXT, (286, 96))
            self.fixtures_table.draw(surf)

        elif self.active_section == 8:  # League Table
            draw_text(surf, "Season:", FONT_BOLD, TEXT, (236, 68))
            self.btn_prev_season.draw(surf)
            self.btn_next_season.draw(surf)
            season_label = self.selected_season if self.selected_season else "No seasons"
            draw_text(surf, season_label, FONT_BOLD, TEXT, (286, 96))
            self.league_table.draw(surf)

        else:
            c = pygame.Rect(236, 100, WIDTH - 252, HEIGHT - 120)
            pygame.draw.rect(surf, CARD, c, border_radius=12)
            draw_text(surf, "Coming soon...", FONT, TEXT_DIM, (c.x + 16, c.y + 16))

# --- Main loop ---
def main():
    running = True
    def stop():
        nonlocal running
        running = False

    screen = ManagerScreen(stop)

    while running:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            screen.handle(event)

        screen.draw(SCREEN)
        pygame.display.flip()
        CLOCK.tick(60)

    pygame.quit()
    sys.exit()

if __name__ == "__main__":
    main()
