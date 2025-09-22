from datetime import date

import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "db", "fm_database.sqlite")

# --- Game Date ---
GAME_DATE = date(2025, 9, 1)

# --- Global tuning knobs ---
GOAL_SCALING = 6.00   # try 0.85 for fewer goals, 1.15 for more


# --- League baselines (computed once lazily) ---
LEAGUE_ATK_MEAN = None
LEAGUE_DEF_MEAN = None


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

# --- Per-position attribute weights ---
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

# --- Attribute caps by position ---
POSITION_ATTRIBUTE_CAPS = {
    "GK": {"at_scoring": 150, "at_dribbling": 400, "at_passing": 700, "at_defending": 400, "at_goalkeeping": 2000},
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
