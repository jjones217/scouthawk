import sqlite3
import pandas as pd

POSITION_MAP = {
    0: '?',  1: 'SP', 2: 'RP',  3: 'C',
    4: '1B', 5: '2B', 6: '3B',  7: 'SS',
    8: 'LF', 9: 'CF', 10: 'RF', 11: 'DH',
    12: 'UT', 13: 'P', 14: 'OF',
}


class OOTPDatabase:
    def __init__(self, path: str):
        self.path = path
        # Open read-only so we never corrupt the save file
        self.conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
        self._tables: list[str] | None = None
        self._col_cache: dict[str, list[str]] = {}

    # ------------------------------------------------------------------
    # Schema helpers
    # ------------------------------------------------------------------

    @property
    def tables(self) -> list[str]:
        if self._tables is None:
            cur = self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            self._tables = [r[0] for r in cur.fetchall()]
        return self._tables

    def columns(self, table: str) -> list[str]:
        if table not in self._col_cache:
            cur = self.conn.execute(f"PRAGMA table_info([{table}])")
            self._col_cache[table] = [r[1] for r in cur.fetchall()]
        return self._col_cache[table]

    def find_table(self, *candidates: str) -> str | None:
        for c in candidates:
            if c in self.tables:
                return c
        # partial match fallback
        for c in candidates:
            for t in self.tables:
                if c.lower() in t.lower():
                    return t
        return None

    def schema_summary(self) -> dict[str, list[str]]:
        return {t: self.columns(t) for t in self.tables}

    def _q(self, sql: str, params=()) -> pd.DataFrame:
        return pd.read_sql_query(sql, self.conn, params=params)

    # ------------------------------------------------------------------
    # Current year detection
    # ------------------------------------------------------------------

    def get_current_year(self) -> int | None:
        # Check leagues table first
        for tname in ['leagues', 'league']:
            t = self.find_table(tname)
            if not t:
                continue
            cols = self.columns(t)
            y_col = next((c for c in cols if 'year' in c.lower()), None)
            if y_col:
                try:
                    val = self.conn.execute(f"SELECT MAX([{y_col}]) FROM [{t}]").fetchone()[0]
                    if val:
                        return int(str(val)[:4])
                except Exception:
                    pass

        # Fallback: max year in stats tables
        for tname in [
            'players_career_batting_stats',
            'players_career_pitching_stats',
            'player_batting_stats',
            'player_pitching_stats',
        ]:
            t = self.find_table(tname)
            if t and 'year' in self.columns(t):
                try:
                    val = self.conn.execute(f"SELECT MAX(year) FROM [{t}]").fetchone()[0]
                    if val:
                        return int(val)
                except Exception:
                    pass
        return None

    def get_years(self) -> list[int]:
        for tname in [
            'players_career_batting_stats',
            'players_career_pitching_stats',
        ]:
            t = self.find_table(tname)
            if t and 'year' in self.columns(t):
                try:
                    rows = self.conn.execute(
                        f"SELECT DISTINCT year FROM [{t}] ORDER BY year DESC"
                    ).fetchall()
                    return [int(r[0]) for r in rows if r[0]]
                except Exception:
                    pass
        return []

    # ------------------------------------------------------------------
    # Base DataFrames
    # ------------------------------------------------------------------

    def _players_df(self) -> pd.DataFrame:
        t = self.find_table('players')
        if not t:
            return pd.DataFrame()
        cols = self.columns(t)

        sel = []
        # id
        id_col = 'player_id' if 'player_id' in cols else ('id' if 'id' in cols else None)
        if not id_col:
            return pd.DataFrame()
        sel.append(f'[{id_col}] AS player_id')

        for col in ['first_name', 'last_name', 'position', 'age']:
            if col in cols:
                sel.append(f'[{col}]')

        for sal in ['salary', 'contract_salary', 'cur_salary']:
            if sal in cols:
                sel.append(f'[{sal}] AS salary')
                break

        df = self._q(f"SELECT {', '.join(sel)} FROM [{t}]")
        if 'position' in df.columns:
            df['POS'] = df['position'].map(POSITION_MAP).fillna(df['position'].astype(str))
        return df

    def _teams_df(self) -> pd.DataFrame:
        t = self.find_table('teams')
        if not t:
            return pd.DataFrame()
        cols = self.columns(t)

        sel = []
        id_col = 'team_id' if 'team_id' in cols else ('id' if 'id' in cols else None)
        if id_col:
            sel.append(f'[{id_col}] AS team_id')
        for col in ['name', 'abbr', 'abbreviation', 'nickname']:
            if col in cols:
                sel.append(f'[{col}]')
                break  # just grab one name column

        if not sel:
            return pd.DataFrame()

        df = self._q(f"SELECT {', '.join(sel)} FROM [{t}]")
        if 'abbreviation' in df.columns:
            df = df.rename(columns={'abbreviation': 'abbr'})
        if 'nickname' in df.columns:
            df = df.rename(columns={'nickname': 'abbr'})
        return df

    def _leagues_df(self) -> pd.DataFrame:
        t = self.find_table('leagues')
        if not t:
            return pd.DataFrame()
        cols = self.columns(t)
        sel = []
        id_col = 'league_id' if 'league_id' in cols else ('id' if 'id' in cols else None)
        if id_col:
            sel.append(f'[{id_col}] AS league_id')
        for col in ['name', 'abbr']:
            if col in cols:
                sel.append(f'[{col}] AS lg_name')
                break
        if not sel:
            return pd.DataFrame()
        df = self._q(f"SELECT {', '.join(sel)} FROM [{t}]")
        return df

    # ------------------------------------------------------------------
    # Batting stats
    # ------------------------------------------------------------------

    def _build_select(self, cols: list[str], needed: dict[str, list[str]]) -> tuple[list[str], dict[str, str]]:
        """Build SQL select list and alias->actual_col map from candidates."""
        select_parts = []
        col_map = {}
        for alias, candidates in needed.items():
            for c in candidates:
                if c in cols:
                    col_map[alias] = c
                    if c == alias:
                        select_parts.append(f'[{c}]')
                    else:
                        select_parts.append(f'[{c}] AS [{alias}]')
                    break
        return select_parts, col_map

    def get_batters(self, year: int = None, min_pa: int = 50) -> pd.DataFrame:
        t = self.find_table(
            'players_career_batting_stats',
            'player_batting_stats',
            'batting_stats',
            'players_batting',
        )
        if not t:
            return pd.DataFrame()

        cols = self.columns(t)
        needed = {
            'player_id': ['player_id', 'id'],
            'year':      ['year'],
            'team_id':   ['team_id'],
            'league_id': ['league_id'],
            'ab':        ['ab', 'at_bats'],
            'h':         ['h', 'hits'],
            'r':         ['r', 'runs'],
            'doubles':   ['2b', 'd2', 'doubles'],
            'triples':   ['3b', 'd3', 'triples'],
            'hr':        ['hr', 'home_runs'],
            'rbi':       ['rbi'],
            'bb':        ['bb', 'walks'],
            'k':         ['k', 'so', 'strikeouts'],
            'sb':        ['sb', 'stolen_bases'],
            'hbp':       ['hbp', 'hit_by_pitch'],
            'sf':        ['sf', 'sac_flies'],
        }
        select_parts, col_map = self._build_select(cols, needed)

        if 'player_id' not in col_map:
            return pd.DataFrame()

        params = []
        where = ''
        if year is not None and 'year' in col_map:
            where = 'WHERE year = ?'
            params.append(year)

        stats = self._q(f"SELECT {', '.join(select_parts)} FROM [{t}] {where}", params)
        if stats.empty:
            return stats

        # PA
        stats['PA'] = stats.get('ab', pd.Series(0, index=stats.index)).fillna(0)
        for c in ['bb', 'hbp', 'sf']:
            if c in stats.columns:
                stats['PA'] += stats[c].fillna(0)

        stats = stats[stats['PA'] >= min_pa].copy()
        if stats.empty:
            return stats

        ab  = stats['ab'].replace(0, pd.NA)
        h   = stats.get('h',       pd.Series(0, index=stats.index)).fillna(0)
        bb  = stats.get('bb',      pd.Series(0, index=stats.index)).fillna(0)
        hbp = stats.get('hbp',     pd.Series(0, index=stats.index)).fillna(0)
        sf  = stats.get('sf',      pd.Series(0, index=stats.index)).fillna(0)
        d2  = stats.get('doubles', pd.Series(0, index=stats.index)).fillna(0)
        d3  = stats.get('triples', pd.Series(0, index=stats.index)).fillna(0)
        hr  = stats.get('hr',      pd.Series(0, index=stats.index)).fillna(0)

        stats['AVG'] = (h / ab).round(3)
        stats['OBP'] = ((h + bb + hbp) / (ab + bb + hbp + sf)).round(3)
        stats['SLG'] = ((h - d2 - d3 - hr + 2*d2 + 3*d3 + 4*hr) / ab).round(3)
        stats['OPS'] = (stats['OBP'] + stats['SLG']).round(3)

        stats = self._merge_players(stats)
        stats = self._merge_teams(stats)

        out = ['Name', 'Age', 'POS', 'Team']
        stat_cols = ['PA', 'AB', 'R', 'H', 'HR', 'RBI', 'SB', 'BB', 'K', 'AVG', 'OBP', 'SLG', 'OPS']
        rename = {
            'age': 'Age', 'ab': 'AB', 'r': 'R', 'h': 'H',
            'hr': 'HR', 'rbi': 'RBI', 'sb': 'SB', 'bb': 'BB',
            'k': 'K', 'salary': 'Salary ($)',
        }
        stats = stats.rename(columns=rename)
        if 'Salary ($)' in stats.columns:
            stat_cols.append('Salary ($)')

        available = [c for c in out + stat_cols if c in stats.columns]
        return stats[available].sort_values('OPS', ascending=False).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Pitching stats
    # ------------------------------------------------------------------

    def get_pitchers(self, year: int = None, min_ip: float = 20.0) -> pd.DataFrame:
        t = self.find_table(
            'players_career_pitching_stats',
            'player_pitching_stats',
            'pitching_stats',
            'players_pitching',
        )
        if not t:
            return pd.DataFrame()

        cols = self.columns(t)
        needed = {
            'player_id': ['player_id', 'id'],
            'year':      ['year'],
            'team_id':   ['team_id'],
            'g':         ['g', 'games'],
            'gs':        ['gs', 'games_started'],
            'ip':        ['ip', 'innings_pitched'],
            'w':         ['w', 'wins'],
            'l':         ['l', 'losses'],
            'sv':        ['sv', 'saves'],
            'h':         ['h', 'hits'],
            'er':        ['er', 'earned_runs'],
            'hr':        ['hr', 'home_runs'],
            'bb':        ['bb', 'walks'],
            'k':         ['k', 'so', 'strikeouts'],
        }
        select_parts, col_map = self._build_select(cols, needed)

        if 'player_id' not in col_map:
            return pd.DataFrame()

        params = []
        where = ''
        if year is not None and 'year' in col_map:
            where = 'WHERE year = ?'
            params.append(year)

        stats = self._q(f"SELECT {', '.join(select_parts)} FROM [{t}] {where}", params)
        if stats.empty:
            return stats

        # OOTP stores IP as X.Y where .1 = 1/3, .2 = 2/3 inning
        if 'ip' in stats.columns:
            ip_raw = stats['ip'].fillna(0)
            ip_int = ip_raw.astype(int)
            ip_frac = (ip_raw - ip_int).round(1)
            stats['IP_true'] = ip_int + (ip_frac * 10 / 3)
            stats = stats[stats['IP_true'] >= min_ip].copy()

        if stats.empty:
            return stats

        ip  = stats['IP_true'].replace(0, pd.NA)
        er  = stats.get('er', pd.Series(0, index=stats.index)).fillna(0)
        bb  = stats.get('bb', pd.Series(0, index=stats.index)).fillna(0)
        h   = stats.get('h',  pd.Series(0, index=stats.index)).fillna(0)
        k   = stats.get('k',  pd.Series(0, index=stats.index)).fillna(0)
        hr  = stats.get('hr', pd.Series(0, index=stats.index)).fillna(0)

        stats['ERA']  = ((er * 9) / ip).round(2)
        stats['WHIP'] = ((bb + h) / ip).round(3)
        stats['K/9']  = ((k * 9) / ip).round(1)
        stats['BB/9'] = ((bb * 9) / ip).round(1)
        stats['HR/9'] = ((hr * 9) / ip).round(2)
        stats['FIP']  = ((13*hr + 3*bb - 2*k) / ip + 3.2).round(2)
        stats['IP']   = stats['ip'].round(1)

        stats = self._merge_players(stats)
        stats = self._merge_teams(stats)

        rename = {
            'age': 'Age', 'g': 'G', 'gs': 'GS', 'w': 'W', 'l': 'L',
            'sv': 'SV', 'h': 'H', 'hr': 'HR', 'bb': 'BB', 'k': 'K',
            'salary': 'Salary ($)',
        }
        stats = stats.rename(columns=rename)

        out = ['Name', 'Age', 'POS', 'Team']
        stat_cols = ['G', 'GS', 'IP', 'W', 'L', 'SV', 'H', 'HR', 'BB', 'K',
                     'ERA', 'WHIP', 'K/9', 'BB/9', 'FIP']
        if 'Salary ($)' in stats.columns:
            stat_cols.append('Salary ($)')

        available = [c for c in out + stat_cols if c in stats.columns]
        return stats[available].sort_values('ERA', ascending=True).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _merge_players(self, df: pd.DataFrame) -> pd.DataFrame:
        players = self._players_df()
        if players.empty or 'player_id' not in df.columns:
            return df
        keep = ['player_id'] + [c for c in ['first_name', 'last_name', 'age', 'POS', 'salary']
                                 if c in players.columns]
        df = df.merge(players[keep], on='player_id', how='left')
        if 'first_name' in df.columns and 'last_name' in df.columns:
            df['Name'] = df['first_name'].fillna('') + ' ' + df['last_name'].fillna('')
            df['Name'] = df['Name'].str.strip()
        return df

    def _merge_teams(self, df: pd.DataFrame) -> pd.DataFrame:
        teams = self._teams_df()
        if teams.empty or 'team_id' not in df.columns:
            return df
        keep = ['team_id'] + [c for c in ['abbr', 'name'] if c in teams.columns]
        df = df.merge(teams[keep[:2]], on='team_id', how='left')  # just id + first name col
        col = 'abbr' if 'abbr' in df.columns else ('name' if 'name' in df.columns else None)
        if col:
            df = df.rename(columns={col: 'Team'})
        return df

    def close(self):
        self.conn.close()
