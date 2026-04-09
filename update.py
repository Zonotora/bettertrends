#!/usr/bin/env python3
"""
update.py — single entry point for the github-trending pipeline.

Subcommands:
  fetch-trending   Scrape github.com/trending and store results in SQLite.
  fetch-upcoming   Fetch up-and-coming repos via GitHub search API.
  tag              Apply keyword tags to upcoming repos.
  render           Build index.html from the database.
  run-all          Run all of the above in order (default when no subcommand given).
"""

from __future__ import annotations

import argparse
import html as html_module
import json
import os
import re
import sqlite3
import subprocess
import time
import urllib.parse
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).parent
DB_PATH = ROOT / "github_trending.db"
OUTPUT_PATH = ROOT / "index.html"

# ══════════════════════════════════════════════════════════════════════════════
# fetch-trending
# ══════════════════════════════════════════════════════════════════════════════

TRENDING_BASE_URL = "https://github.com/trending"
ARTICLE_RE = re.compile(r"<article class=\"Box-row\">(.*?)</article>", re.DOTALL)
REPO_HREF_RE = re.compile(
    r'<h2 class="h3 lh-condensed">\s*<a [^>]*href="/([^"/]+/[^"/]+)"', re.DOTALL
)
DESCRIPTION_RE = re.compile(
    r'<p class="col-9 color-fg-muted my-1 tmp-pr-4">\s*(.*?)\s*</p>', re.DOTALL
)
LANGUAGE_RE = re.compile(r"<span itemprop=\"programmingLanguage\">(.*?)</span>")
STARS_LINK_RE = re.compile(
    r'href="/([^"/]+/[^"/]+)/stargazers"[^>]*>\s*.*?\s*([\d,]+)</a>', re.DOTALL
)
FORKS_LINK_RE = re.compile(
    r'href="/([^"/]+/[^"/]+)/forks"[^>]*>\s*.*?\s*([\d,]+)</a>', re.DOTALL
)
PERIOD_STARS_RE = re.compile(r"([\d,]+)\s+stars this (day|week|month)")


def _strip_tags(text: str) -> str:
    return re.sub(r"\s+", " ", html_module.unescape(re.sub(r"<[^>]+>", " ", text))).strip()


def _parse_int(value: str | None) -> int | None:
    return int(value.replace(",", "")) if value else None


def _fetch_trending_html(since: str) -> str:
    url = f"{TRENDING_BASE_URL}?since={since}"
    result = subprocess.run(
        ["curl", "-L", "--retry", "3", url],
        check=True, text=True, capture_output=True,
    )
    return result.stdout


def _parse_trending(document: str, limit: int) -> list[dict]:
    repos: list[dict] = []
    seen: set[str] = set()
    for rank, article in enumerate(ARTICLE_RE.findall(document), start=1):
        m = REPO_HREF_RE.search(article)
        if not m:
            continue
        full_name = m.group(1)
        if full_name in seen:
            continue
        seen.add(full_name)
        owner, repo_name = full_name.split("/", 1)
        dm = DESCRIPTION_RE.search(article)
        lm = LANGUAGE_RE.search(article)
        sm = STARS_LINK_RE.search(article)
        fm = FORKS_LINK_RE.search(article)
        pm = PERIOD_STARS_RE.search(article)
        repos.append({
            "owner": owner,
            "repo_name": repo_name,
            "full_name": full_name,
            "repo_url": f"https://github.com/{full_name}",
            "description": _strip_tags(dm.group(1)) if dm else None,
            "language": _strip_tags(lm.group(1)) if lm else None,
            "total_stars": _parse_int(sm.group(2)) if sm else None,
            "forks": _parse_int(fm.group(2)) if fm else None,
            "stars_gained": _parse_int(pm.group(1)) if pm else None,
            "stars_period_label": pm.group(2) if pm else None,
            "source_rank": rank,
        })
        if len(repos) >= limit:
            break
    return repos


def _init_trending_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS trending_runs (
            id INTEGER PRIMARY KEY,
            source TEXT NOT NULL,
            source_url TEXT NOT NULL,
            since_window TEXT NOT NULL,
            fetched_at_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS trending_repositories (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL REFERENCES trending_runs(id) ON DELETE CASCADE,
            source_rank INTEGER NOT NULL,
            owner TEXT NOT NULL,
            repo_name TEXT NOT NULL,
            full_name TEXT NOT NULL,
            repo_url TEXT NOT NULL,
            description TEXT,
            language TEXT,
            total_stars INTEGER,
            forks INTEGER,
            stars_gained INTEGER,
            stars_period_label TEXT,
            UNIQUE (run_id, full_name)
        );
        CREATE INDEX IF NOT EXISTS idx_trending_runs_window_fetched
        ON trending_runs(since_window, fetched_at_utc DESC);
        CREATE INDEX IF NOT EXISTS idx_trending_repositories_full_name
        ON trending_repositories(full_name);
    """)


def cmd_fetch_trending(args: argparse.Namespace) -> None:
    fetched_at = datetime.now(UTC).isoformat(timespec="seconds")
    document = _fetch_trending_html(args.since)
    repos = _parse_trending(document, args.limit)
    if not repos:
        raise RuntimeError("No repositories parsed from GitHub Trending page.")
    with sqlite3.connect(args.db) as conn:
        _init_trending_db(conn)
        source_url = f"{TRENDING_BASE_URL}?since={args.since}"
        run_id = conn.execute(
            "INSERT INTO trending_runs (source, source_url, since_window, fetched_at_utc) VALUES (?,?,?,?)",
            ("github_trending", source_url, args.since, fetched_at),
        ).lastrowid
        conn.executemany(
            """INSERT INTO trending_repositories
               (run_id,source_rank,owner,repo_name,full_name,repo_url,description,
                language,total_stars,forks,stars_gained,stars_period_label)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [(run_id, r["source_rank"], r["owner"], r["repo_name"], r["full_name"],
              r["repo_url"], r["description"], r["language"], r["total_stars"],
              r["forks"], r["stars_gained"], r["stars_period_label"]) for r in repos],
        )
        conn.commit()
    print(f"fetch-trending: stored {len(repos)} repos (run_id={run_id}, since={args.since}).")


# ══════════════════════════════════════════════════════════════════════════════
# fetch-upcoming
# ══════════════════════════════════════════════════════════════════════════════

API_BASE_URL = "https://api.github.com/search/repositories"


@dataclass(frozen=True)
class SearchWindow:
    start: date
    end: date

    @property
    def label(self) -> str:
        return f"{self.start.isoformat()}..{self.end.isoformat()}"


def _month_windows(months_back: int, today: date) -> list[SearchWindow]:
    windows: list[SearchWindow] = []
    current_start = today.replace(day=1)
    for offset in range(months_back):
        mi = current_start.month - offset
        yr = current_start.year
        while mi <= 0:
            mi += 12
            yr -= 1
        start = date(yr, mi, 1)
        if offset == 0:
            end = today
        else:
            nxt = date(yr, mi, 1)
            if nxt.month == 12:
                end = date(nxt.year + 1, 1, 1) - timedelta(days=1)
            else:
                end = date(nxt.year, nxt.month + 1, 1) - timedelta(days=1)
        windows.append(SearchWindow(start=start, end=end))
    return windows


def _fetch_api_page(window: SearchWindow, page: int, per_page: int, min_stars: int,
                    cache_dir: Path, cache_only: bool) -> tuple[dict, dict[str, str]]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    base = f"{window.start.isoformat()}_{window.end.isoformat()}_page{page}"
    headers_path = cache_dir / f"{base}.headers.txt"
    body_path = cache_dir / f"{base}.json"

    if cache_only:
        if not body_path.exists():
            raise FileNotFoundError(f"Missing cache: {body_path}")
        return json.loads(body_path.read_text(encoding="utf-8")), _parse_response_headers(headers_path)

    query = f"is:public fork:false archived:false created:{window.label} stars:>={min_stars}"
    url = f"{API_BASE_URL}?q={urllib.parse.quote(query)}&sort=stars&order=desc&per_page={per_page}&page={page}"
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    auth = ["-H", f"Authorization: Bearer {token}"] if token else []
    cmd = ["curl", "-L", "--retry", "3", "--retry-all-errors",
           *auth, "-H", "Accept: application/vnd.github+json",
           "-D", str(headers_path), "-o", str(body_path), url]

    last_err = None
    for attempt in range(5):
        try:
            subprocess.run(cmd, check=True, text=True, capture_output=True)
            last_err = None
            break
        except subprocess.CalledProcessError as e:
            last_err = e
            if body_path.exists() and body_path.stat().st_size > 0:
                break
            time.sleep(2 ** attempt)
    if last_err and not body_path.exists():
        raise last_err

    headers = _parse_response_headers(headers_path)
    _maybe_wait_rate_limit(headers)
    return json.loads(body_path.read_text(encoding="utf-8")), headers


def _parse_response_headers(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            result[k.strip().lower()] = v.strip()
    return result


def _maybe_wait_rate_limit(headers: dict[str, str]) -> None:
    remaining = headers.get("x-ratelimit-remaining")
    reset = headers.get("x-ratelimit-reset")
    if remaining == "0" and reset:
        secs = max(0, int(reset) - int(time.time())) + 1
        time.sleep(secs)


def _init_upcoming_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS upcoming_runs (
            id INTEGER PRIMARY KEY,
            source TEXT NOT NULL,
            query_strategy TEXT NOT NULL,
            target_count INTEGER NOT NULL,
            months_back INTEGER NOT NULL,
            pages_per_window INTEGER NOT NULL,
            per_page INTEGER NOT NULL,
            min_stars INTEGER NOT NULL,
            fetched_at_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS upcoming_repositories (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL REFERENCES upcoming_runs(id) ON DELETE CASCADE,
            github_id INTEGER NOT NULL,
            node_id TEXT,
            owner_login TEXT NOT NULL,
            repo_name TEXT NOT NULL,
            full_name TEXT NOT NULL,
            html_url TEXT NOT NULL,
            description TEXT,
            language TEXT,
            stargazers_count INTEGER NOT NULL,
            forks_count INTEGER NOT NULL,
            watchers_count INTEGER,
            open_issues_count INTEGER,
            size_kb INTEGER,
            created_at TEXT,
            updated_at TEXT,
            pushed_at TEXT,
            default_branch TEXT,
            license_spdx_id TEXT,
            topics_json TEXT,
            source_window_start TEXT NOT NULL,
            source_window_end TEXT NOT NULL,
            source_page INTEGER NOT NULL,
            source_rank_in_page INTEGER NOT NULL,
            overall_rank INTEGER NOT NULL,
            UNIQUE (run_id, github_id)
        );
        CREATE INDEX IF NOT EXISTS idx_upcoming_runs_fetched
        ON upcoming_runs(fetched_at_utc DESC);
        CREATE INDEX IF NOT EXISTS idx_upcoming_repositories_stars
        ON upcoming_repositories(run_id, stargazers_count DESC);
        CREATE INDEX IF NOT EXISTS idx_upcoming_repositories_full_name
        ON upcoming_repositories(full_name);
        CREATE TABLE IF NOT EXISTS upcoming_repository_tags (
            id INTEGER PRIMARY KEY,
            repository_id INTEGER NOT NULL REFERENCES upcoming_repositories(id) ON DELETE CASCADE,
            tag TEXT NOT NULL,
            matched_terms_json TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            UNIQUE (repository_id, tag)
        );
        CREATE INDEX IF NOT EXISTS idx_upcoming_repository_tags_tag
        ON upcoming_repository_tags(tag);
    """)


def cmd_fetch_upcoming(args: argparse.Namespace) -> None:
    fetched_at = datetime.now(UTC).isoformat(timespec="seconds")
    today = datetime.now(UTC).date()
    windows = _month_windows(args.months_back, today)

    unique: dict[int, dict] = {}
    for window in windows:
        for page in range(1, args.pages_per_window + 1):
            payload, _ = _fetch_api_page(
                window, page, args.per_page, args.min_stars,
                args.cache_dir, args.cache_only,
            )
            for rank_in_page, repo in enumerate(payload.get("items", []), start=1):
                gid = int(repo["id"])
                candidate = {
                    "github_id": gid,
                    "node_id": repo.get("node_id"),
                    "owner_login": repo["owner"]["login"],
                    "repo_name": repo["name"],
                    "full_name": repo["full_name"],
                    "html_url": repo["html_url"],
                    "description": repo.get("description"),
                    "language": repo.get("language"),
                    "stargazers_count": int(repo["stargazers_count"]),
                    "forks_count": int(repo["forks_count"]),
                    "watchers_count": int(repo.get("watchers_count") or 0),
                    "open_issues_count": int(repo.get("open_issues_count") or 0),
                    "size_kb": int(repo.get("size") or 0),
                    "created_at": repo.get("created_at"),
                    "updated_at": repo.get("updated_at"),
                    "pushed_at": repo.get("pushed_at"),
                    "default_branch": repo.get("default_branch"),
                    "license_spdx_id": (repo.get("license") or {}).get("spdx_id"),
                    "topics_json": json.dumps(repo.get("topics", []), separators=(",", ":")),
                    "source_window_start": window.start.isoformat(),
                    "source_window_end": window.end.isoformat(),
                    "source_page": page,
                    "source_rank_in_page": rank_in_page,
                }
                existing = unique.get(gid)
                if existing is None or candidate["stargazers_count"] > existing["stargazers_count"]:
                    unique[gid] = candidate

    ordered = sorted(
        unique.values(),
        key=lambda r: (-r["stargazers_count"], r["created_at"] or "", r["full_name"]),
    )
    for rank, repo in enumerate(ordered, start=1):
        repo["overall_rank"] = rank

    repos = ordered[: args.target_count]
    if len(repos) < args.target_count:
        raise RuntimeError(f"Only collected {len(repos)} repos, below target {args.target_count}.")

    with sqlite3.connect(args.db) as conn:
        _init_upcoming_db(conn)
        run_id = conn.execute(
            """INSERT INTO upcoming_runs
               (source,query_strategy,target_count,months_back,pages_per_window,per_page,min_stars,fetched_at_utc)
               VALUES (?,?,?,?,?,?,?,?)""",
            ("github_search_api", "recent_monthly_windows_sorted_by_stars",
             args.target_count, args.months_back, args.pages_per_window,
             args.per_page, args.min_stars, fetched_at),
        ).lastrowid
        conn.executemany(
            """INSERT INTO upcoming_repositories
               (run_id,github_id,node_id,owner_login,repo_name,full_name,html_url,description,
                language,stargazers_count,forks_count,watchers_count,open_issues_count,size_kb,
                created_at,updated_at,pushed_at,default_branch,license_spdx_id,topics_json,
                source_window_start,source_window_end,source_page,source_rank_in_page,overall_rank)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [(run_id, r["github_id"], r["node_id"], r["owner_login"], r["repo_name"],
              r["full_name"], r["html_url"], r["description"], r["language"],
              r["stargazers_count"], r["forks_count"], r["watchers_count"],
              r["open_issues_count"], r["size_kb"], r["created_at"], r["updated_at"],
              r["pushed_at"], r["default_branch"], r["license_spdx_id"], r["topics_json"],
              r["source_window_start"], r["source_window_end"], r["source_page"],
              r["source_rank_in_page"], r["overall_rank"]) for r in repos],
        )
        conn.commit()
    print(f"fetch-upcoming: stored {len(repos)} repos (run_id={run_id}).")


# ══════════════════════════════════════════════════════════════════════════════
# tag
# ══════════════════════════════════════════════════════════════════════════════

_TAGS: dict[str, list[str]] = {
    "ai": [
        r"\bai\b", r"artificial intelligence", r"\bllm[s]?\b", r"\bml\b", r"machine learning",
        r"deep learning", r"\bgenai\b", r"generative ai", r"multimodal", r"\binference\b",
        r"\bembedding[s]?\b", r"\bprompt[s]?\b", r"\bopenai\b", r"\banthropic\b", r"\bclaude\b",
        r"\bgpt\b", r"\bgemini\b", r"\bollama\b", r"\bcodex\b", r"\bcopilot\b",
        r"\bdiffusion\b", r"\brag\b",
    ],
    "agent": [
        r"\bagent\b", r"\bagents\b", r"\bagentic\b", r"multi-agent", r"multi agent",
        r"autonomous agent", r"agent framework", r"agentic framework", r"agent harness",
        r"agent skills",
    ],
    "devtools": [
        r"developer", r"devtool", r"\bcli\b", r"\bplugin\b", r"\btoolkit\b", r"\bframework\b",
        r"\blibrary\b", r"\bsdk\b", r"\beditor\b", r"\bide\b", r"\bcodex\b", r"\bcopilot\b",
    ],
    "frontend": [
        r"\bfrontend\b", r"\bui\b", r"\bux\b", r"\breact\b", r"\btailwind\b",
        r"\bcomponent\b", r"web app", r"\buikit\b",
    ],
    "data": [
        r"\bresearch\b", r"\bbenchmark\b", r"\bdataset\b", r"\banalytics\b", r"\banalysis\b",
        r"\bforecast\b", r"\bprediction\b", r"knowledge graph", r"\bevaluation\b",
    ],
    "infra": [
        r"\bdocker\b", r"\bkubernetes\b", r"\bterraform\b", r"\binfra\b", r"\binfrastructure\b",
        r"\bdeployment\b", r"\bdevops\b", r"\bproxy\b", r"\bcloud\b", r"\bmonitoring\b",
    ],
    "security": [
        r"\bsecurity\b", r"\bprivacy\b", r"\bauth\b", r"\bauthentication\b", r"\bencryption\b",
        r"\bvulnerability\b", r"\bosint\b", r"\bthreat\b", r"\bcompliance\b",
    ],
    "productivity": [
        r"\bproductivity\b", r"\bmemory\b", r"\bnotes?\b", r"\bknowledge\b", r"\bwiki\b",
        r"\bdocumentation\b", r"\bdocs\b", r"organi[sz]e", r"\btask\b", r"project management",
    ],
    "media": [
        r"\bvideo\b", r"\baudio\b", r"\bimage\b", r"\bvoice\b", r"\btts\b", r"\bmusic\b",
        r"\bscreen\b", r"\bcontent\b", r"\bmedia\b",
    ],
    "desktop": [
        r"\bmacos\b", r"\bwindows\b", r"\blinux\b", r"\bdesktop\b", r"\bmobile\b",
        r"\bandroid\b", r"\bios\b", r"\biphone\b", r"\bipad\b", r"operating system",
    ],
}
_TAG_RE = {tag: [re.compile(p, re.IGNORECASE) for p in patterns]
           for tag, patterns in _TAGS.items()}


def _matched_terms(text: str, regexes: list[re.Pattern]) -> list[str]:
    seen: list[str] = []
    for rx in regexes:
        for item in rx.findall(text):
            tok = (" ".join(item) if isinstance(item, tuple) else str(item)).strip().lower()
            if tok and tok not in seen:
                seen.append(tok)
    return seen


def cmd_tag(args: argparse.Namespace) -> None:
    now = datetime.now(UTC).isoformat(timespec="seconds")
    with sqlite3.connect(args.db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, full_name, description, language, topics_json FROM upcoming_repositories"
            " WHERE run_id = (SELECT MAX(id) FROM upcoming_runs) ORDER BY overall_rank"
        ).fetchall()
        tag_rows: list[tuple] = []
        for row in rows:
            text = "\n".join([row["full_name"] or "", row["description"] or "",
                              row["language"] or "", row["topics_json"] or ""])
            for tag, regexes in _TAG_RE.items():
                matches = _matched_terms(text, regexes)
                if matches:
                    tag_rows.append((int(row["id"]), tag, json.dumps(matches, separators=(",", ":")), now))
        conn.execute("DELETE FROM upcoming_repository_tags")
        conn.executemany(
            "INSERT INTO upcoming_repository_tags (repository_id,tag,matched_terms_json,created_at_utc) VALUES (?,?,?,?)",
            tag_rows,
        )
        conn.commit()
    print(f"tag: {len(tag_rows)} tag assignments across {len(rows)} repos.")


# ══════════════════════════════════════════════════════════════════════════════
# render
# ══════════════════════════════════════════════════════════════════════════════

def _query_repos(conn: sqlite3.Connection) -> list[dict]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT base.id, base.overall_rank, base.full_name, base.html_url,
               base.stargazers_count, base.language, base.description,
               base.created_date, base.owner_login,
               COALESCE(t.tags_json, '[]') AS tags_json
        FROM (
            SELECT id, overall_rank, full_name, html_url, stargazers_count,
                   language, description, owner_login,
                   substr(created_at, 1, 10) AS created_date
            FROM upcoming_repositories
            WHERE run_id = (SELECT MAX(id) FROM upcoming_runs)
        ) base
        LEFT JOIN (
            SELECT repository_id, json_group_array(tag) AS tags_json
            FROM (SELECT repository_id, tag FROM upcoming_repository_tags ORDER BY tag)
            GROUP BY repository_id
        ) t ON t.repository_id = base.id
        ORDER BY base.overall_rank
    """).fetchall()
    result = [dict(r) for r in rows]
    for r in result:
        r["tags"] = json.loads(r.pop("tags_json"))
    return result


def _query_summary(conn: sqlite3.Connection) -> dict:
    conn.row_factory = sqlite3.Row
    return dict(conn.execute("""
        SELECT COUNT(*) AS repo_count, MAX(stargazers_count) AS max_stars,
               MIN(stargazers_count) AS min_stars,
               MIN(substr(created_at,1,10)) AS oldest_created_at,
               MAX(substr(created_at,1,10)) AS newest_created_at
        FROM upcoming_repositories
        WHERE run_id = (SELECT MAX(id) FROM upcoming_runs)
    """).fetchone())


def _query_tag_counts(conn: sqlite3.Connection) -> dict[str, int]:
    conn.row_factory = sqlite3.Row
    return {r["tag"]: r["repo_count"] for r in conn.execute(
        """SELECT tag, COUNT(*) AS repo_count FROM upcoming_repository_tags
           WHERE repository_id IN (SELECT id FROM upcoming_repositories WHERE run_id = (SELECT MAX(id) FROM upcoming_runs))
           GROUP BY tag ORDER BY repo_count DESC, tag"""
    ).fetchall()}



def _percentile(sv: list[int], p: float) -> int:
    if not sv:
        return 0
    idx = (len(sv) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(sv) - 1)
    return round(sv[lo] * (1 - (idx - lo)) + sv[hi] * (idx - lo))


def _build_stats(repos: list[dict]) -> dict:
    stars: list[int] = []
    languages: Counter = Counter()
    months: Counter = Counter()
    owners: Counter = Counter()
    tag_counts: Counter = Counter()
    tag_star_sums: dict = defaultdict(int)

    for repo in repos:
        tags = repo["tags"]
        s = int(repo["stargazers_count"])
        stars.append(s)
        languages[repo["language"] or "Unknown"] += 1
        months[(repo["created_date"] or "")[:7] or "Unknown"] += 1
        owners[repo.get("owner_login") or "Unknown"] += 1
        for tag in tags:
            tag_counts[tag] += 1
            tag_star_sums[tag] += s

    stars.sort()
    tag_avg = {t: round(tag_star_sums[t] / c) for t, c in tag_counts.items()}
    return {
        "repo_count": len(repos),
        "avg_stars": round(sum(stars) / len(stars)) if stars else 0,
        "median_stars": _percentile(stars, 0.50),
        "p75_stars": _percentile(stars, 0.75),
        "p90_stars": _percentile(stars, 0.90),
        "p99_stars": _percentile(stars, 0.99),
        "max_stars": stars[-1] if stars else 0,
        "top_tags": [{"label": t, "value": c, "avg_stars": tag_avg[t]}
                     for t, c in tag_counts.most_common(10)],
        "top_languages": [{"label": l, "value": c} for l, c in languages.most_common(8)],
        "month_series": [{"label": m, "value": months[m]} for m in sorted(months)],
        "top_owners": [{"label": o, "value": c} for o, c in owners.most_common(8)],
    }


def _fmt(n: int) -> str:
    return f"{n:,}"


def _bar_chart(title: str, data: list[dict], color: str) -> str:
    if not data:
        return ""
    mx = max(item["value"] for item in data) or 1
    rows_html = []
    for item in data:
        w = (item["value"] / mx) * 100
        avg = f"avg ★{_fmt(item['avg_stars'])}" if "avg_stars" in item else ""
        rows_html.append(
            f'<div class="b-row">'
            f'<span class="b-label">{item["label"]}</span>'
            f'<div class="b-track"><div class="b-fill" style="width:{w:.1f}%;background:{color}"></div></div>'
            f'<span class="b-val">{_fmt(item["value"])}</span>'
            + (f'<span class="b-meta">{avg}</span>' if avg else "")
            + "</div>"
        )
    return (
        f'<div class="chart-panel">'
        f'<div class="panel-title">{title}</div>'
        f'<div class="bars">{"".join(rows_html)}</div>'
        f"</div>"
    )


def _build_html(repos: list[dict], summary: dict, tag_counts: dict[str, int], stats: dict) -> str:
    generated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

    payload = json.dumps({
        "repos": [{
            "n": r["full_name"],
            "r": r["overall_rank"],
            "s": r["stargazers_count"],
            "l": r["language"] or "Unknown",
            "d": r["description"] or "",
            "c": r["created_date"] or "",
            "t": r["tags"],
        } for r in repos],
        "summary": summary,
        "tag_counts": tag_counts,
        "stats": {
            "top_tags": stats["top_tags"],
        },
        "generated_at": generated_at,
    }, separators=(",", ":"))

    tag_buttons = "\n".join(
        f'          <button class="fchip tag-fchip" data-tag="{tag}">'
        f'{tag} <span class="fchip-n">{count}</span></button>'
        for tag, count in tag_counts.items()
    )

    charts_html = (
        _bar_chart("Top tags by repo count", stats["top_tags"], "#f97316")
        + _bar_chart("Top languages", stats["top_languages"], "#34d399")
        + _bar_chart("Created by month", stats["month_series"], "#a78bfa")
        + _bar_chart("Top owners", stats["top_owners"], "#f472b6")
    )

    findings_html = "".join(f"<li>{line}</li>" for line in [
        f"{_fmt(stats['repo_count'])} repositories indexed.",
        f"Median stars: {_fmt(stats['median_stars'])}  |  P90: {_fmt(stats['p90_stars'])}  |  P99: {_fmt(stats['p99_stars'])}",
        f"Average stars: {_fmt(stats['avg_stars'])}",
    ])

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>bettertrends</title>
  <style>
    :root {{
      --bg:      #0a0a0a;
      --surface: #181818;
      --border:  #333333;
      --text:    #e8e8e8;
      --muted:   #888888;
      --accent:  #f97316;
      --font:    'JetBrains Mono', 'Fira Code', 'Cascadia Code', 'Consolas', monospace;
    }}
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    html {{ font-size: 14px; }}
    body {{ background: var(--bg); color: var(--text); font-family: var(--font); line-height: 1.5; }}
    .page {{ max-width: 1600px; margin: 0 auto; padding: 24px 20px 48px; }}

    /* header */
    .header {{ border-bottom: 1px solid var(--border); padding-bottom: 16px; margin-bottom: 24px; }}
    .header-top {{ display: flex; align-items: baseline; gap: 16px; flex-wrap: wrap; flex: 1; }}
    .header-row {{ display: flex; align-items: center; justify-content: space-between; }}
    .gh-link {{ font-size: 0.78rem; color: var(--muted); text-decoration: none; white-space: nowrap; }}
    .gh-link:hover {{ color: var(--text); }}
    .site-title {{ font-size: 1.25rem; font-weight: 700; color: var(--accent); letter-spacing: -0.02em; }}
    .site-subtitle {{ color: var(--muted); font-size: 0.85rem; }}
    .header-meta {{ margin-top: 8px; display: flex; gap: 24px; flex-wrap: wrap; font-size: 0.8rem; color: var(--muted); }}
    .header-meta span {{ white-space: nowrap; }}
    .meta-val {{ color: var(--text); }}

    /* tabs */
    .tabs {{ display: flex; gap: 2px; margin-bottom: 20px; border-bottom: 1px solid var(--border); }}
    .tab {{ padding: 8px 18px; font-family: var(--font); font-size: 0.85rem; color: var(--muted);
            background: none; border: none; border-bottom: 2px solid transparent;
            cursor: pointer; margin-bottom: -1px; }}
    .tab:hover {{ color: var(--text); }}
    .tab.active {{ color: var(--accent); border-bottom-color: var(--accent); }}

    /* panels */
    .panel {{ display: none; }}
    .panel.active {{ display: block; }}

    /* toolbar */
    .toolbar {{ display: flex; gap: 14px; flex-wrap: wrap; align-items: flex-end;
                margin-bottom: 16px; padding-bottom: 14px; border-bottom: 1px solid var(--border); }}
    .search-wrap {{ display: flex; flex-direction: column; gap: 4px; }}
    .toolbar-label {{ font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }}
    .search-input {{ padding: 7px 10px; background: var(--surface); border: 1px solid var(--border);
                     color: var(--text); font-family: var(--font); font-size: 0.85rem;
                     border-radius: 4px; width: 280px; outline: none; }}
    .search-input:focus {{ border-color: var(--accent); }}
    .fchip-group {{ display: flex; gap: 6px; flex-wrap: wrap; align-items: center; }}
    .fchip {{ padding: 4px 9px; font-family: var(--font); font-size: 0.75rem; background: var(--surface);
              border: 1px solid var(--border); color: var(--muted); cursor: pointer;
              border-radius: 3px; white-space: nowrap; }}
    .fchip:hover {{ border-color: var(--accent); color: var(--text); }}
    .fchip.active {{ background: rgba(249,115,22,0.12); border-color: var(--accent); color: var(--accent); }}
    .fchip-n {{ opacity: 0.65; }}
    .visible-count {{ font-size: 0.8rem; color: var(--muted); white-space: nowrap; align-self: center; }}
    .visible-count span {{ color: var(--text); }}

    /* table */
    .table-wrap {{ overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
    thead th {{ padding: 8px 12px; text-align: left; font-size: 0.72rem; text-transform: uppercase;
                letter-spacing: 0.07em; color: var(--muted); border-bottom: 1px solid var(--border);
                white-space: nowrap; }}
    tbody tr {{ border-bottom: 1px solid var(--border); }}
    tbody tr:hover {{ background: var(--surface); }}
    tbody td {{ padding: 9px 12px; vertical-align: middle; }}
    .td-rank {{ color: var(--muted); font-size: 0.75rem; width: 48px; }}
    .repo-link {{ color: var(--text); text-decoration: none; font-weight: 600; }}
    .repo-link:hover {{ color: var(--accent); }}
    .repo-desc {{ font-size: 0.78rem; color: var(--muted); margin-top: 3px; max-width: 520px;
                  white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .td-lang {{ color: var(--muted); white-space: nowrap; }}
    .td-created {{ color: var(--muted); white-space: nowrap; font-size: 0.8rem; }}
    .inline-tags {{ color: var(--muted); font-size: 0.78rem; }}
    .inline-tags .tag {{ color: var(--accent); }}
    .stars-bar-wrap {{ min-width: 160px; }}
    .stars-bar-track {{ height: 6px; background: var(--border); overflow: hidden; margin-bottom: 4px; }}
    .stars-bar-fill {{ height: 100%; background: var(--accent); min-width: 2px; }}
    .stars-val {{ font-variant-numeric: tabular-nums; color: var(--text); font-size: 0.82rem; }}
    .hidden {{ display: none !important; }}

    /* stats */
    .stats-grid {{ display: grid; grid-template-columns: repeat(6,minmax(0,1fr)); gap: 1px;
                   background: var(--border); border: 1px solid var(--border);
                   border-radius: 4px; overflow: hidden; margin-bottom: 24px; }}
    .stat-cell {{ background: var(--surface); padding: 14px 16px; }}
    .stat-label {{ font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.07em;
                   color: var(--muted); margin-bottom: 6px; }}
    .stat-value {{ font-size: 1.1rem; font-weight: 700; color: var(--text); font-variant-numeric: tabular-nums; }}
    .charts-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px; }}
    .chart-panel {{ background: var(--surface); border: 1px solid var(--border);
                    border-radius: 4px; padding: 16px 18px; }}
    .panel-title {{ font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.07em;
                    color: var(--muted); margin-bottom: 14px; }}
    .bars {{ display: grid; gap: 9px; }}
    .b-row {{ display: grid; grid-template-columns: 180px 1fr 60px auto; gap: 10px;
              align-items: center; font-size: 0.82rem; }}
    .b-label {{ color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .b-track {{ height: 8px; background: var(--border); overflow: hidden; }}
    .b-fill {{ height: 100%; }}
    .b-val {{ color: var(--text); font-variant-numeric: tabular-nums; text-align: right; }}
    .b-meta {{ color: var(--muted); font-size: 0.72rem; white-space: nowrap; }}
    .scatter-panel {{ background: var(--surface); border: 1px solid var(--border);
                      border-radius: 4px; padding: 16px 18px; margin-bottom: 24px; }}
    .scatter-controls {{ display: flex; gap: 14px; flex-wrap: wrap; margin-bottom: 14px; }}
    .ctrl-wrap {{ display: flex; flex-direction: column; gap: 4px; }}
    .ctrl-select {{ padding: 6px 9px; background: var(--bg); border: 1px solid var(--border);
                    color: var(--text); font-family: var(--font); font-size: 0.82rem; border-radius: 3px; }}
    .scatter-wrap {{ overflow-x: auto; border: 1px solid var(--border); border-radius: 3px; }}
    svg text {{ font-family: var(--font); }}
    .findings-panel {{ background: var(--surface); border: 1px solid var(--border);
                       border-radius: 4px; padding: 16px 18px; }}
    .findings-panel ul {{ padding-left: 16px; color: var(--muted); line-height: 1.9; font-size: 0.85rem; }}

    /* footer */
    .footer {{ margin-top: 32px; padding-top: 14px; border-top: 1px solid var(--border);
               font-size: 0.75rem; color: var(--muted); }}

    @media (max-width: 1100px) {{
      .stats-grid {{ grid-template-columns: repeat(3,1fr); }}
      .charts-grid {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 700px) {{
      .stats-grid {{ grid-template-columns: repeat(2,1fr); }}
      .b-row {{ grid-template-columns: 1fr 1fr; }}
      .search-input {{ width: 100%; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <header class="header">
      <div class="header-row">
        <div class="header-top">
          <span class="site-title">bettertrends</span>
          <span class="site-subtitle">trending github repositories</span>
        </div>
        <a class="gh-link" href="https://github.com/Zonotora/bettertrends" target="_blank" rel="noreferrer">github.com/Zonotora/bettertrends</a>
      </div>
      <div class="header-meta">
        <span><span class="meta-val">{_fmt(summary["repo_count"])}</span> repos</span>
        <span>stars <span class="meta-val">{_fmt(summary["min_stars"])}</span> – <span class="meta-val">{_fmt(summary["max_stars"])}</span></span>
        <span>created <span class="meta-val">{summary["oldest_created_at"]}</span> – <span class="meta-val">{summary["newest_created_at"]}</span></span>
        <span>generated <span class="meta-val">{generated_at}</span></span>
      </div>
    </header>

    <nav class="tabs">
      <button class="tab active" data-panel="repos">repos</button>
      <button class="tab" data-panel="stats">stats</button>
    </nav>

    <div class="panel active" id="panel-repos">
      <div class="toolbar">
        <div class="search-wrap">
          <div class="toolbar-label">Search</div>
          <input class="search-input" id="search" type="search" placeholder="repo, owner, language, tag…">
        </div>
        <div>
          <div class="toolbar-label">Tags</div>
          <div class="fchip-group">
{tag_buttons}
          </div>
        </div>
        <div>
          <div class="toolbar-label">Language</div>
          <select class="ctrl-select" id="filter-lang">
            <option value="">all languages</option>
          </select>
        </div>
        <div class="visible-count">showing <span id="visible-count">{_fmt(summary["repo_count"])}</span> repos</div>
      </div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>#</th><th>repository</th><th>language</th><th>created</th><th>tags</th><th>stars</th></tr></thead>
          <tbody id="repo-body"></tbody>
        </table>
      </div>
    </div>

    <div class="panel" id="panel-stats">
      <div class="stats-grid">
        <div class="stat-cell"><div class="stat-label">Repositories</div><div class="stat-value">{_fmt(stats["repo_count"])}</div></div>
        <div class="stat-cell"><div class="stat-label">Avg stars</div><div class="stat-value">{_fmt(stats["avg_stars"])}</div></div>
        <div class="stat-cell"><div class="stat-label">Median stars</div><div class="stat-value">{_fmt(stats["median_stars"])}</div></div>
        <div class="stat-cell"><div class="stat-label">P75</div><div class="stat-value">{_fmt(stats["p75_stars"])}</div></div>
        <div class="stat-cell"><div class="stat-label">P90</div><div class="stat-value">{_fmt(stats["p90_stars"])}</div></div>
        <div class="stat-cell"><div class="stat-label">P99</div><div class="stat-value">{_fmt(stats["p99_stars"])}</div></div>
      </div>
      <div class="charts-grid">
{charts_html}
      </div>
      <div class="scatter-panel">
        <div class="panel-title">Stars vs. creation time</div>
        <div class="scatter-controls">
          <div class="ctrl-wrap">
            <div class="toolbar-label">Tag</div>
            <select class="ctrl-select" id="sc-tag"><option value="">all tags</option></select>
          </div>
          <div class="ctrl-wrap">
            <div class="toolbar-label">Language</div>
            <select class="ctrl-select" id="sc-lang"><option value="">all languages</option></select>
          </div>
        </div>
        <div class="scatter-wrap">
          <svg id="scatter" width="1400" height="380" viewBox="0 0 1400 380"></svg>
        </div>
      </div>
      <div class="findings-panel">
        <div class="panel-title">Summary</div>
        <ul>{findings_html}</ul>
      </div>
    </div>

    <footer class="footer">source: <code>github_trending.db</code> · generated {generated_at}</footer>
  </div>

  <script id="payload" type="application/json">{payload}</script>
  <script>
  (function () {{
    const P = JSON.parse(document.getElementById('payload').textContent);
    const repos = P.repos.map(r => ({{
      full_name: r.n,
      html_url: 'https://github.com/' + r.n,
      overall_rank: r.r,
      stargazers_count: r.s,
      language: r.l,
      description: r.d,
      created_date: r.c,
      created_month: r.c.slice(0, 7),
      tags: r.t,
    }}));
    const maxStars = P.summary.max_stars;
    const topTags = P.stats.top_tags;

    document.querySelectorAll('.tab').forEach(btn => {{
      btn.addEventListener('click', () => {{
        document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById('panel-' + btn.dataset.panel).classList.add('active');
        if (btn.dataset.panel === 'stats') renderScatter();
      }});
    }});

    const tbody = document.getElementById('repo-body');
    const searchInput = document.getElementById('search');
    const visibleCount = document.getElementById('visible-count');
    const filterLang = document.getElementById('filter-lang');
    const activeTags = new Set();

    const allLangs = [...new Set(repos.map(r => r.language).filter(Boolean))].sort();
    allLangs.forEach(lang => {{
      const opt = document.createElement('option');
      opt.value = lang; opt.textContent = lang;
      filterLang.appendChild(opt);
    }});

    function fmtInt(n) {{ return new Intl.NumberFormat('en-US').format(n); }}

    function buildRow(repo) {{
      const tr = document.createElement('tr');
      tr.dataset.search = (repo.full_name + ' ' + repo.language + ' ' + repo.tags.join(' ')).toLowerCase();
      tr.dataset.tags = repo.tags.join(',');
      tr.dataset.lang = repo.language;
      const width = Math.max(2, (repo.stargazers_count / maxStars) * 100);
      const tagStr = repo.tags.length
        ? repo.tags.map(t => `<span class="tag">${{t}}</span>`).join(' ')
        : '<span style="color:var(--border)">—</span>';
      tr.innerHTML = `
        <td class="td-rank">${{repo.overall_rank}}</td>
        <td>
          <a class="repo-link" href="${{repo.html_url}}" target="_blank" rel="noreferrer">${{repo.full_name}}</a>
          ${{repo.description ? `<div class="repo-desc">${{repo.description}}</div>` : ''}}
        </td>
        <td class="td-lang">${{repo.language}}</td>
        <td class="td-created">${{repo.created_date}}</td>
        <td class="inline-tags">${{tagStr}}</td>
        <td>
          <div class="stars-bar-wrap">
            <div class="stars-bar-track"><div class="stars-bar-fill" style="width:${{width.toFixed(1)}}%"></div></div>
            <div class="stars-val">${{fmtInt(repo.stargazers_count)}}</div>
          </div>
        </td>`;
      return tr;
    }}

    const rows = repos.map(buildRow);
    rows.forEach(r => tbody.appendChild(r));

    function applyFilter() {{
      const q = searchInput.value.trim().toLowerCase();
      const lang = filterLang.value;
      let n = 0;
      rows.forEach(row => {{
        const rowTags = row.dataset.tags ? row.dataset.tags.split(',').filter(Boolean) : [];
        const ok = (!q || row.dataset.search.includes(q))
          && (activeTags.size === 0 || [...activeTags].every(t => rowTags.includes(t)))
          && (!lang || row.dataset.lang === lang);
        row.classList.toggle('hidden', !ok);
        if (ok) n++;
      }});
      visibleCount.textContent = fmtInt(n);
    }}

    function toggleTag(tag, btn) {{
      activeTags.has(tag) ? activeTags.delete(tag) : activeTags.add(tag);
      btn.classList.toggle('active', activeTags.has(tag));
      applyFilter();
    }}

    searchInput.addEventListener('input', applyFilter);
    filterLang.addEventListener('change', applyFilter);
    document.querySelectorAll('.tag-fchip').forEach(btn => {{
      btn.addEventListener('click', () => toggleTag(btn.dataset.tag, btn));
    }});

    const scTag = document.getElementById('sc-tag');
    const scLang = document.getElementById('sc-lang');
    topTags.forEach(item => {{
      const opt = document.createElement('option');
      opt.value = item.label; opt.textContent = item.label;
      scTag.appendChild(opt);
    }});
    [...new Set(repos.map(r => r.language))].sort().forEach(l => {{
      const opt = document.createElement('option');
      opt.value = l; opt.textContent = l;
      scLang.appendChild(opt);
    }});

    let scatterDirty = true;
    function renderScatter() {{
      if (!scatterDirty) return;
      scatterDirty = false;
      const tag = scTag.value, lang = scLang.value;
      const filtered = repos.filter(r => (!tag || r.tags.includes(tag)) && (!lang || r.language === lang));
      const svg = document.getElementById('scatter');
      const W = 1400, H = 380, pad = {{ l:64, r:20, t:20, b:38 }};
      const iW = W - pad.l - pad.r, iH = H - pad.t - pad.b;
      const months = [...new Set(repos.map(r => r.created_month))].sort();
      const localMax = filtered.length ? Math.max(...filtered.map(r => r.stargazers_count)) : 1;
      const xM = m => {{ const i = months.indexOf(m); return months.length < 2 ? pad.l + iW/2 : pad.l + (i/(months.length-1))*iW; }};
      const yS = s => pad.t + iH - (s/localMax)*iH;
      const circles = filtered.map(r => {{
        const c = r.tags.includes('ai') ? '#f97316' : r.tags.includes('agent') ? '#facc15' : '#4b5563';
        return `<circle cx="${{xM(r.created_month).toFixed(1)}}" cy="${{yS(r.stargazers_count).toFixed(1)}}" r="4" fill="${{c}}" opacity="0.75"><title>${{r.full_name}} · ${{r.created_date}} · ${{fmtInt(r.stargazers_count)}} ★</title></circle>`;
      }}).join('');
      const monthLabels = months.map(m => `<text x="${{xM(m).toFixed(1)}}" y="${{H-6}}" text-anchor="middle" fill="#4b5563" font-size="11">${{m}}</text>`).join('');
      const ticks = [0,0.25,0.5,0.75,1].map(f => {{
        const v = Math.round(localMax*f), y = yS(v).toFixed(1);
        return `<line x1="${{pad.l}}" y1="${{y}}" x2="${{W-pad.r}}" y2="${{y}}" stroke="#1f2937"/>`
             + `<text x="${{pad.l-4}}" y="${{(+y+4).toFixed(1)}}" text-anchor="end" fill="#4b5563" font-size="11">${{fmtInt(v)}}</text>`;
      }}).join('');
      svg.innerHTML = ticks
        + `<line x1="${{pad.l}}" y1="${{pad.t+iH}}" x2="${{W-pad.r}}" y2="${{pad.t+iH}}" stroke="#374151"/>`
        + monthLabels + circles;
    }}

    scTag.addEventListener('change', () => {{ scatterDirty = true; renderScatter(); }});
    scLang.addEventListener('change', () => {{ scatterDirty = true; renderScatter(); }});
  }})();
  </script>
</body>
</html>"""


def cmd_render(args: argparse.Namespace) -> None:
    with sqlite3.connect(args.db) as conn:
        repos = _query_repos(conn)
        summary = _query_summary(conn)
        tag_counts = _query_tag_counts(conn)
    if not repos:
        raise RuntimeError("No rows in upcoming_repositories.")
    stats = _build_stats(repos)
    html = _build_html(repos, summary, tag_counts, stats)
    args.output.write_text(html, encoding="utf-8")
    print(f"render: wrote {args.output}")


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--db", type=Path, default=DB_PATH, help="SQLite database path.")
    sub = parser.add_subparsers(dest="cmd")

    # fetch-trending
    p_ft = sub.add_parser("fetch-trending", help="Scrape github.com/trending.")
    p_ft.add_argument("--since", choices=["daily", "weekly", "monthly"], default="weekly")
    p_ft.add_argument("--limit", type=int, default=25)

    # fetch-upcoming
    p_fu = sub.add_parser("fetch-upcoming", help="Fetch up-and-coming repos via GitHub API.")
    p_fu.add_argument("--target-count", type=int, default=1000)
    p_fu.add_argument("--months-back", type=int, default=12)
    p_fu.add_argument("--per-page", type=int, default=100)
    p_fu.add_argument("--pages-per-window", type=int, default=1)
    p_fu.add_argument("--min-stars", type=int, default=100)
    p_fu.add_argument("--cache-dir", type=Path, default=ROOT / "api_cache")
    p_fu.add_argument("--cache-only", action="store_true")

    # tag
    sub.add_parser("tag", help="Apply keyword tags.")

    # render
    p_r = sub.add_parser("render", help="Build index.html.")
    p_r.add_argument("--output", type=Path, default=OUTPUT_PATH)

    # run-all
    p_all = sub.add_parser("run-all", help="Run full pipeline (default).")
    p_all.add_argument("--since", choices=["daily", "weekly", "monthly"], default="weekly")
    p_all.add_argument("--limit", type=int, default=25)
    p_all.add_argument("--target-count", type=int, default=1000)
    p_all.add_argument("--months-back", type=int, default=12)
    p_all.add_argument("--per-page", type=int, default=100)
    p_all.add_argument("--pages-per-window", type=int, default=1)
    p_all.add_argument("--min-stars", type=int, default=100)
    p_all.add_argument("--cache-dir", type=Path, default=ROOT / "api_cache")
    p_all.add_argument("--cache-only", action="store_true")
    p_all.add_argument("--output", type=Path, default=OUTPUT_PATH)

    args = parser.parse_args()

    # default to run-all when invoked with no subcommand
    if args.cmd is None:
        args.cmd = "run-all"
        args.since = "weekly"
        args.limit = 25
        args.target_count = 1000
        args.months_back = 12
        args.per_page = 100
        args.pages_per_window = 1
        args.min_stars = 100
        args.cache_dir = ROOT / "api_cache"
        args.cache_only = False
        args.output = OUTPUT_PATH

    if args.cmd == "fetch-trending":
        cmd_fetch_trending(args)
    elif args.cmd == "fetch-upcoming":
        cmd_fetch_upcoming(args)
    elif args.cmd == "tag":
        cmd_tag(args)
    elif args.cmd == "render":
        cmd_render(args)
    elif args.cmd == "run-all":
        cmd_fetch_trending(args)
        cmd_fetch_upcoming(args)
        cmd_tag(args)
        cmd_render(args)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
