#!/usr/bin/env python3

"""
Export a Douban user's movie ratings.
Usage: python douban.py <username> [-o output_dir]

Author: David P.
"""

import argparse
import hashlib
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from lxml import html

re_year = re.compile(r"\b(?:19|20)\d\d\b")
re_date = re.compile(r"\b(?:19|20)\d\d-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])\b")
re_id = re.compile(r"/subject/(\d+)/")
re_difficulty = re.compile(r"difficulty\s*=\s*(\d+)")
rating_map = {
    "rating1-t": 1,
    "rating2-t": 2,
    "rating3-t": 3,
    "rating4-t": 4,
    "rating5-t": 5,
    "": 0,  # unrated
}

# --- HTTP session ---
session = requests.Session()
session.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/116.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "zh;q=0.8,en-US;q=0.5,en;q=0.3",
    }
)

_pow_lock = threading.Lock()


def _solve_pow(cha: str, difficulty: int) -> int:
    """
    Python version of Douban's JS PoW:
    find smallest nonce >= 1 such that
    sha512(cha + str(nonce)) has a hex digest starting with difficulty zeros.
    """
    target = "0" * difficulty
    cha_bytes = cha.encode("utf-8")
    nonce = 0
    while True:
        nonce += 1
        digest = hashlib.sha512(cha_bytes + str(nonce).encode("ascii")).hexdigest()
        if digest.startswith(target):
            return nonce


def _maybe_handle_pow(res: requests.Response, original_url: str) -> requests.Response:
    """
    If `res` is the PoW challenge page, solve it and then re-fetch original_url.
    Otherwise, just return `res` unchanged.
    """
    text = res.text

    # Cheap check: real pages won't contain both of these
    if 'id="cha"' not in text or 'id="tok"' not in text:
        return res

    tree = html.fromstring(text)
    tok_nodes = tree.xpath('//input[@id="tok"]/@value')
    cha_nodes = tree.xpath('//input[@id="cha"]/@value')
    red_nodes = tree.xpath('//input[@id="red"]/@value')
    action_nodes = tree.xpath('//form[@id="sec"]/@action')

    if not (tok_nodes and cha_nodes and red_nodes):
        # Not the challenge form we expect; just return original response
        return res

    tok = tok_nodes[0]
    cha = cha_nodes[0]
    red = red_nodes[0]
    action = action_nodes[0] if action_nodes else "/c"

    challenge_url = urljoin(res.url, action)

    m = re_difficulty.search(text)
    difficulty = int(m.group(1)) if m else 4

    with _pow_lock:
        # Only one thread solves at a time (good for CPU and not hammering Douban)
        sol = _solve_pow(cha, difficulty)
        payload = {
            "tok": tok,
            "cha": cha,
            "sol": str(sol),
            "red": red,
        }

        parsed = urlparse(res.url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        headers = dict(session.headers)
        headers.update(
            {
                "Referer": res.url,
                "Origin": origin,
            }
        )

        r2 = session.post(challenge_url, data=payload, headers=headers)
        r2.raise_for_status()

    res2 = session.get(original_url)
    res2.raise_for_status()
    return res2


def get_tree(url) -> html.HtmlElement:
    res = session.get(url)
    res.raise_for_status()
    res = _maybe_handle_pow(res, url)
    return html.fromstring(res.content)


def parse_page(tree: html.HtmlElement):
    """Parse a single 'collect' page."""
    for it in tree.xpath(
        "//div[contains(@class,'grid-view')]//div[contains(@class,'comment-item')]"
    ):
        url = it.xpath(".//li[@class='title']/a/@href")[0]
        _id = int(re_id.search(url)[1])

        # ----- titles: "中文 / Original" or "Original" (usually Chinese) -----
        title = it.xpath("normalize-space(.//li[@class='title']/a/em)").partition(" / ")
        title_zh = title[0].strip()
        title = title[2].strip()
        if not title or title == title_zh:
            title = title_zh
            title_zh = None
            if not title:
                raise ValueError(f"No title found for id {_id}.")

        # ----- film year (YYYY) -----
        year = it.xpath("string(.//li[@class='intro'])")
        try:
            year = int(re_year.match(year.lstrip())[0])  # match from the start of intro
        except TypeError:
            # fallback: open the movie's subject page and read (YYYY) in the header
            page_tree = get_tree(url)
            year = page_tree.xpath("string(//h1/span[@class='year'])")
            try:
                year = int(re_year.search(year)[0])
            except TypeError as e:
                raise ValueError(f"No year found in '{title}': {e}")

        # ----- rating (1..5) -----
        rating = it.xpath("string(.//li/span[starts-with(@class,'rating')][1]/@class)")
        try:
            rating = rating_map[rating.strip()]
        except KeyError:
            raise ValueError(f"Unknown rating '{rating}' in: {title}")

        # ----- rating date (YYYY-MM-DD) -----
        rated_at = it.xpath("string(.//li/span[@class='date'])")
        try:
            rated_at = re_date.search(rated_at)[0]
        except TypeError:
            raise ValueError(f"Invalid rating date '{rated_at}' in: {title}")

        # ----- comment -----
        comment = it.xpath("string(.//li/span[@class='comment'])").strip() or None

        data = {
            "id": _id,
            "title": title,
            "title_zh": title_zh,
            "year": year,
            "rating": rating,
            "rated_at": rated_at,
            "comment": comment,
        }
        for k in "title_zh", "comment":
            if data[k] is None:
                del data[k]

        yield _id, data


def scrape(username: str, max_pages: int | None = None) -> dict[int, dict]:
    """Scrape all rated movies of a user, newest ratings first."""
    url = f"https://movie.douban.com/people/{username}/collect?sort=time&mode=grid"

    print("Scanning page 1 ...", end="\r")
    tree = get_tree(url)
    results = dict(parse_page(tree))

    last_page = int(
        tree.xpath(
            "number(//div[@class='paginator']//span[@class='thispage']/@data-total-page)"
        )
    )
    if max_pages is not None and 0 < max_pages < last_page:
        last_page = max_pages

    done = 1
    urls = (f"{url}&start={p * 15}" for p in range(1, last_page))
    with ThreadPoolExecutor(max_workers=5) as ex:
        for tree in ex.map(get_tree, urls):
            done += 1
            print(f"Scanning page {done}/{last_page} ...", end="\r")
            results.update(parse_page(tree))

    print()  # clear line
    return results


def ensure_outdir(outdir: Path | None) -> Path:
    if outdir:
        outdir = outdir.resolve()
    else:
        outdir = Path(__file__).parent.resolve() / "output"
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir


def read_json(json_path) -> list[dict]:
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return []
    if isinstance(data, list):
        return data
    raise ValueError(f"Invalid JSON format in {json_path}: expected a list.")


def write_json(json_path, data: list[dict]):
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def write_markdown(md_path, data: list[dict], username: str):
    """
    Write movie titles to a markdown file, grouped by ratings 5→0.
    """
    buckets = [[] for _ in range(6)]
    for d in data:
        buckets[d["rating"]].append(d)

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Douban movie ratings of {username}\n")
        for rating in range(5, -1, -1):
            if not buckets[rating]:
                continue
            if rating > 0:
                f.write(f"\n## {'★'*rating} ({rating} stars)\n\n")
            else:
                f.write("\n## Unrated\n\n")
            for d in buckets[rating]:
                title_zh = d.get("title_zh")
                f.write(
                    f"* {d['title']} / {title_zh} ({d['year']})\n"
                    if title_zh
                    else f"* {d['title']} ({d['year']})\n"
                )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export a Douban user's movie ratings."
    )
    parser.add_argument("username", help="Douban username (people/<username>)")
    parser.add_argument(
        "-m",
        "--max-pages",
        type=int,
        help="Maximum number of pages to scrape",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        help="Output directory (default: <script_dir>/output)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    username = args.username

    json_path = ensure_outdir(args.output_dir) / f"douban_{username}.json"
    md_path = json_path.with_suffix(".md")

    # Scrape
    data = scrape(username, args.max_pages)
    scrape_count = len(data)

    # Read JSON (list -> dict)
    old = {d["id"]: d for d in read_json(json_path)}
    added_count = len(data.keys() - old.keys())

    # Add missed entries from old data
    old = [old[k] for k in (old.keys() - data.keys())]
    data = list(data.values())
    if old:
        data.extend(old)
        data.sort(key=lambda r: r["rated_at"], reverse=True)

    # Write JSON (as a list)
    write_json(json_path, data)

    # Write Markdown (grouped by rating)
    write_markdown(md_path, data, username)

    print(
        f"Scraped: {scrape_count}, new: {added_count}, total: {len(data)}\n"
        "Exported movie ratings to:\n"
        f"  {json_path}\n"
        f"  {md_path}"
    )


if __name__ == "__main__":
    main()
