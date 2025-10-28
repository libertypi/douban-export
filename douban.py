#!/usr/bin/env python3

"""
Export a Douban user's movie ratings.
Usage: python douban.py <username> [-o output_dir]

Author: David P.
"""

import argparse
import json
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from lxml import html

re_year = re.compile(r"\b(?:19|20)\d\d\b")
re_date = re.compile(r"\b(?:19|20)\d\d-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])\b")
re_id = re.compile(r"/subject/(\d+)/")
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


def get_tree(url) -> html.HtmlElement:
    res = session.get(url)
    res.raise_for_status()
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


def ensure_output_dir(path_str: str | None) -> Path:
    if path_str:
        out_dir = Path(path_str).resolve()
    else:
        out_dir = Path(__file__).parent.resolve() / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


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
    for r in data:
        buckets[r["rating"]].append(r)

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Douban movie ratings of {username}\n")
        for rating in range(5, -1, -1):
            if not buckets[rating]:
                continue
            if rating > 0:
                f.write(f"\n## {'★'*rating} ({rating} stars)\n\n")
            else:
                f.write("\n## Unrated\n\n")
            for r in buckets[rating]:
                title_zh = r.get("title_zh")
                f.write(
                    f"* {r['title']} / {title_zh} ({r['year']})\n"
                    if title_zh
                    else f"* {r['title']} ({r['year']})\n"
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
        help="Output directory (default: <script_dir>/output)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    username = args.username

    json_path = ensure_output_dir(args.output_dir) / f"douban_{username}.json"
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
