from lxml import html
from urllib.parse import urljoin
import requests
import os
import gzip
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from db_config import fetch_pending_country_urls, insert_region_urls, update_country_status

headers = {
    "user-agent": "Mozilla/5.0"
}

BASE_URL = "https://worldpostalcode.com"
MAX_WORKERS = 10
BATCH_SIZE = 1000

visited_urls = set()
visited_lock = threading.Lock()


def clean_text(text):
    return " ".join(text.split()).strip()


def safe_filename(name):
    invalid = r'<>:"/\|?*'
    for ch in invalid:
        name = name.replace(ch, "_")
    return name.strip()


def country():
    response = requests.get(BASE_URL, headers=headers, timeout=20)
    response.raise_for_status()

    tree = html.fromstring(response.content)
    links = tree.xpath("//div[contains(@class,'regions')]//a")

    country_list = []

    for link in links:
        href = link.get("href")
        name = clean_text(link.text_content())

        if not href or not name:
            continue

        country_list.append({
            "Country_name": name,
            "Country_URL": urljoin(BASE_URL, href),
            "Status": "Pending"
        })

    return country_list


def save_region_backup(region_url, content, country_name):
    base_path = r"D:\Vishal Mistry\world_files\world"
    folder_path = os.path.join(base_path, safe_filename(country_name))
    os.makedirs(folder_path, exist_ok=True)

    url_part = region_url.replace(BASE_URL + "/", "").strip("/")
    file_name = safe_filename(url_part.replace("/", "_")) or "index"

    file_path = os.path.join(folder_path, f"{file_name}.html.gz")

    with gzip.open(file_path, "wb") as f:
        f.write(content)


def mark_url_visited(url):
    with visited_lock:
        if url in visited_urls:
            return False
        visited_urls.add(url)
        return True


def extract_regions(html_data, country_name, country_url, current_url, parent_region=None, level=1):
    tree = html.fromstring(html_data)
    region_rows = []

    blocks = tree.xpath("//h2[normalize-space()='Regions']/following-sibling::div[1]")

    for block in blocks:
        links = block.xpath(".//a")

        for link in links:
            region_name = clean_text("".join(link.xpath(".//text()")))
            href = "".join(link.xpath("./@href")).strip()

            if not region_name or not href:
                continue

            full_url = urljoin(current_url, href)

            if not mark_url_visited(full_url):
                continue

            region_rows.append({
                "country_name": country_name,
                "country_url": country_url,
                "parent_region": parent_region,
                "region_name": region_name,
                "region_url": full_url,
                "level_no": level,
                "status": "Pending"
            })

    return region_rows


def crawl_sub_regions(session, country_name, country_url, region_name, region_url, level, batch):
    try:
        response = session.get(region_url, headers=headers, timeout=20)
        response.raise_for_status()
    except Exception as e:
        print(f"Error in crawl_sub_regions({region_url}): {e}")
        return

    save_region_backup(region_url, response.content, country_name)

    sub_regions = extract_regions(
        html_data=response.content,
        country_name=country_name,
        country_url=country_url,
        current_url=region_url,
        parent_region=region_name,
        level=level
    )

    if sub_regions:
        batch.extend(sub_regions)

        if len(batch) >= BATCH_SIZE:
            insert_region_urls(batch)
            print(f"Inserted Region Rows: {len(batch)}")
            batch.clear()

        for sub in sub_regions:
            crawl_sub_regions(
                session=session,
                country_name=country_name,
                country_url=country_url,
                region_name=sub["region_name"],
                region_url=sub["region_url"],
                level=level + 1,
                batch=batch
            )


def process_country(row):
    global visited_urls

    country_name = row["country_name"]
    country_url = row["country_url"]

    print(f"Processing: {country_name}")

    with visited_lock:
        visited_urls = set()

    batch = []
    session = requests.Session()

    try:
        response = session.get(country_url, headers=headers, timeout=20)
        response.raise_for_status()
    except Exception as e:
        print(f"Error in process_country({country_url}): {e}")
        session.close()
        return

    save_region_backup(country_url, response.content, country_name)

    top_regions = extract_regions(
        html_data=response.content,
        country_name=country_name,
        country_url=country_url,
        current_url=country_url,
        parent_region=None,
        level=1
    )

    if top_regions:
        batch.extend(top_regions)

        for region in top_regions:
            crawl_sub_regions(
                session=session,
                country_name=country_name,
                country_url=country_url,
                region_name=region["region_name"],
                region_url=region["region_url"],
                level=2,
                batch=batch
            )

    if batch:
        insert_region_urls(batch)
        print(f"Inserted Region Rows: {len(batch)}")

    update_country_status(country_url, "Done")
    session.close()


def region():
    countries = fetch_pending_country_urls()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_country, row) for row in countries]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Thread error: {e}")