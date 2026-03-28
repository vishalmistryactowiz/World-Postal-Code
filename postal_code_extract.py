from lxml import html
import os
import gzip
from threading import Thread, Lock
from db_config import create_postal_table, insert_postal_codes, update_region_status

base_path = r"D:\Vishal Mistry\world_files\world"

BATCH_SIZE = 500
CHUNK_SIZE = 200

print_lock = Lock()


def extract_postal_codes(html_data):
    postal_codes = []

    try:
        tree = html.fromstring(html_data)
    except Exception as e:
        with print_lock:
            print(f"HTML Parse Error -> {e}")
        return postal_codes

    blocks = tree.xpath("//div[@class='unit'] | //div[@class='unit full']")
    country_xpath = tree.xpath("//h1//text()")
    clean_title = " ".join(x.strip() for x in country_xpath if x.strip()).strip()

    for block in blocks:
        postal_code = block.xpath(
            "normalize-space(string(.//div[contains(@class,'code')]//span))"
        )

        if clean_title and postal_code:
            postal_codes.append({
                "country_region": clean_title,
                "postal_code": postal_code
            })

    return postal_codes


def file_to_region_url(file_path):
    try:
        file_name = os.path.basename(file_path)
        name = file_name.replace(".html.gz", "")
        region_path = name.replace("_", "/")
        return "https://worldpostalcode.com/" + region_path
    except Exception as e:
        with print_lock:
            print(f"URL Convert Error: {file_path} -> {e}")
        return None


def flush_batch(batch):
    if batch:
        try:
            print(f"Insert Query | Table: postal_codes | Rows: {len(batch)}")
            insert_postal_codes(batch)
            batch.clear()
        except Exception as e:
            with print_lock:
                print(f"Insert Error -> {e}")


def process_files(file_chunk):
    batch = []

    for file_path in file_chunk:
        try:
            with gzip.open(file_path, "rb") as f:
                raw = f.read()

            try:
                html_data = raw.decode("utf-8")
            except UnicodeDecodeError:
                html_data = raw.decode("latin-1", errors="ignore")

            postal_codes = extract_postal_codes(html_data)

            if postal_codes:
                batch.extend(postal_codes)

            region_url = file_to_region_url(file_path)
            if region_url:
                update_region_status(region_url, "Done")

            if len(batch) >= BATCH_SIZE:
                flush_batch(batch)

            with print_lock:
                print(f"Processed: {file_path} | Extracted: {len(postal_codes)}")

        except Exception as e:
            with print_lock:
                print(f"Process Error: {file_path} -> {e}")

    flush_batch(batch)


def get_all_gz_files(path):
    all_files = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".gz"):
                all_files.append(os.path.join(root, file))
    return all_files


def chunk_list(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def main_postal():
    create_postal_table()

    all_files = get_all_gz_files(base_path)

    with print_lock:
        print(f"Total .gz files found: {len(all_files)}")

    threads = []

    for file_chunk in chunk_list(all_files, CHUNK_SIZE):
        t = Thread(target=process_files, args=(file_chunk,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    with print_lock:
        print("All files processed successfully.")
