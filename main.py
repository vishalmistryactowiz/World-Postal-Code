from parser import country, region
from postal_code_extract import main_postal
from db_config import create_country_table, create_region_table, insert_country_urls


def main():
    create_country_table()
    create_region_table()

    country_urls = country()
    insert_country_urls(country_urls)

    region()
    main_postal()

if __name__ == "__main__":
    main()