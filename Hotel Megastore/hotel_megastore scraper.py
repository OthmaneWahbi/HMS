import psycopg2
from bs4 import BeautifulSoup
import requests
from datetime import datetime

connection = psycopg2.connect(
    database="hotel_megastore_competition",
    user='postgres',
    password='root',
    host='localhost',
    port='5432'
)
cursor = connection.cursor()

session = requests.Session()
session.get("https://www.hotelmegastore.com/fr/")

cursor.execute("SELECT competitors_name, competitor_id FROM public.competitors;")
existing_competitors = dict(cursor.fetchall())

if "Hotel Megastore" not in existing_competitors:
    insert_competitor_query = """
        INSERT INTO public.competitors (competitors_name, website, last_scrapped)
        VALUES (%s, %s, %s)
        RETURNING competitor_id;
    """
    cursor.execute(insert_competitor_query, ("Hotel Megastore", "https://www.hotelmegastore.com", datetime.now()))
    competitor_id = cursor.fetchone()[0]
else:
    competitor_id = existing_competitors["Hotel Megastore"]

connection.commit()
cursor.close()
connection.close()



def replace_letters_with_ascii(text):
    result = ""
    for char in text:
        if char.isalpha():
            ascii_code = ord(char)
            result += str(ascii_code)
        else:
            result += char
    return result

def insert_into_database(product_details):
    connection = psycopg2.connect(
        database="hotel_megastore_competition",
        user='postgres',
        password='root',
        host='localhost',
        port='5432'
    )
    cursor = connection.cursor()

    cursor.execute("SELECT category_name, category_id FROM product_categories;")
    existing_categories = dict(cursor.fetchall())

    if product_details["category"] not in existing_categories:
        insert_category_query = """
                INSERT INTO public.product_categories (category_name)
                VALUES (%s)
                RETURNING category_id;
        """
        cursor.execute(insert_category_query, (product_details["category"],))
        category_id = cursor.fetchone()[0]
    else:
        category_id = existing_categories[product_details["category"]]

    cursor.execute("SELECT product_id,product_name FROM public.products WHERE competitor_id = %s;", (competitor_id,))
    existing_products = {int(product_id[0]) for product_id in cursor.fetchall()}
    product_id = product_details["ref"]
    product_id = replace_letters_with_ascii(product_id)
    if int(product_id) not in existing_products:
        insert_product_query = """
                INSERT INTO public.products (product_id,competitor_id, category_id, product_name, scrapped_at, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s);
            """
        cursor.execute(insert_product_query,
                       (product_id,competitor_id, category_id, product_details["title"], datetime.now(), datetime.now()))
    else:
        update_last_updated_query = """
                UPDATE public.products
                SET last_updated = %s
                WHERE product_id = %s;
            """
        cursor.execute(update_last_updated_query, (datetime.now(), product_id))

    check_pricing_tier_query = """
        SELECT price_per_unit
        FROM public.product_pricing_tiers
        WHERE product_id = %s AND competitor_id = %s AND min_quantity = %s;
    """
    for price_info in product_details["prices"]:
        cursor.execute(check_pricing_tier_query, (product_id, competitor_id, price_info["min_quantite"]))
        existing_record = cursor.fetchone()
        if existing_record:
            existing_price = existing_record[0]
            new_price = price_info["prix"]
            if float(existing_price) != float(new_price):
                update_pricing_tier_query = """
                            UPDATE public.product_pricing_tiers
                            SET price_per_unit = %s, last_updated = %s
                            WHERE product_id = %s AND competitor_id = %s AND min_quantity = %s
                            RETURNING pricing_tier_id;
                        """
                cursor.execute(update_pricing_tier_query,
                               (new_price, datetime.now(), product_id, competitor_id, price_info["min_quantite"]))
                pricing_tier_id = cursor.fetchone()[0]

                insert_new_pricing = """
                                            INSERT INTO public.changed_prices (product_id, pricing_tier_id, old_price, new_price)
                                            VALUES (%s, %s, %s, %s)
                                        """
                cursor.execute(insert_new_pricing,
                               (product_id, pricing_tier_id,existing_price, new_price))

            else:
                update_last_updated_query = """
                            UPDATE public.product_pricing_tiers
                            SET last_updated = %s
                            WHERE product_id = %s AND competitor_id = %s AND min_quantity = %s;
                        """
                cursor.execute(update_last_updated_query,
                               (datetime.now(), product_id, competitor_id, price_info["min_quantite"]))
        else:
            insert_pricing_tier_query = """
                INSERT INTO public.product_pricing_tiers (product_id, competitor_id, min_quantity, price_per_unit, scrapped_at, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s);
            """

            cursor.execute(insert_pricing_tier_query, (
                product_id, competitor_id, price_info["min_quantite"], price_info["prix"], datetime.now(),
                datetime.now()))

    cursor.execute("""
        SELECT specification_name, value
        FROM public.product_specifications
        WHERE product_id = %s AND competitor_id = %s;
    """, (product_id, competitor_id))

    existing_specifications = {spec_name: value for spec_name, value in cursor.fetchall()}
    insert_specification_query = """
        INSERT INTO public.product_specifications (product_id, competitor_id, specification_name, value, scrapped_at, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s);
    """

    for characteristic in product_details["characteristics"]:
        spec_name = characteristic["name"]
        spec_value = characteristic["value"]

        if spec_name in existing_specifications:
            existing_value = existing_specifications[spec_name]

            if existing_value != spec_value:
                cursor.execute("""
                    UPDATE public.product_specifications
                    SET value = %s, last_updated = %s
                    WHERE product_id = %s AND competitor_id = %s AND specification_name = %s;
                """, (spec_value, datetime.now(), product_id, competitor_id, spec_name))
            else:
                cursor.execute("""
                    UPDATE public.product_specifications
                    SET last_updated = %s
                    WHERE product_id = %s AND competitor_id = %s AND specification_name = %s;
                """, (datetime.now(), product_id, competitor_id, spec_name))
        else:
            cursor.execute(insert_specification_query, (
                product_id, competitor_id, spec_name, spec_value, datetime.now(), datetime.now()))

    connection.commit()
    cursor.close()
    connection.close()

def scrape_page(url, filter_index=0):
    if "hotelmegastore.com" not in url:
        url = "https://www.hotelmegastore.com" + url
    response = session.get(url, allow_redirects=False)
    soup = BeautifulSoup(response.text, 'html.parser')

    filters = soup.find_all('div', class_='container-type-filtre fermerCaracteristiques')
    if filter_index >= len(filters):
        return [scrape_product_details(soup, response.url)]

    current_filter = filters[filter_index]

    filter_label = current_filter.find('p', class_='libelle-filtre').text.lower()

    if 'color' in filter_label or 'colour' in filter_label:
        return scrape_page(url, filter_index + 1)

    options = current_filter.find_all('div', class_='item')
    if filter_index != 0:
        options = [option for option in options if 'disabled' not in option.get('class', [])]

    product_details = []
    for option in options:
        option_url = option.find('a').get('href')
        product_details.extend(scrape_page(option_url, filter_index + 1))

    return product_details

def scrape_product_details(soup, url=""):
    if "hotelmegastore.com" not in url:
        url = "https://www.hotelmegastore.com" + url
    print(url)
    product_details = {}
    product_title = soup.find("h1", {"class": "titreProduit"}).text.strip()
    product_details["title"] = product_title
    product_category = soup.find_all('li', {'class': 'breadcrumb-item'})[-2].text.strip()
    product_details["category"] = product_category
    product_ref = soup.find('p', {"class": "ref"}).text.lower().replace("ref .", "").strip()
    product_details["ref"] = product_ref.split(":")[-1].replace(".","").strip()

    product_details["prices"] = []
    product_details["characteristics"] = []
    product_price_information = soup.find_all(class_="item-prix")
    product_caracteristiques = soup.find_all(class_="col-lg-6 col-xs-12 px-0")
    for product_price in product_price_information:
        data_min_quantite = product_price['data-min-quantite'].strip()
        data_prix = product_price['data-prix'].replace("€", "").strip()
        product_details["prices"].append({"min_quantite": data_min_quantite, "prix": data_prix})
        for product_caracteristique in product_caracteristiques:
            caracteristique_name = product_caracteristique.find(class_="fontOutfit-Bold").text
            caracteristique_value = product_caracteristique.find(class_="text-capitalize").text
            product_details["characteristics"].append({"name": caracteristique_name, "value": caracteristique_value})
    print(product_details)
    insert_into_database(product_details)
    return product_details

all_products_urls = []

url = "https://www.hotelmegastore.com"
response = session.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

hrefs = [a.get('href') for a in soup.select('p.menu_simple_title > a.rayon_ligne_colonne')]

for href in hrefs:
    if href:
        response = session.get("https://www.hotelmegastore.com" + href)
        index = 1
        soup = BeautifulSoup(response.text, 'html.parser')
        next_span = soup.find('span', class_='next')
        products_links = [a.get('href') for a in
                          soup.select('div.container_vignette_produit_refonte.taille_fixe > a')]
        while next_span:
            index += 1
            response = session.get("https://www.hotelmegastore.com" + next_span.find('a').get('href'))
            soup = BeautifulSoup(response.text, 'html.parser')
            next_span = soup.find('span', class_='next')
            products_links += [a.get('href') for a in
                               soup.select('div.container_vignette_produit_refonte.taille_fixe > a')]
        for product_link in products_links:
            print(url + product_link)
            all_products_urls.append(url+product_link)

for product_url in all_products_urls:
    scrape_page(product_url)
