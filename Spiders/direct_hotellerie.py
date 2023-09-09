from datetime import datetime
import psycopg2
import requests
import scrapy
from bs4 import BeautifulSoup

connection = psycopg2.connect(
    database="hotel_megastore_competition",
    user='postgres',
    password='root',
    host='localhost',
    port='5432'
)
cursor = connection.cursor()

cursor.execute("SELECT competitors_name, competitor_id FROM public.competitors;")
existing_competitors = dict(cursor.fetchall())

if "direct-hotellerie" not in existing_competitors:
    insert_competitor_query = """
        INSERT INTO public.competitors (competitors_name, website, last_scrapped)
        VALUES (%s, %s, %s)
        RETURNING competitor_id;
    """
    cursor.execute(insert_competitor_query, ("direct-hotellerie", "https://www.direct-hotellerie.fr/", datetime.now()))
    competitor_id = cursor.fetchone()[0]
else:
    update_last_updated_query = """
                   UPDATE public.competitors
                   SET last_scrapped = %s
                   WHERE competitor_id = %s;
               """
    competitor_id = existing_competitors["direct-hotellerie"]
    cursor.execute(update_last_updated_query, (datetime.now(), competitor_id))

connection.commit()
cursor.close()
connection.close()

categories = []

url = "https://www.direct-hotellerie.com/"
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

all_uls = soup.find_all('ul', {'data-depth': 1})
for ul in all_uls:
    all_big_categories = ul.find_all('li', recursive=False)
    for big_category in all_big_categories:
        div_element = big_category.find('ul', {'data-depth': 2})
        if not div_element:
            category = big_category.find("a").text
            link = big_category.find("a")["href"]
            categories.append({'category': category, 'link': link})

all_uls = soup.find_all('ul', {'data-depth': 2})
for ul in all_uls:
    for li in ul.find_all('li', {'class': 'category'}):
        category = li.a.text
        link = li.a['href']

        category_info = {'category': category, 'link': link}
        categories.append(category_info)

urls = []

for category in categories:
    response = requests.get(category["link"])
    soup = BeautifulSoup(response.text, 'html.parser')
    next_page_url = ''
    next_page_bool = True
    while next_page_bool:
        products = soup.find_all('span', {'class': 'ptitle'})
        for product in products:
            product_link = product.a["href"]
            urls.append(product_link)
            print(product_link)
        try:
            next_page_url = soup.find("a", {'rel': 'next'})["href"]
        except:
            next_page_bool = False

        if next_page_url == response.url:
            next_page_bool = False
        if next_page_bool:
            response = requests.get(next_page_url)
            soup = BeautifulSoup(response.text, 'html.parser')


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

    if int(product_id) not in existing_products:
        insert_product_query = """
                INSERT INTO public.products (product_id,competitor_id, category_id, product_name, scrapped_at, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s);
            """
        cursor.execute(insert_product_query,
                       (product_id, competitor_id, category_id, product_details["title"], datetime.now(), datetime.now()))
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
    cursor.execute(check_pricing_tier_query, (product_id, competitor_id, product_details["min_quantite"]))
    existing_record = cursor.fetchone()
    if existing_record:
        existing_price = existing_record[0]
        new_price = product_details["prix_ttc"]
        if float(existing_price) != float(new_price):
            update_pricing_tier_query = """
                        UPDATE public.product_pricing_tiers
                        SET price_per_unit = %s, last_updated = %s
                        WHERE product_id = %s AND competitor_id = %s AND min_quantity = %s
                        RETURNING pricing_tier_id;
                    """
            cursor.execute(update_pricing_tier_query,
                           (new_price, datetime.now(), product_id, competitor_id, product_details["min_quantite"]))
            pricing_tier_id = cursor.fetchone()[0]

            insert_new_pricing = """
                                        INSERT INTO public.changed_prices (product_id, pricing_tier_id, old_price, new_price)
                                        VALUES (%s, %s, %s, %s)
                                    """
            cursor.execute(insert_new_pricing,
                           (product_id, pricing_tier_id, existing_price, new_price))
        else:
            update_last_updated_query = """
                        UPDATE public.product_pricing_tiers
                        SET last_updated = %s
                        WHERE product_id = %s AND competitor_id = %s AND min_quantity = %s;
                    """
            cursor.execute(update_last_updated_query,
                           (datetime.now(), product_id, competitor_id, product_details["min_quantite"]))
    else:
        insert_pricing_tier_query = """
            INSERT INTO public.product_pricing_tiers (product_id, competitor_id, min_quantity, price_per_unit, scrapped_at, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s);
        """

        cursor.execute(insert_pricing_tier_query, (
            product_id, competitor_id, product_details["min_quantite"], product_details["prix_ttc"], datetime.now(),
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


class DirectHotellerieSpider(scrapy.Spider):
    name = "direct_hotellerie"
    allowed_domains = ["direct-hotellerie.com"]
    start_urls = urls
    def parse(self, response):
        product_details = {}
        soup = BeautifulSoup(response.text, 'html.parser')
        item_name = soup.find('h1', {"itemprop": 'name'}).text
        product_ref = soup.find(id="product-reference").find("span", {"itemprop": "sku"}).text
        product_categories = soup.find_all("li", {"class":"breadcrumb-item"})
        category_name = ""
        for product_category in product_categories[:-1]:
            if "accueil" not in product_category.text.lower():
                category_name += product_category.text.replace("\n","").strip() + "/"

        product_features = soup.find("section", {"class": "product-features"})
        if product_features:
            product_features = product_features.find_all('li')
            product_features = [product_feature.text.strip() for product_feature in product_features]

        #qt_min = soup.find(id='quantity_wanted')["min"]
        options_names = []
        select = soup.find(id='variant')
        if select:
            all_options = select.find_all('option')
            for option in all_options:
                if "choisir" not in option.text.lower():
                    options_names.append(option.text)

        table_de_prix = soup.find("table", {"class": "table-product-discounts"})
        if table_de_prix:
            table_de_prix = table_de_prix.find('tbody')
            table_rows = table_de_prix.find_all("tr")
            for row in table_rows:
                cells = row.find_all("td")
                discount_tier = ' '.join(cells[0].text.replace("\n", "").split())
                #prix_HT = cells[1].text.replace("\xa0", "").strip().replace(",", '.')
                prix_TTC = cells[2].text.replace("\xa0", "").strip().replace(",", '.')
                product_details["title"] = item_name
                if category_name.endswith('/'):
                    category_name = category_name[:-1]
                product_details["category"] = category_name
                product_details["ref"] = product_ref
                product_details["prix_ttc"] = prix_TTC.replace("€", "").strip()
                product_details["min_quantite"] = discount_tier.split(" ")[0]
                product_details["characteristics"] = []
                if product_features:
                    for feature in product_features:
                        feature_name = feature.split('-')[0]
                        feature_value = feature.split('-')[-1]
                        product_details["characteristics"].append({"name": feature_name, "value": feature_value})

                print(product_details)
                insert_into_database(product_details)
                yield product_details

