from datetime import datetime
import scrapy
import requests
from bs4 import BeautifulSoup
import psycopg2

this_competitor = "Bos-Direct"
this_competitor_website = "https://www.bos-direct.com/"

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

if this_competitor not in existing_competitors:
    insert_competitor_query = """
        INSERT INTO public.competitors (competitors_name, website, last_scrapped)
        VALUES (%s, %s, %s)
        RETURNING competitor_id;
    """
    cursor.execute(insert_competitor_query, (this_competitor, this_competitor_website, datetime.now()))
    competitor_id = cursor.fetchone()[0]
else:
    competitor_id = existing_competitors[this_competitor]

connection.commit()
cursor.close()
connection.close()

xml_url = "https://www.bos-direct.com/1_fr_0_sitemap.xml"

response = requests.get(xml_url)
xml_content = response.text

soup = BeautifulSoup(xml_content, "xml")
url_elements = soup.find_all("loc", text=lambda text: text.startswith("https://www.bos-direct.com/boutique/") and text.endswith(".html"))

urls = []

for url_element in url_elements:
    url = url_element.string.strip()
    urls.append(url)

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
    product_id = int(product_details["product_id"])

    if product_id not in existing_products:
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


class BosDirectSpider(scrapy.Spider):
    name = "bos_direct"
    allowed_domains = ["bos-direct.com"]
    start_urls = urls

    def parse(self, response):
        product_name = response.css('h1[itemprop="name"]::text').get()
        product_ref = response.css('td[id="product_reference"]::text').get()
        product_id = product_ref
        category_items = []
        breadcrumb_elements = response.css(
            'span[itemtype="http://data-vocabulary.org/Breadcrumb"] span[itemprop="title"]::text').extract()
        for text in breadcrumb_elements:
            if text.upper() != "BOUTIQUE":
                category_items.append(text)
        category = '/'.join(category_items)
        p_element = response.css('p.lot#product_unity')
        min_quantity = p_element.attrib['rel']
        td_element = response.css('td#product_reference')

        sku_text = td_element.css('span[itemprop="sku"]::text').get()
        price = response.css('span#our_price_display').attrib['rel']
        product_details = {
            'product_id' : str(sku_text)+str(competitor_id),
            'category': category,
            'title': product_name,
            "min_quantite":min_quantity,
            "prix_ttc" : price
        }
        product_details["characteristics"] = []
        characteristiques = response.css("table.table-data-sheet").css("tr")
        for row in characteristiques:
            title = row.css('td::text').getall()[0]
            value = row.css('td::text').getall()[1]
            if value and title!="Référence":
                product_details["characteristics"].append({"name": title, "value": value})
            elif title == "Référence":
                pass
        yield product_details
        insert_into_database(product_details)


