from datetime import datetime
import psycopg2
import scrapy
import re
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By

all_products_urls = []

chromedriver_path = 'C:\\Users\\Toshiba\\Desktop\\chromedriver\\chromedriver.exe'
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--blink-settings=imagesEnabled=false')
chrome_options.add_argument('--headless')
driver = webdriver.Chrome(executable_path=chromedriver_path,options=chrome_options)
driver.maximize_window()
driver.implicitly_wait(3)
print("Driver started")

driver.get("https://www.ecotel.fr/Liste.aspx")
driver.find_elements(By.CSS_SELECTOR,"button[class='close']")[1].click()
driver.find_element(By.CSS_SELECTOR,"button[id='dropdownNb']").click()
nbr_produits_pr_page = driver.find_element(By.CSS_SELECTOR,"a[onclick='NbPerPage(72)']")
driver.execute_script("arguments[0].setAttribute('onclick', 'NbPerPage(1000)')", nbr_produits_pr_page)
nbr_produits_pr_page.click()
time.sleep(2)
driver.find_element(By.CSS_SELECTOR,"button[id='tarteaucitronAllDenied2']").click()
button = driver.find_element(By.CSS_SELECTOR,"ul[name='DataPagerProducts']").find_elements(By.TAG_NAME,"li")[-1]
while button:
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    all_products = soup.find_all('a', {'class': 'h-100'})
    hrefs = [anchor.get('href') for anchor in all_products]
    for href in hrefs:
        all_products_urls.append("https://www.ecotel.fr/" + href)
    print("One page of products scrapped")
    time.sleep(1)
    button = driver.find_element(By.CSS_SELECTOR, "ul[name='DataPagerProducts']").find_elements(By.TAG_NAME, "li")[-1]
    button_soup = soup.select_one("ul[name='DataPagerProducts'] li:last-of-type")
    if 'disabled' in button_soup.get('class', []):
        button = False
    else:
        button.click()
driver.close()


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

if "Ecotel" not in existing_competitors:
    insert_competitor_query = """
        INSERT INTO public.competitors (competitors_name, website, last_scrapped)
        VALUES (%s, %s, %s)
        RETURNING competitor_id;
    """
    cursor.execute(insert_competitor_query, ("Ecotel", "https://www.ecotel.fr/", datetime.now()))
    competitor_id = cursor.fetchone()[0]
else:
    update_last_updated_query = """
                   UPDATE public.competitors
                   SET last_scrapped = %s
                   WHERE competitor_id = %s;
               """
    competitor_id = existing_competitors["Ecotel"]
    cursor.execute(update_last_updated_query, (datetime.now(), competitor_id))

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


class ProductSpider(scrapy.Spider):
    name = 'ecotel_spider'

    def start_requests(self):
        for url in all_products_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        reference_et_marque = response.css('div.col-md-6').get()
        ref_pattern = r"Référence : ([A-Za-z0-9]+)"
        reference = re.findall(ref_pattern,reference_et_marque)[0]
        reference = replace_letters_with_ascii(reference)
        prix_lot = response.css('p.prix-lot::text').get()
        if prix_lot:
            pattern = r'Vendu par (\d+)'
            match = re.search(pattern, prix_lot)
            if match:
                nbr_min = match.group(1)
            else:
                nbr_min = 1
        else:
            nbr_min = 1

        category_items = response.css('div.fil-ariane ul li:not(:nth-child(1)):not(:nth-child(2)) a::text').getall()
        category = '/'.join(category_items)

        product_details = {
            'product_id' : reference,
            'category': category,
            'title': response.css('div.col-md-9 h1::text').get(),
            'desciption': response.css('div.description p::text').get(),
            "min_quantite": nbr_min,
        }

        prix_ttc = response.css('p.prix-ttc::text').get()
        if prix_ttc:
            prix_ttc = prix_ttc.split('€')[0].strip()
            product_details["prix_ttc"] = prix_ttc.replace(',', '.').replace(' ', '').replace(" ", "")

        prix_unitaire_ht = response.css('p.prix-unite::text').get()
        if prix_unitaire_ht:
            prix_unitaire_ht = prix_unitaire_ht.split('€')[0].strip()
            product_details["prix_ht"] = prix_unitaire_ht.replace(',', '.').replace(' ', '').replace(" ", "")

        prix_ht = response.css('p.prix-ht::text').get()
        if prix_ht:
            prix_ht = prix_ht.split('€')[0].strip()
            product_details["prix_ht"] = prix_ht.replace(',', '.').replace(' ', '').replace(" ", "")

        info_text = response.css('div.col-md-6::text').get()
        for info in info_text:
            reference_match = re.search(r'Référence\s*:\s*(\S+)', info)
            marque_match = re.search(r'Marque\s*:\s*(\S+)', info)

            if reference_match:
                product_details['reference'] = reference_match.group(1)
            if marque_match:
                product_details['marque'] = marque_match.group(1)

        pattern = r'\([\d,]+ € TTC\)'
        prix_lot = response.css('p.prix-lot::text').get()

        if prix_lot:
            match = re.search(pattern, prix_lot)
            if match:
                extracted_value = match.group().replace('(', '').replace(')', '')
                product_details["prix_lot(TTC)"] = extracted_value.split('€')[0].strip().replace(',', '.')
                prix_ttc = float(product_details["prix_lot(TTC)"].replace(',', '.')) / float(nbr_min)
                product_details["prix_ttc"] = "{:.2f}".format(prix_ttc)

        caracteristiques = response.css("div.caractéristique").css("div.row")
        product_details["characteristics"] = []
        for row in caracteristiques:
            title = row.css('div.title::text').get().strip()
            value = row.css('div.value::text').get().strip()
            if value:
                product_details["characteristics"].append({"name": title, "value": value})
        yield product_details
        insert_into_database(product_details)
