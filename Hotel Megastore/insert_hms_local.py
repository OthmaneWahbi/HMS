import psycopg2
from datetime import datetime
import pandas as pd

def replace_letters_with_ascii(text):
    result = ""
    for char in text:
        if char.isalpha():
            ascii_code = ord(char)
            result += str(ascii_code)
        else:
            result += char
    return result

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

if "HMS local" not in existing_competitors:
    insert_competitor_query = """
        INSERT INTO public.competitors (competitors_name, website, last_scrapped)
        VALUES (%s, %s, %s)
        RETURNING competitor_id;
    """
    cursor.execute(insert_competitor_query, ("HMS local", "HMS local", datetime.now()))
    competitor_id = cursor.fetchone()[0]
else:
    competitor_id = existing_competitors["HMS local"]

connection.commit()
cursor.close()
connection.close()


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
    product_id = int(replace_letters_with_ascii(product_details["product_id"]))

    if product_id not in existing_products:
        insert_product_query = """
                INSERT INTO public.products (product_id,competitor_id, category_id, product_name, scrapped_at, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s);
            """
        cursor.execute(insert_product_query,
                       (product_id, competitor_id, category_id, product_details["title"], datetime.now(),
                        datetime.now()))
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
        if existing_price != new_price:
            update_pricing_tier_query = """
                        UPDATE public.product_pricing_tiers
                        SET price_per_unit = %s, last_updated = %s
                        WHERE product_id = %s AND competitor_id = %s AND min_quantity = %s;
                    """
            cursor.execute(update_pricing_tier_query,
                           (new_price, datetime.now(), product_id, competitor_id, product_details["min_quantite"]))
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
        spec_name = ""
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


df = pd.read_csv('Articles de HMS.csv', delimiter=";", encoding="utf-8")
df = df[df['Libelle Nature d\'article'] == 'Marchandise']

for index, row in df.iterrows():
    product_details = {}
    if pd.notnull(row['Article']) and pd.notnull(
            row['Article Quantité min d\'achat']) and pd.notnull(row['Prix standard (prix catalogue unitaire)']):
        product_id = row['Article'].replace(".","").strip() + "7277"
        category_name = row['Groupe article']
        title = row["Désignation"]
        nbr_min = row["Article Quantité min d'achat"]
        prix_ttc = row["Prix standard (prix catalogue unitaire)"].replace(",",".")
        caracteristique = row["Désignation 2"]
        product_details = {
            'product_id': product_id,
            'category': category_name,
            'title': title,
            "min_quantite": nbr_min,
            "prix_ttc" : prix_ttc,
            "characteristics" : [{
                "name" : "",
                "value" : caracteristique
            }]
        }
        insert_into_database(product_details)

