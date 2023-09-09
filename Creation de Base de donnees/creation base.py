import psycopg2

db_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "root",
    "host": "localhost",
}

new_db_name = "hotel_megastore_competition"

def create_tables():
    try:
        conn = psycopg2.connect(**new_db_params)

        cursor = conn.cursor()

        create_competitors_table = """
        CREATE TABLE competitors (
            competitor_id SERIAL PRIMARY KEY,
            competitors_name VARCHAR(255) NOT NULL,
            website VARCHAR(255),
            last_scrapped DATE
        );
        """

        create_product_categories_table = """
        CREATE TABLE product_categories (
            category_id SERIAL PRIMARY KEY,
            category_name VARCHAR(255) NOT NULL
        );
        """

        create_products_table = """
        CREATE TABLE products (
            product_id NUMERIC PRIMARY KEY,
            category_id INTEGER NOT NULL,
            competitor_id INTEGER,
            product_name VARCHAR(255),
            scrapped_at DATE,
            last_updated DATE
        );
        """

        create_product_specifications_table = """
        CREATE TABLE product_specifications (
            specification_id SERIAL PRIMARY KEY,
            product_id NUMERIC,
            competitor_id INTEGER,
            specification_name VARCHAR(255) NOT NULL,
            value VARCHAR(255),
            scrapped_at DATE,
            last_updated DATE
        );
        """

        create_product_pricing_tiers_table = """
        CREATE TABLE product_pricing_tiers (
            pricing_tier_id SERIAL PRIMARY KEY,
            product_id NUMERIC,
            competitor_id INTEGER,
            min_quantity INTEGER,
            price_per_unit NUMERIC,
            scrapped_at DATE,
            last_updated DATE
        );
        """

        create_changed_prices_table = """
        CREATE TABLE changed_prices (
            product_id NUMERIC,
            pricing_tier_id NUMERIC,
            old_price NUMERIC,
            new_price NUMERIC,
            PRIMARY KEY (product_id, pricing_tier_id)
        );
        """

        cursor.execute(create_competitors_table)
        cursor.execute(create_product_categories_table)
        cursor.execute(create_products_table)
        cursor.execute(create_product_specifications_table)
        cursor.execute(create_product_pricing_tiers_table)
        cursor.execute(create_changed_prices_table)

        conn.commit()
        cursor.close()
        conn.close()

        print("Tables created successfully.")

    except psycopg2.DatabaseError as e:
        print(f"Error creating tables: {e}")
    finally:
        if conn is not None:
            conn.close()

conn = psycopg2.connect(**db_params)
conn.autocommit = True

cursor = conn.cursor()

cursor.execute(f"CREATE DATABASE {new_db_name}")

cursor.close()
conn.close()

new_db_params = db_params.copy()
new_db_params["dbname"] = new_db_name

conn = psycopg2.connect(**new_db_params)

create_tables()

conn.close()
