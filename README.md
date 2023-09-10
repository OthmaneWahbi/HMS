## Project Overview

This project consists of several components for data scraping, database creation, and product categorization. Each component is organized into its respective folder:

- **Categories Clustering**: This directory contains the script to perform product categorization and clustering.

- **Creation de Base de Donnees**: This directory contains the script to create the database and necessary tables.

- **Hotel Megastore**: This directory contains two Python scripts for data acquisition from the Hotel Megastore website. One script fetches data directly from the website and updates the database, while the other reads data from a local file.

- **Spiders**: This directory contains web scraping spiders, with each spider targeting a different competitor's website based on its name.
## Database Setup
**ATTENTION !**

Before running any other script, it's essential to create the database and the required tables. In the "Creation de Base de Donnees" folder, you will find a Python script that is used to create the necessary database and tables. To run this script, execute it as a normal Python script:

## Running the Spiders

To run the spiders located in the "Spiders" folder, you can use the Scrapy framework. Each spider scrapes data from a different competitor's website. Here's how to run a spider:

```bash
scrapy runspider Spiders/spidername.py
```

Replace spidername.py with the name of the spider you want to run.

## Running the Hotel Megastore Scrapers
In the "Hotel Megastore" folder, you will find two Python scripts:

- **hotelmegastore scraper.py** : This script scrapes data from the Hotel Megastore website and updates the database with the fetched data.
- **insert_hms_local.py** : This script reads data from a local file (presumably "Articles de HMS.csv") and performs data insertion into the database.

To run either of these scripts, you can execute them as normal Python scripts:

```bash
python hotelmegastore scraper.py
```
OR
```bash
python insert_hms_local.py
```
## Database Creation
In the "Creation de Base de Donnees" folder, you will find a Python script that is used to create the necessary database and tables. To run this script, execute it as a normal Python script:
```bash
python creation_base.py
```
## Categories Clustering
The "Categories Clustering" component is responsible for gathering data from the database and performing product categorization and clustering. It collects product information, prices, and other relevant data to create 150 clusters. 
Feel free to reach out if you have any questions or need further assistance with running any part of the project.

Running the script in the folder mentionned creates two files.
raw_data.csv with all the products and other relevant information in the database, and after the processing we have the named_clustered.xlsx file which is an excel file with the products clustered.






