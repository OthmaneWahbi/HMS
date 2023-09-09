## Project Overview

This project consists of several components for data scraping, database creation, and product categorization. Each component is organized into its respective folder:

- **Categories Clustering**: This directory contains the script to perform product categorization and clustering.

- **Creation de Base de Donnees**: This directory contains the script to create the database and necessary tables.

- **Hotel Megastore**: This directory contains two Python scripts for data acquisition from the Hotel Megastore website. One script fetches data directly from the website and updates the database, while the other reads data from a local file.

- **Spiders**: This directory contains web scraping spiders, with each spider targeting a different competitor's website based on its name.

## Running the Spiders

To run the spiders located in the "Spiders" folder, you can use the Scrapy framework. Each spider scrapes data from a different competitor's website. Here's how to run a spider:

```bash
scrapy runspider Spiders/spidername.py
