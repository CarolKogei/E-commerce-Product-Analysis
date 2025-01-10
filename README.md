# Webscraping

Webscraping url = https://www.jumia.co.ke/
Webscraping products
    * TVs
    * Cookers

***Packages***
* BeautifulSoup
* Requests


save to csv
clean and save, then upload to db

or save directly to db

# pip install -r requirements.txt

Naming conventions:
- data files
{website}_product.csv

- scripts:
{website}_scrape/clean_{product}.ipynb

- dags
{website}_scrape/clean_{product}.py
