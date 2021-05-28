from requests_html import HTMLSessifrom requests_html import HTMLSession
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("scrap_mobilede")\
    .config("spark.jars", "/Users/shahulmhd/PycharmProjects/mysql-connector-java-8.0.21/mysql-connector-java-8.0.21.jar")\
    .getOrCreate()


def create_webdriver():
    chrome_options = Options()
    chrome_options.use_chromium = True
    chrome_options.headless = True
    driver = webdriver.Chrome(ChromeDriverManager().install())
    return driver


for pageNum in range(1, 4):
    session = HTMLSession()
    url = f"""
        https://suchen.mobile.de/fahrzeuge/search.html?damageUnrepaired=NO_DAMAGE_UNREPAIRED&grossPrice=true&isSearchRequest=true&makeModelVariant1.makeId=17200&makeModelVariant1.modelId=8&maxMileage=150000&maxPrice=17500&minFirstRegistrationDate=2016-01-01&pageNumber={pageNum}&scopeId=C&sfmr=false"""\
        .format(pageNum)

    request = session.get(url)
    request.html.render(sleep=1)
    soup = BeautifulSoup(request.text, "html.parser")
    carResultList = soup.find_all('div', class_='cBox-body cBox-body--resultitem dealerAd rbt-reg rbt-no-top')

eachCarPageLinks = []
for item in carResultList:
    for link in item.find_all('a', href=True):
        eachCarPageLinks.append(link['href'])

selectedFinalCarList = []
for link in eachCarPageLinks:
    session = HTMLSession()
    request = session.get(link)
    request.html.render(sleep=1)
    soup = BeautifulSoup(request.text, "html.parser")
    # ------------------------------------------------
    # date_annonce
    car_name = soup.find('h1').text
    # reviews
    price = soup.find('span', class_='h3 rbt-prime-price').text
    mileage = soup.find("div", {"id": "rbt-mileage-v"}).text
    categorie = soup.find("div", {"id": "rbt-category-v"}).text
    power = soup.find("div", {"id": "rbt-power-v"}).text
    fuel = soup.find("div", {"id": "rbt-fuel-v"}).text
    try :
        emmission_co2 = soup.find("div", {"id": "rbt-envkv.emission-v"}).text
    except :
        emmission_co2 = "not available"
    try :
        classe_energie = soup.find("div", {"id": "rbt-envkv.efficiencyClass-v"}).text
    except :
        classe_energie = "no available"
    date_premiere_circulation = soup.find("div", {"id": "rbt-firstRegistration-v"}).text
    lieu_geographique = soup.find("p", {"id": "rbt-db-address"}).text
    # note_handler = soup.find('span', {"class" :'star-rating-s u-valign-middle u-margin-right-9'}, {"data-rating"}).text

    """
    divTag = soup.find_all("div", {"id": "rbt-envkv.consumption-v"}, )
    for tag in divTag:
        consommation = []
        tdTags = tag.find_all("div", {"class": "u-margin-bottom-9"})
            for conso in tdTags:
                consommation.append(conso.text)
            print(consommation)
    A REVOIR CETTE PARTIE POUR RECUPERER UNE SEULE CONSO OU SEPAREMENT DANS CHAQUE COLONNE 
    """

    car_details = {
        'car_name': car_name,
        'price': price,
        'mileage': mileage,
        'categorie': categorie,
        'power': power,
        'fuel': fuel,
        # 'consommation' : consommation,
        'emmission_co2': emmission_co2,
        'classe_energie': classe_energie,
        'date_premiere_circulation': date_premiere_circulation,
        'lieu_geographique': lieu_geographique,
        # 'note_handler': note_handler
    }
    selectedFinalCarList.append(car_details)


# Create data frame
selectedFinalCarListDF = spark.createDataFrame(selectedFinalCarList)
#selectedFinalCarListDF.show()


selectedFinalCarListDF.write.format('jdbc')\
    .options(
    url='jdbc:mysql://localhost/automobile',
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='mobile_de',
    user='root',
    password='azertyuiop')\
    .mode("overwrite" )\
    .save()


