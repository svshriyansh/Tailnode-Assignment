from bs4 import BeautifulSoup
import requests
from pymongo import MongoClient
import certifi
import sys

def get_database():

    CONNECTION_STRING = "mongodb+srv://Shriyansh:eb0Q3dK7SPn0iSqM@cluster0.gpzb5.mongodb.net/tailnode?retryWrites=true&w=majority"
    client = MongoClient(CONNECTION_STRING, tlsCAFile = certifi.where())
    print("Database Connected")
    return client['tailnode']
    
def delete_collecion(collection_name):
    print(f'dropping collection...')
    collection_name.drop({})

def store_userData(data,table):
    table.insert_many(data)

def book_data(soup):
    obj = []
    ol = soup.find('ol',{"class":"row"})
    i = 1
    list_ol = ol.find_all("li")
    for li in list_ol:
        arr = {}
        h3 = li.find("h3")
        a = h3.find("a")
        book_name = a["title"]
        price = li.find("p",{"class":"price_color"})
        price = (price.text)
        texts = li.find("p",{"class":"instock availability"}) 
        stock = (texts.text.strip())
        rateClass = li.find("p")
        rate = rateClass['class'][1]
        # print(rate)
        arr['Name'] = book_name
        arr['Price'] = price
        arr['Avalibality'] = stock
        arr['rating'] = rate
        obj.append(arr)
        i += 1
    return obj

def scrap(collection):
    start = 1
    last_page = 51
    table = collection['Book data']
    delete_collecion(table)
    max_bars_count = 20
    spaces = f"%-{max_bars_count}s"
    for i in range(start,last_page):
        count = last_page- start
        j = i / count
        sys.stdout.write('\r')
        sys.stdout.write(f"{spaces} %d%%" % ('#'*int(j*max_bars_count), 100*j))
        sys.stdout.flush()
        no = i
        url = f'http://books.toscrape.com/catalogue/page-{no}.html'
        response = requests.get(url)
        soup = BeautifulSoup(response.text,"html5lib")
        data = book_data(soup)
        store_userData(data,table)
    print("\nProcess Completed")


if __name__ == "__main__":
    collection = get_database()
    scrap(collection)    
    
