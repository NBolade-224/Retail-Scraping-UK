import time, boto3, json, asyncio, psycopg, psycopg2, sys, openpyxl
from datetime import datetime
from requests_html import HTMLSession
import pandas as pd

if not sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class innitClass:
    def __init__(self,status,tablename):
        self.table = tablename if status == 'prod' else f"{tablename}testing"
        self.envStatus = True if status == 'prod' else False
        self.version = '0.1'
        self.duplicates = set()
        self.asession  = HTMLSession()
        self.date =  pd.to_datetime('today').strftime('%d%m%y')

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name="eu-west-1"
        )

        get_secret_value_response = client.get_secret_value(SecretId="RedshiftCon")
        secret = get_secret_value_response['SecretString']
        self.secret_dict = json.loads(secret)
        self.conninfo = f'host={self.secret_dict["awsRSep"]} port={5432} dbname={"postgres"} user={self.secret_dict["awsRSu"]} password={self.secret_dict["awsRSp"]}'
        
        self.connection = psycopg2.connect(
            database="postgres",
            user=self.secret_dict['awsRSu'],
            password=self.secret_dict['awsRSp'],
            host=self.secret_dict['awsRSep'],
            port='5432'
        )
        self.cursor = self.connection.cursor() # standard connection
        self.runNumber = self.getLastRun(self.table)
        self.cursor.close()
        self.connection.close()
    
    def getLastRun(self,tempTableName):
        self.cursor.execute(f"""
        select run_number from {tempTableName}
        GROUP BY run_number
        ORDER BY run_number DESC
        LIMIT 2""")
        result = self.cursor.fetchmany(2)
        try:
            runNumber = result[0][0] + 1
        except:
            runNumber = 1
        print(f'current Run - {runNumber}')
        return runNumber

    def sainsConversion(self,price):
        if price[0] == '£':
            return '%.2f' % float(price[1:])
        elif price[len(price)-1] == 'p':
            return "%.2f" % (float(price[:-1])*0.01)
        else:
            return price

    async def queryTable(self,tempTableName):
        async with await psycopg.AsyncConnection.connect(conninfo=self.conninfo) as aconn:
            async with aconn.cursor() as cur:
                await cur.execute(f"select * from {tempTableName}")
                result = await cur.fetchmany(10)
                print(result)

    async def addToTable(self,data):
        async with await psycopg.AsyncConnection.connect(conninfo=self.conninfo) as aconn:
            async with aconn.cursor() as cur:
                
                ## Query 1 - Create Temp Table
                await cur.execute(f"""
                    create global temp table temptable1 (
                        product varchar(200),
                        category varchar(25),
                        price varchar(25),
                        price_per_weight varchar(25),
                        price_date varchar(6),
                        product_url varchar(2000),
                        time_of_scrape varchar(50),
                        scrape_version varchar(50),
                        run_number integer
                    ) ON COMMIT DELETE ROWS""")
                print("Temp Table created")

                ## Query 2 - Add Data to Temp Table
                await cur.executemany(f"""insert into temptable1 
                        (product, category, price, price_per_weight, price_date, product_url, time_of_scrape, scrape_version, run_number) 
                        values(%s, %s, %s, %s, %s, %s, %s, %s, %s)""", data)
                print("Temp Insert Successfull")

                ## Query 3 - Copy Data from Temp Table to Main Table
                await cur.execute(f"""insert into {self.table} 
                                (product, category, price, price_per_weight, price_date, product_url, time_of_scrape, scrape_version, run_number)
                                SELECT product, category, price, price_per_weight, price_date, product_url, time_of_scrape, scrape_version, run_number
                                FROM temptable1""")
                print("Temp table copy to main table successful")
                
                ## Commit Changes and Close
                await aconn.commit()
                await aconn.close()
                print("Commit Successfull")

    async def scraper(self,currenCat,eachUrl):
        pageIndexNumber = self.startPageIndex
        data = []
        envStatus = True
        while envStatus == True:
            attempt = 1
            while attempt < 5:
                try:
                    page = self.asession.get(eachUrl.format(pageNumber=pageIndexNumber), timeout=5)
                    attempt = 100 
                except:
                    attempt = attempt +1
                    time.sleep(30)
                    print("Trying to connect again")

            products = page.html.find(self.productListSelector)
            if len(products) == 0:
                break

            for eachProduct in products:
                try:
                    Prod = eachProduct.find(self.productNameSelector)[0].text
                    print(Prod)
                    Cat = currenCat
                    price = eachProduct.find(self.productPriceSelector)[0].text
                    pricePerKilo = eachProduct.find(self.productPricePerKiloSelector)[0].text
                    for x in eachProduct.find(self.urlSelector)[0].absolute_links : url = x
                    date = str(self.date)
                    print(url)
                    price = self.sainsConversion(price)
                    print(price)
                    if Prod not in self.duplicates:
                        self.duplicates.add(Prod)
                        data.append((Prod,Cat,price,pricePerKilo,date,url,'timetest',self.version,self.runNumber))
                except:
                    print("Error")
                    continue

            pageIndexNumber += self.pageIndexIteration
            print(f"End of page {int(pageIndexNumber/self.pageIndexIteration)} of category {currenCat}")
            envStatus = self.envStatus
        await self.addToTable(data) ## shift to left

    async def couritine(self):
        coroutines = [self.scraper(cat, url) for cat, url in self.Urls.items()]
        await asyncio.gather(*coroutines)

    def main(self):
        if checkIfScheduleAlreadyDone(self.table,self.date) == True:
            asyncio.run(self.couritine())
            writeToExcelScheduler(self.table,self.date)
        else:
            print("Not run due to already being done")
            return

class SainsburysScraper(innitClass):
    def __init__(self,status):
        super().__init__(status,'sainsburys')
        self.productListSelector = ".gridItem"
        self.productNameSelector = ".productNameAndPromotions a"  
        self.urlSelector = ".productNameAndPromotions a"  
        self.productPriceSelector = ".pricePerUnit"
        self.productPricePerKiloSelector = ".pricePerMeasure"
        self.Urls = {
            "Bakery":"https://www.sainsburys.co.uk/shop/CategorySeeAllView?listId=&catalogId=10241&searchTerm=&beginIndex={pageNumber}&pageSize=120&orderBy=FAVOURITES_FIRST&top_category=&langId=44&storeId=10151&categoryId=12320&promotionId=&parent_category_rn=",
            "Fruit_and_Veg":"https://www.sainsburys.co.uk/shop/gb/groceries/fruit-veg/CategorySeeAllView?langId=44&storeId=10151&catalogId=10241&categoryId=12518&orderBy=FAVOURITES_FIRST&beginIndex={pageNumber}&promotionId=&listId=&searchTerm=&hasPreviousOrder=&previousOrderId=&categoryFacetId1=&categoryFacetId2=&ImportedProductsCount=&ImportedStoreName=&ImportedSupermarket=&bundleId=&parent_category_rn=&top_category=&pageSize=120#langId=44&storeId=10151&catalogId=10241&categoryId=12518&parent_category_rn=&top_category=&pageSize=120&orderBy=FAVOURITES_FIRST&searchTerm=&catSeeAll=true&beginIndex=0&categoryFacetId1=12518&categoryFacetId2=",
            "Meat_and_Fish":"https://www.sainsburys.co.uk/shop/gb/groceries/meat-fish/CategorySeeAllView?langId=44&storeId=10151&catalogId=10241&categoryId=13343&orderBy=FAVOURITES_FIRST&beginIndex={pageNumber}&promotionId=&listId=&searchTerm=&hasPreviousOrder=&previousOrderId=&categoryFacetId1=&categoryFacetId2=&ImportedProductsCount=&ImportedStoreName=&ImportedSupermarket=&bundleId=&parent_category_rn=&top_category=&pageSize=120#langId=44&storeId=10151&catalogId=10241&categoryId=13343&parent_category_rn=&top_category=&pageSize=120&orderBy=FAVOURITES_FIRST&searchTerm=&catSeeAll=true&beginIndex=0&categoryFacetId1=13343&categoryFacetId2=",
            "Dairys_Eggs_and_Chilled":"https://www.sainsburys.co.uk/shop/gb/groceries/dairy-eggs-and-chilled/CategorySeeAllView?langId=44&storeId=10151&catalogId=10241&categoryId=428866&orderBy=FAVOURITES_FIRST&beginIndex={pageNumber}&promotionId=&listId=&searchTerm=&hasPreviousOrder=&previousOrderId=&categoryFacetId1=&categoryFacetId2=&ImportedProductsCount=&ImportedStoreName=&ImportedSupermarket=&bundleId=&parent_category_rn=&top_category=&pageSize=120#langId=44&storeId=10151&catalogId=10241&categoryId=428866&parent_category_rn=&top_category=&pageSize=120&orderBy=FAVOURITES_FIRST&searchTerm=&catSeeAll=true&beginIndex=0&categoryFacetId1=428866&categoryFacetId2=", 
            "Frozen":"https://www.sainsburys.co.uk/shop/gb/groceries/frozen-/CategorySeeAllView?langId=44&storeId=10151&catalogId=10241&categoryId=218831&orderBy=FAVOURITES_FIRST&beginIndex={pageNumber}&promotionId=&listId=&searchTerm=&hasPreviousOrder=&previousOrderId=&categoryFacetId1=&categoryFacetId2=&ImportedProductsCount=&ImportedStoreName=&ImportedSupermarket=&bundleId=&parent_category_rn=&top_category=&pageSize=120#langId=44&storeId=10151&catalogId=10241&categoryId=218831&parent_category_rn=&top_category=&pageSize=120&orderBy=FAVOURITES_FIRST&searchTerm=&catSeeAll=true&beginIndex=0&categoryFacetId1=218831&categoryFacetId2=",
            "Food_Cupboard":"https://www.sainsburys.co.uk/shop/gb/groceries/food-cupboard/CategorySeeAllView?langId=44&storeId=10151&catalogId=10241&categoryId=12422&pageSize=120&beginIndex={pageNumber}&promotionId=&listId=&searchTerm=&hasPreviousOrder=&previousOrderId=&categoryFacetId1=12422&categoryFacetId2=&bundleId=&parent_category_rn=&top_category=&orderBy=NAME_ASC#langId=44&storeId=10151&catalogId=10241&categoryId=12422&parent_category_rn=&top_category=&pageSize=120&orderBy=NAME_ASC&searchTerm=&catSeeAll=true&beginIndex=0&categoryFacetId1=12422&categoryFacetId2=",        
            "Drinks":"https://www.sainsburys.co.uk/shop/gb/groceries/drinks/CategorySeeAllView?langId=44&storeId=10151&catalogId=10241&categoryId=12192&pageSize=120&beginIndex={pageNumber}&promotionId=&listId=&searchTerm=&hasPreviousOrder=%5BLjava.lang.String%3B%40302857f7&previousOrderId=&categoryFacetId1=&categoryFacetId2=&bundleId=&parent_category_rn=&top_category=&orderBy=NAME_ASC#langId=44&storeId=10151&catalogId=10241&categoryId=12192&parent_category_rn=&top_category=&pageSize=120&orderBy=NAME_ASC&searchTerm=&catSeeAll=true&beginIndex=0&categoryFacetId1=12192&categoryFacetId2=",
            "Household":"https://www.sainsburys.co.uk/shop/gb/groceries/household/CategorySeeAllView?langId=44&storeId=10151&catalogId=10241&categoryId=12564&pageSize=120&beginIndex={pageNumber}&promotionId=&listId=&searchTerm=&hasPreviousOrder=%5BLjava.lang.String%3B%40682fc624&previousOrderId=&categoryFacetId1=&categoryFacetId2=&bundleId=&parent_category_rn=&top_category=&orderBy=NAME_ASC#langId=44&storeId=10151&catalogId=10241&categoryId=12564&parent_category_rn=&top_category=&pageSize=120&orderBy=NAME_ASC&searchTerm=&catSeeAll=true&beginIndex=0&categoryFacetId1=12564&categoryFacetId2="
        }
        self.startPageIndex = 0
        self.pageIndexIteration = 120

class TescoScraper(innitClass):
    def __init__(self,status):
        super().__init__(status,'tesco')
        self.productListSelector = ".product-list--list-item"
        self.productNameSelector = ".ldbwMG"    
        self.urlSelector = "a"
        self.productPriceSelector = ".beans-price__text"
        self.productPricePerKiloSelector = ".beans-price__subtext"
        self.Urls = {
            "Bakery":"https://www.tesco.com/groceries/en-GB/shop/bakery/all?page={pageNumber}&count=48",
            "Fresh":"https://www.tesco.com/groceries/en-GB/shop/fresh-food/all?page={pageNumber}&count=48",
            "Frozen":"https://www.tesco.com/groceries/en-GB/shop/frozen-food/all?page={pageNumber}&count=48",
            "Food_Cupboard":"https://www.tesco.com/groceries/en-GB/shop/food-cupboard/all?page={pageNumber}&count=48",
            "Drinks":"https://www.tesco.com/groceries/en-GB/shop/drinks/all?page={pageNumber}&count=48",
            "Household":"https://www.tesco.com/groceries/en-GB/shop/household/all?page={pageNumber}&count=48",
        }
        self.startPageIndex = 1
        self.pageIndexIteration = 1

def snsAlert(table, msg):
    body = f"{table} {msg}"
    client = boto3.client('sns')
    client.publish (
        TargetArn = "arn:aws:sns:eu-west-1:246968638326:Topic1",
        Message = json.dumps({'default': body}),
        MessageStructure = 'json'
    )

def handler(event, context):
    try:
        start_time = time.time()
        env = event['env']
        tableName = event['table']
        if tableName == 'sainsburys':
            SainsburysScraper(env).main()
        elif tableName == 'tesco':
            TescoScraper(env).main()
        else:
            snsAlert(tableName,'Unknown Table')
            return
        
        msg = "success"
        snsAlert(tableName,msg)
        print(time.time() - start_time)
        return f'Scrape complete and took {time.time() - start_time} seconds'
    
    except Exception as error:
        print(f"Exception Triggered - {error}")
        msg = f" ***FAILED*** - {error}"
        snsAlert(tableName,msg)

def loadWorkBook(table):
    # Assign Column Numbers
    assignedColumns = {'tesco':2,'sainsburys':3}
    columnNumber = assignedColumns[table]

    # Load Workbook
    wb = openpyxl.load_workbook('C:\\Users\\nickb\\Desktop\\New folder\\PROJECTS\\RETAIL SCRAPING\\lambdaScraper\\Schedule.xlsx')
    ws = wb.active
    return (wb,ws,columnNumber)

def writeToExcelScheduler(table,date):
    wb,ws,columnNumber = loadWorkBook(table)
    # Loop through A column to find row of todays date
    cleanDate = datetime.strptime(date,'%d%m%y').strftime('%Y-%m-%d')
    for x in range(2,10):
        excelDate = ws.cell(row=x, column=1).value.date()
        if str(cleanDate) == str(excelDate):
            ws.cell(row=x, column=columnNumber).value = "X"
    # Save workbook
    wb.save('C:\\Users\\nickb\\Desktop\\New folder\\PROJECTS\\RETAIL SCRAPING\\lambdaScraper\\Schedule.xlsx')

def checkIfScheduleAlreadyDone(table,date):
    wb,ws,columnNumber = loadWorkBook(table)
    # Loop through A column to find row of todays date
    cleanDate = datetime.strptime(date,'%d%m%y').strftime('%Y-%m-%d')
    for x in range(2,10):
        excelDate = ws.cell(row=x, column=1).value.date()
        if str(cleanDate) == str(excelDate):
            if ws.cell(row=x, column=columnNumber).value == "X":
                return False
    # Save workbook
    wb.save('C:\\Users\\nickb\\Desktop\\New folder\\PROJECTS\\RETAIL SCRAPING\\lambdaScraper\\Schedule.xlsx')
    return True

###########################################################################################################################################
## For Local Testing (comment out when deploying)
def testing():
    start_time = time.time()
    SainsburysScraper('prod').main()
    TescoScraper('prod').main()
    print(time.time() - start_time)
testing()

#############################################################################################################################################
#############################################################################################################################################
#############################################################################################################################################

## Notes

## Total Products for Sainsburys = 17000
## Total Products for Tesco = 14634

## Timing to complete Lambda
##  10min for Sainsburys
##  12.5min for Tesco

## AWS Timings
## 1716mb = 68 secs (same as local) and equivalent to 1vcpu
## 1716mb = 304 secs (same as local) and equivalent to 1vcpu, with max memory usage of 208 MB
## 1716mb = 486 secs (same as local) and equivalent to 1vcpu, with max memory usage of 208 MB
