import psycopg2, boto3, json
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name="eu-west-1"
)

get_secret_value_response = client.get_secret_value(SecretId="RedshiftCon")
secret = get_secret_value_response['SecretString']
secret_dict = json.loads(secret)

connection = psycopg2.connect(
    database="postgres",
    user=secret_dict['awsRSu'],
    password=secret_dict['awsRSp'],
    host=secret_dict['awsRSep'],
    port='5432'
)

app = FastAPI()
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def returnIndex():
    return FileResponse('./webPage/index.html')

@app.get("/js")
def returnAsset():
    return FileResponse(f'./webPage/index.js')

@app.get("/css")
def returnAsset():
    return FileResponse(f'./webPage/index.css')

@app.get("/apiEndpoint")
def apiReturn(searchtable,input):
    query = queryTable(searchtable)
    params = (input,)
    dataframe = pd.read_sql_query(query, connection, params=params)
    print(dataframe)
    try:
        productName = dataframe['product'].values[0]
        productPrice = dataframe['price'].values.tolist()
        productDate = dataframe['price_date'].values.tolist()
        return (productPrice,productDate,productName)
    except:
        return ([0],[0],"No Data Found")

def queryTable(searchtable):
    if searchtable == 'Sainsburys':
        return """
            select product, price, price_date
            from sainsburys
            where product = %s
            order by price_date asc
        """ 
    elif searchtable == 'Tesco':
        return """
            select product, price, price_date 
            from Tesco
            where product = %s
            order by price_date asc
        """ 
    else:
        return "blank"
