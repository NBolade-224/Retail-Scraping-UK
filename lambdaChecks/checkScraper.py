import boto3, json, psycopg2
import pandas as pd

class mainClass:
    def __init__(self):
        self.date =  pd.to_datetime('today').strftime('%d%m%y')

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name="eu-west-1"
        )

        get_secret_value_response = client.get_secret_value(SecretId="RedshiftCon")
        secret = get_secret_value_response['SecretString']
        self.secret_dict = json.loads(secret)

        self.connection = psycopg2.connect(
            database="postgres",
            user=self.secret_dict['awsRSu'],
            password=self.secret_dict['awsRSp'],
            host=self.secret_dict['awsRSep'],
            port='5432'
        )
        self.cursor = self.connection.cursor() 

    def getTotalProductForLastRun(self,TableName):
        self.cursor.execute(f"""
        select COUNT(product) from {TableName}
        WHERE PRICE_DATE = '{self.date}'""")
        result = self.cursor.fetchone()
        try:
            total = result[0]
        except:
            total = 0

        self.cursor.close()
        self.connection.close()
        return total
    
def snsAlert(total,table):
    body = f"Less than expected products Scraped: {table} - {total}"
    client = boto3.client('sns')
    client.publish (
        TargetArn = "arn:aws:sns:eu-west-1:246968638326:Topic1",
        Message = json.dumps({'default': body}),
        MessageStructure = 'json'
    )

def handler(event, context):
    try:
        table = event['table']
        total = mainClass().getTotalProductForLastRun(table)
        if total < 15000:
            snsAlert(total,table)
        return total
    except Exception as error:
        print(f"Exception Triggered - {error}")
        msg = f" ***FAILED*** {error}"
        snsAlert(0,msg)



#############################################################################################################################################
#############################################################################################################################################
#############################################################################################################################################

## For Local Testing
def timeTest():
    try:
        table = 'sainsburys'
        total = mainClass().getTotalProductForLastRun(table)
        if total < 15000:
            snsAlert(total,table)
            print(f"Total {total} less than expected, SNS sent")
        else:
            print(f"Total is fine {total}")
    except Exception as error:
        print(f"Exception Triggered - {error}")
        msg = f" ***FAILED*** {error}"
        snsAlert(0,msg)

# timeTest()
