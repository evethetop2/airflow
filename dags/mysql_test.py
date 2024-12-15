import pymysql
import pandas as pd
import json

conn = pymysql.connect(host='127.0.0.1', user='root', password='gksdud12', db='hyperconnect', charset='utf8')
cur = conn.cursor()
sql  = 'select * from table_a'
cur.execute(sql)
result=cur.fetchall()
df = pd.DataFrame(result, columns=['dt','hr','value'])
print(df.info())
df[['id', 'user_name']] = df['value'].apply(lambda x: pd.Series(json.loads(x)))
df.drop('value', axis=1, inplace=True)
print(df)

conn.close()