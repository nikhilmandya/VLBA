import pandas as pd
import textblob
from datetime import datetime
df=pd.read_csv("/home/nikhil/airflow/script/Politics/politics.csv")
polarity=[]
subjectivity=[]
for i in range(0,len(df['0'])):
	print(df['0'][i])
	polarity.append(textblob.TextBlob(str(df['0'][i])).sentiment[0])
	subjectivity.append(textblob.TextBlob(str(df['0'][i])).sentiment[1])
df['polarity']=polarity
df['subjectivity']=subjectivity
df.to_csv("/home/nikhil/airflow/script/Politics/politics.csv")
