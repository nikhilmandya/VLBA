import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
import pandas as pd
import json
from pandas.io.json import json_normalize
from tweepy.streaming import StreamListener
from datetime import datetime
import time

ck="6sSDsk1kDtl05Oh5k6oQiYZTK"
cs="NakCIC8ntYk0iPCsl7qErFuNMsbU0LEmmdcNVhrr90giv2R92K"
at="2793027104-AiQ4nBQaydifbrTaJm46dLNVThg6NdXr3Q7H6fu"
ats="49l4sVDinAnRWFNfeFe5TR3m20AI5MJcFIWpruDsJEd8X"




auth=OAuthHandler(ck,cs)


auth.set_access_token(at,ats)



dd=[]
class listener(StreamListener):
    def __init__(self, time_limit=60):
        self.start_time = time.time()
        self.limit = time_limit
       
        super(listener, self).__init__()
    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:

            all_data = json.loads(data)
        
            tweet = all_data["text"]
        
            
            dd.append(tweet)
            
            
    
        

          
        
            return True
        else:
            
            return False

    def on_error(self, status):
        print(status)

st=Stream(auth,listener(time_limit=20))
st.filter(track=["politics"])
df=pd.DataFrame(dd)
df.to_csv('/home/nikhil/airflow/script/Politics/politics.csv')
