#!/usr/bin/env python
# coding: utf-8

# In[4]:


import tweepy 
from tweepy import OAuthHandler, Stream


# In[7]:


from tweepy.streaming import StreamListener
import socket 
import json


# In[8]:


consumer_key = "your-consumer-key"
consumer_secret = "your-consumer-secret"
access_token = "your-access-token"
access_secret = "your-access-secret"


# In[9]:


class TweetListener(StreamListener):

    def __init__(self,csocket):
        self.client_socket = csocket

    def on_data(self,data):

        try:
            msg = json.loads(data)#create a message coming from json file
            print(msg["text"].encode("utf-8"))# if there is a emoji utf-8 convert it as a blank, without utf 8 error
            self.client_socket.send(msg["text"].encode("utf-8"))
            return True
        except BaseException as e :
            print ("ERROR %s" %e)
        return True

    def on_error(self,status):
        print(status)
        return True


# In[10]:


def sendData(c_socket):## to setup our connection
    auth = OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_token,access_secret)
    
    api = tweepy.API(auth)
    twitter_stream = Stream(api.auth,TweetListener(c_socket))
    twitter_stream.filter(track=["python"])


# In[ ]:


## sendData connecting to twitter, twitter_stream filter everything by a certain string


# In[ ]:


if __name__ == "__main__":
    s = socket.socket()
    host = "127.0.0.1"
    port = 3333
    s.bind((host,port))

    print("listening on port: %s" %port)
    s.listen(5)
    c,addr = s.accept()
    
    print("Received request from: " + str(addr))
    sendData(c)


# In[ ]:





# In[ ]:





# In[ ]:




