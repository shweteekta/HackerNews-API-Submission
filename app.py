from flask import Flask, request
from pymongo.server_api import ServerApi
import time

app = Flask(__name__)
port = 5000
import requests
import json
from pymongo import MongoClient
from bson.json_util import dumps

client = MongoClient("mongodb+srv://admin:admin@cluster0.sx5hfvo.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))
db = client["Hackernews"]
news_collection = db["news"]
timestamp = db["Timestamp"]
previous_news = db["Previous"]
comments_collection = db['comments']

@app.route("/top-stories")
def getTopStories():
    checkCache()
    news_data = news_collection.find().sort("score", -1).limit(10)
    return convertCursorToJson(news_data)

@app.route("/past-stories")
def getPastStories():
    return convertCursorToJson(previous_news.find())

@app.route("/comments/<story_id>")
def getComments(story_id):
    return fetchComments(story_id)

def getAPI():
    if news_collection.count_documents({}) != 0:
        query = news_collection.find().sort("score", -1).limit(10)
        previous_news.drop()
        previous_news.insert_many(query)
        news_collection.drop()
    ts = time.time()
    timestamp.drop()
    Time = { "timestamp" : ts}
    timestamp.insert_one(Time)
    parse_json = getRequest('https://hacker-news.firebaseio.com/v0/topstories.json?print=pretty')
    for i in list(parse_json):
         pjson = getRequest("https://hacker-news.firebaseio.com/v0/item/" + str(i) + ".json?print=pretty")
         if pjson["type"] == "story":
            print("story", pjson['id'])
            stories ={"title":pjson['title'],"url":pjson.get("url",""),"score" :pjson["score"],"time":pjson["time"],"user" : pjson["by"] }
            news_collection.insert_one(stories)
    return

def fetchComments(story_id):
    url = "https://hacker-news.firebaseio.com/v0/item/" + str(story_id) + ".json?print=pretty"
    cjson = getRequest(url)
    result = []
    for i in cjson['kids']:
        eachResponse = getRequest("https://hacker-news.firebaseio.com/v0/item/" + str(i)+ ".json?print=pretty")
        comments = {"Id":eachResponse['id'],"No of child comments": len(eachResponse.get("kids","")), "Text": eachResponse.get("text",""), "Hacker News Handle": cjson["by"]}
        result.append(comments)
    return sorted(result, key= lambda x: x["No of child comments"], reverse=True)[:10]

def checkCache():
    tdocs = timestamp.count_documents({})
    ts = time.time()
    count = timestamp.find_one()
    print(count['timestamp'] - ts )
    if tdocs == 0 or abs(ts- count['timestamp']) > 900 :
        getAPI()
    return

def getRequest(url):
    session = requests.Session()
    getJson = json.loads(session.get(url).text)
    return getJson

def convertCursorToJson(data):
    list_cur = list(data)
    news_data = dumps(list_cur, indent=2)
    return news_data

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=port)
