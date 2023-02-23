from flask import Flask, request
from pymongo.server_api import ServerApi
import time
import requests
import json
from pymongo import MongoClient
from bson.json_util import dumps

app = Flask(__name__)
port = 5000
client = MongoClient("mongodb+srv://admin:admin@cluster0.sx5hfvo.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))
db = client["Hackernews"]
news_collection = db["news"]
timestamp_collection = db["Timestamp"]
previous_news = db["Previous"]
comments_collection = db['comments']

@app.route("/")
def main():
    return "Use /top-stories for finding top 10 stories based on score, /past-stories for previous loaded top 10 stories and /comments to check the top 10 comments"

@app.route("/top-stories")
def getTopStories():
    checkCache()
    # Sorting the news data based on the top 10 scores
    news_data = news_collection.find().sort("score", -1).limit(10)
    return convertCursorToJson(news_data)

@app.route("/past-stories")
def getPastStories():
    return convertCursorToJson(previous_news.find())

@app.route("/comments/<story_id>")
def getComments(story_id):
    return fetchComments(story_id)

async def getAPI():
    # if there are items inside the news collection then put it to past stories and clear the current collection
    if news_collection.count_documents({}) != 0:
        query = news_collection.find().sort("score", -1).limit(10)
        previous_news.drop()
        previous_news.insert_many(query)
        news_collection.drop()
    # Update the timestamp collection with current time
    ts = time.time()
    timestamp_collection.drop()
    Time = { "timestamp" : ts}
    timestamp_collection.insert_one(Time)
    #fetch all the ids for the top stories
    parse_json = getRequest('https://hacker-news.firebaseio.com/v0/topstories.json?print=pretty')
    for i in list(parse_json):
        #fetch each items from top stories and store the required elements from stories in the news_collection
         pjson = getRequest("https://hacker-news.firebaseio.com/v0/item/" + str(i) + ".json?print=pretty")
         # Since the type can be job as well so check for the story type.
         if pjson["type"] == "story":
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
    # Sorting the comments which are stored in the list based on the number of child comments and getting top 10
    return sorted(result, key= lambda x: x["No of child comments"], reverse=True)[:10]

def checkCache():
    tdocs = timestamp_collection.count_documents({})
    ts = time.time()
    count = timestamp_collection.find_one()
    # Checking whether the database is empty or the time difference is greater than 15 mins
    if tdocs == 0 or abs(ts- count['timestamp']) > 900 :
        getAPI()
    return

async def getRequest(url):
    # Getting the response from the requests.
    session = requests.Session()
    getJson = json.loads(session.get(url).text)
    return getJson

def convertCursorToJson(data):
    # Converted cursor format data of mongodb to json
    list_cur = list(data)
    news_data = dumps(list_cur, indent=2)
    return news_data

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=port)
