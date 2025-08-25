from fastapi import FastAPI
from publisher import Publisher
from get_data import newsgroups_interesting, newsgroups_not_interesting
import uvicorn


app = FastAPI()
pub = Publisher()
@app.get("/produce1")
async def get_producer_with_tofic_1():
    producer = pub.get_producer_config()
    pub.publish_message(producer,pub.topic1,pub.event1)
    return {"status": "message sent", "topic": pub.topic1}

@app.get("/produce2")
async def get_producer_with_tofic_2():
    producer = pub.get_producer_config()
    pub.publish_message(producer,pub.topic2,pub.event2)
    return {"status": "message sent", "topic": pub.topic2}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)