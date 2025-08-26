from fastapi import FastAPI


from subskriber import Subskriber
app = FastAPI()
sub = Subskriber()

@app.get("/consumer1")
async def get_consumer_with_topic_1():
    messages=sub.consumer_messages(n=10)
    return {"status": "message consumed", "topic": sub.topic,"messages": messages}



