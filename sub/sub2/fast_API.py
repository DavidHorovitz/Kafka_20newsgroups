from fastapi import FastAPI
import uvicorn

from subskriber import Subskriber
app = FastAPI()
sub = Subskriber()

@app.get("/consumer2")
async def get_consumer_with_topic_1():
    messages=sub.consumer_messages(n=10)
    return {"status": "message consumed", "topic": sub.topic,"messages": messages}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
