from fastapi import FastAPI
from src.api.routers import ingestion, extraction, rag

app = FastAPI(title="Auction Notice Data Extraction Pipeline API", version="0.0.1")

app.include_router(ingestion.router)
app.include_router(extraction.router)
app.include_router(rag.router)

@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}
