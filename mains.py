from fastapi import FastAPI, BackgroundTasks, Query
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import asyncio
import httpx
from pymongo.errors import DuplicateKeyError
from typing import Optional

app = FastAPI()

# MongoDB setup
MONGO_URI = "mongodb://localhost:27017"  # Use env var in Render
client = AsyncIOMotorClient(MONGO_URI)
db = client.news_db
collection = db.articles

# Ensure unique index on 'link'
async def ensure_indexes():
    await collection.create_index("link", unique=True)

# Scraper functions
async def scrape_the_hindu():
    try:
        print("[+] Scraping The Hindu...")
        url = "https://www.thehindu.com/news/national/"
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        articles = []
        for item in soup.select(".story-card-news"):
            title_tag = item.find("a")
            if title_tag:
                article = {
                    "title": title_tag.text.strip(),
                    "link": title_tag["href"],
                    "source": "The Hindu",
                    "scraped_at": datetime.utcnow()
                }
                articles.append(article)
        if articles:
            await collection.insert_many(articles, ordered=False)
            print(f"[The Hindu] Inserted {len(articles)} articles.")
    except DuplicateKeyError:
        print("[The Hindu] Duplicate entries skipped.")
    except Exception as e:
        print(f"[Error] The Hindu scraping failed: {e}")

async def scrape_times_of_india():
    try:
        print("[+] Scraping Times of India...")
        url = "https://timesofindia.indiatimes.com/india"
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        articles = []
        for item in soup.select(".w_tle a"):
            article = {
                "title": item.text.strip(),
                "link": f"https://timesofindia.indiatimes.com{item['href']}",
                "source": "Times of India",
                "scraped_at": datetime.utcnow()
            }
            articles.append(article)
        if articles:
            await collection.insert_many(articles, ordered=False)
            print(f"[TOI] Inserted {len(articles)} articles.")
    except DuplicateKeyError:
        print("[TOI] Duplicate entries skipped.")
    except Exception as e:
        print(f"[Error] Times of India scraping failed: {e}")

async def scrape_ani_news():
    try:
        print("[+] Scraping ANI News...")
        url = "https://www.aninews.in/category/national/"
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        articles = []
        for item in soup.select(".category-news-list .news-title"):
            link = item.find("a")
            if link:
                article = {
                    "title": link.text.strip(),
                    "link": f"https://www.aninews.in{link['href']}",
                    "source": "ANI",
                    "scraped_at": datetime.utcnow()
                }
                articles.append(article)
        if articles:
            await collection.insert_many(articles, ordered=False)
            print(f"[ANI] Inserted {len(articles)} articles.")
    except DuplicateKeyError:
        print("[ANI] Duplicate entries skipped.")
    except Exception as e:
        print(f"[Error] ANI scraping failed: {e}")

# Master scraping function
async def scrape_all_sources():
    await scrape_the_hindu()
    await scrape_times_of_india()
    await scrape_ani_news()

# Background scheduler
scheduler = BackgroundScheduler()

def start_scheduler():
    loop = asyncio.get_event_loop()
    scheduler.add_job(lambda: asyncio.run_coroutine_threadsafe(scrape_all_sources(), loop),
                      trigger=IntervalTrigger(hours=1))
    scheduler.start()

@app.on_event("startup")
async def startup_event():
    print("[*] Starting scheduler and setting up indexes...")
    await ensure_indexes()
    start_scheduler()

@app.on_event("shutdown")
def shutdown_event():
    print("[*] Shutting down scheduler...")
    scheduler.shutdown()

# API Endpoints
@app.get("/news/latest")
async def get_latest_news(limit: int = 10):
    cursor = collection.find().sort("scraped_at", -1).limit(limit)
    return await cursor.to_list(length=limit)

@app.get("/news/by_source")
async def get_news_by_source(source: str, limit: int = 10):
    cursor = collection.find({"source": source}).sort("scraped_at", -1).limit(limit)
    return await cursor.to_list(length=limit)

@app.get("/news/search")
async def search_news(
    keyword: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: int = 10
):
    query = {}

    if keyword:
        query["title"] = {"$regex": keyword, "$options": "i"}
    
    if from_date or to_date:
        date_query = {}
        if from_date:
            date_query["$gte"] = datetime.fromisoformat(from_date)
        if to_date:
            date_query["$lte"] = datetime.fromisoformat(to_date)
        query["scraped_at"] = date_query

    cursor = collection.find(query).sort("scraped_at", -1).limit(limit)
    return await cursor.to_list(length=limit)

@app.post("/scrape")
async def trigger_scrape(background_tasks: BackgroundTasks):
    background_tasks.add_task(scrape_all_sources)
    return {"message": "Scraping started in background"}
