from fastapi import FastAPI
from app.api.endpoints import dashboard

app = FastAPI(title="Vet Animal Wellness API")

app.include_router(dashboard.router, prefix="/api/dashboard", tags=["dashboard"])

@app.get("/")
def read_root():
    return {"Hello": "World", "Project": "Vet Animal Wellness Backend"}

@app.get("/health")
def health_check():
    return {"status": "ok"}
