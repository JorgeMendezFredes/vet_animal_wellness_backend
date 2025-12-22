from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.endpoints import dashboard

app = FastAPI(title="Vet Animal Wellness API")

# Configure CORS
origins = [
    "https://vetanimalwellness.cl",
    "http://localhost:5173",
    "http://localhost:4321", # Default Astro dev port
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard.router, prefix="/api/dashboard", tags=["dashboard"])

@app.get("/")
def read_root():
    return {"Hello": "World", "Project": "Vet Animal Wellness Backend"}

@app.get("/health")
def health_check():
    return {"status": "ok"}
