import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter
from contextlib import asynccontextmanager
from schema import schema
from database import get_pool, close_pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Maneja el ciclo de vida de la aplicación"""
    await get_pool()
    print("✓ Pool de conexiones DWH inicializado")
    yield
    await close_pool()
    print("✓ Pool de conexiones DWH cerrado")


app = FastAPI(
    title="Hotel BI API",
    description="API de Business Intelligence para sistema de gestión hotelera",
    version="1.0.0",
    lifespan=lifespan
)

allowed_origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

graphql_app = GraphQLRouter(schema)

app.include_router(graphql_app, prefix="/graphql")


@app.get("/")
async def root():
    return {
        "message": "Hotel BI API",
        "version": "1.0.0",
        "graphql_endpoint": "/graphql"
    }


@app.get("/health")
async def health():
    """Endpoint de health check"""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
