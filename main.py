from fastapi import FastAPI
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
