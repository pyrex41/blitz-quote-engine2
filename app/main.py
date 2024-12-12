from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from app.routers import quotes  # Import your router
import os
import dotenv

dotenv.load_dotenv()

app = FastAPI()

sync_url = os.getenv("NEW_QUOTE_DB_URL")
auth_token = os.getenv("NEW_QUOTE_DB_KEY")
print(f"sync_url: {sync_url}")
print(f"auth_token: {auth_token}")





@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )

@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors()},
    )

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": "An unexpected error occurred"},
    )

app.include_router(quotes.router)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Medicare Supplement Rate API"}
