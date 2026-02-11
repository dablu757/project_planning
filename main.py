from fastapi import FastAPI

from tc_router import router as tc_router

app = FastAPI(title="Project Planning Task Chain")
app.include_router(tc_router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
