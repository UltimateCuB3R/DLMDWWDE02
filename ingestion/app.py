from __future__ import annotations

import os
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

try:
    from ingestion.ingest import InternalTable, load_csv_file
except ModuleNotFoundError:  # Allows `python ingestion\app.py` from project root.
    from ingest import InternalTable, load_csv_file

app = FastAPI(title="CSV Loader Service", version="1.0.0")


def _serialize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _serialize_internal_table(internal_table: InternalTable) -> dict[str, Any]:
    return {
        "table_name": internal_table.table_name,
        "columns": [asdict(column) for column in internal_table.columns],
        "rows": [
            {column_name: _serialize_value(cell_value) for column_name, cell_value in row.items()}
            for row in internal_table.rows
        ],
    }


class LoadCsvRequest(BaseModel):
    file_path: str
    table_name: str
    encoding: str = "utf-8-sig"


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/load-csv")
def load_csv(payload: LoadCsvRequest) -> dict[str, Any]:
    try:
        internal_table = load_csv_file(
            file_path=payload.file_path,
            table_name=payload.table_name,
            encoding=payload.encoding,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return _serialize_internal_table(internal_table)


def run() -> None:
    host = os.getenv("CSV_SERVICE_HOST", "0.0.0.0")
    port_value = os.getenv("CSV_SERVICE_PORT", "8000")
    try:
        port = int(port_value)
    except ValueError as exc:
        raise ValueError(f"Invalid CSV_SERVICE_PORT value: {port_value}") from exc

    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    run()
