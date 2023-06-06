import asyncio
import json

from fastapi import FastAPI, Depends, Body, HTTPException, status, Request
from sqlalchemy.future import select
from sqlalchemy import update
from sqlalchemy.orm import joinedload
from sqlalchemy.ext.asyncio import AsyncSession
from aiokafka import AIOKafkaProducer


from .models import Clinic, Client, association_table
from .db import get_db, Database
from .schema import AccessRequest

app = FastAPI()
producer = AIOKafkaProducer(bootstrap_servers='127.0.0.1:9092')


@app.on_event("startup")
async def startup_event():
    await asyncio.gather(
        get_db().create_tables(),
        producer.start()
    )


@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()


class ClinicAccess:
    @staticmethod
    async def base_access(
        session: AsyncSession, client_id, clinic_id, request_mode: bool
    ):
        clinic = await session.scalar(
            select(Clinic)
            .where(Clinic.id == clinic_id)
        )
        client = await session.scalar(
            select(Client)
            .where(Client.id == client_id)
        )
        if not client or not clinic:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="No such client/clinic exists"
            )
        query = (
            update(association_table)
            .where(association_table.c.clinic_id == clinic.id)
            .where(association_table.c.client_id == client.id)
            .values(status=request_mode)
        )
        await session.execute(query)
        await session.commit()


@app.post("/clinic/request")
async def request_access(
    body: AccessRequest = Body(), db: Database = Depends(get_db)
):
    async with db.begin() as session:
        await ClinicAccess.base_access(
            session,
            body.client_id,
            body.clinic_id,
            False
        )
    return {"message": "Access requested"}


@app.post("/client/{client_id}/authorize")
async def authorize_access(
    body: AccessRequest = Body(), db: Database = Depends(get_db)
):
    async with db.begin() as session:
        await ClinicAccess.base_access(
            session,
            body.client_id,
            body.clinic_id,
            True
        )
    return {"message": "Access requested"}


@app.post("/mock")
async def mock_data(
    db: Database = Depends(get_db)
):
    async with db.begin() as session:
        clinic = Clinic(api_urls="http://127.0.0.1:8000/test")
        client = Client()
        client.clinics.append(clinic)
        session.add(client)
        session.add(clinic)
        clinic = await session.scalar(
            select(Clinic)
            .where(Clinic.id == 1)
        )
    return {"message": "Data mocked"}


@app.get("/client/{obj_id}")
async def get_client(
    obj_id: int,
    db: Database = Depends(get_db),
):
    async with db.begin() as session:
        client = await session.scalar(
            select(Client)
            .where(Client.id == obj_id)
            .options(joinedload(Client.clinics))
        )
    return {
        "id": client.id,
        "clinics": client.clinics
    }


@app.get("/clinic/{obj_id}")
async def get_clinic(
    obj_id: int,
    db: Database = Depends(get_db)
):
    async with db.begin() as session:
        clinic = await session.scalar(
            select(Clinic)
            .where(Clinic.id == obj_id)
            .options(joinedload(Clinic.clients))
        )
    return {
        "id": clinic.id,
        "clients": clinic.clients
    }


@app.post("/machine/temperature")
async def post_temperature(
    client_id: int, temperature: float,
    db: Database = Depends(get_db)
):
    async with db.begin() as session:
        result = await session.execute(
                select(Clinic.api_urls)
                .join(association_table)
                .join(Client)
                .where(association_table.c.status == True, association_table.c.client_id == client_id)
            )
        api_urls = [row[0] for row in result]

    result = {"api_urls": api_urls, "temperature": temperature}
    await producer.send_and_wait(
        "requests", json.dumps(result).encode()
    )
    return result


@app.post("/test")
async def test(request: Request):
    print("Recieved: ", await request.body())
