from dishka import FromDishka
from dishka.integrations.fastapi import inject
from fastapi import APIRouter, status, HTTPException
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession

from hack.core.models import Appeal, Lead
from hack.core.models.appeal import AppealStatusEnum
from hack.core.services.uow_ctl import UoWCtl
from hack.core.services.appeal_routing import (
    AppealRoutingService,
    NoAvailableOperatorError,
)
from hack.rest_server.schemas.appeals import (
    AppealDTO,
    CreateAppealDTO,
    UpdateAppealDTO,
)


router = APIRouter(
    prefix="/appeals",
)


@router.post(
    "",
    response_model=AppealDTO,
    status_code=status.HTTP_201_CREATED,
)
@inject
async def create_appeal(
    session: FromDishka[AsyncSession],
    uow_ctl: FromDishka[UoWCtl],
    routing_service: FromDishka[AppealRoutingService],
    payload: CreateAppealDTO,
) -> Appeal:
    appeal = Appeal(
        status=AppealStatusEnum.ACTIVE,
        lead_id=payload.lead_id,
        lead_source_id=payload.lead_source_id,
        assigned_operator_id=None,
    )
    session.add(appeal)
    await session.flush()

    try:
        operator = await routing_service.allocate_operator(
            appeal=appeal,
        )
    except NoAvailableOperatorError:
        operator = None

    if operator is not None:
        appeal.assigned_operator_id = operator.id
        await session.flush()

    await uow_ctl.commit()
    return appeal


@router.put(
    "/{appeal_id}",
    response_model=AppealDTO,
)
@inject
async def update_appeal(
    session: FromDishka[AsyncSession],
    uow_ctl: FromDishka[UoWCtl],
    appeal_id: int,
    payload: UpdateAppealDTO,
) -> Appeal:
    stmt = (select(Appeal)
            .where(Appeal.id == appeal_id))
    appeal = await session.scalar(stmt)

    if appeal is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Appeal not found",
        )

    appeal.status = payload.status
    await session.flush()
    await uow_ctl.commit()

    return appeal


@router.delete(
    "/{appeal_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
@inject
async def delete_appeal(
    session: FromDishka[AsyncSession],
    uow_ctl: FromDishka[UoWCtl],
    appeal_id: int,
) -> None:
    stmt = (select(Appeal)
            .where(Appeal.id == appeal_id))
    appeal = await session.scalar(stmt)

    if appeal is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Appeal not found",
        )

    await session.delete(appeal)
    await session.flush()
    await uow_ctl.commit()
    return None


@router.get(
    "",
    response_model=list[AppealDTO],
)
@inject
async def list_appeals(
    session: FromDishka[AsyncSession],
) -> list[Appeal]:
    stmt = (select(Appeal)
            .order_by(Appeal.id))
    appeals = await session.scalars(stmt)
    return list(appeals)


@router.get(
    "/{appeal_id}",
    response_model=AppealDTO,
)
@inject
async def get_appeal(
    session: FromDishka[AsyncSession],
    appeal_id: int,
) -> Appeal:
    stmt = (select(Appeal)
            .where(Appeal.id == appeal_id))
    appeal = await session.scalar(stmt)

    if appeal is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Appeal not found",
        )

    return appeal
