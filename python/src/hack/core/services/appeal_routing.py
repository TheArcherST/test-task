import random

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from hack.core.errors.appeal_routing import NoAvailableOperatorError
from hack.core.models import Appeal

from hack.core.models import Operator
from hack.core.models.appeal import AppealStatusEnum
from hack.core.models.operator import LeadSourceOperator, OperatorStatusEnum


class AppealRoutingService:
    def __init__(
            self,
            session: AsyncSession,
    ):
        self.session = session

    async def allocate_operator(
            self,
            appeal: Appeal,
    ) -> Operator:
        active_appeals_count_subq = (
            select(func.count(Appeal.id))
            .where(
                Appeal.assigned_operator_id == Operator.id,
                Appeal.status == AppealStatusEnum.ACTIVE,
            )
            .correlate(Operator)
            .scalar_subquery()
        )

        stmt = (
            select(Operator, LeadSourceOperator.routing_factor)
            .join(
                LeadSourceOperator,
                LeadSourceOperator.operator_id == Operator.id,
            )
            .where(
                LeadSourceOperator.lead_source_id == appeal.lead_source_id,
                Operator.status == OperatorStatusEnum.ACTIVE,
                active_appeals_count_subq < Operator.active_appeals_limit,
            )
            # lock operator for concurrent routing
            # lock is to be released by usecase logic
            .with_for_update()
        )

        result = await self.session.execute(stmt)
        rows = result.all()

        if not rows:
            raise NoAvailableOperatorError(
                f"No available operators for"
                f" lead_source_id={appeal.lead_source_id}"
            )

        # Weighted random choice by routing_factor
        operators: list[Operator] = [row[0] for row in rows]
        weights: list[int] = [row[1] for row in rows]

        total_weight = sum(weights)
        if total_weight <= 0:
            raise NoAvailableOperatorError(
                f"Non-positive total routing weight for"
                f" lead_source_id={appeal.lead_source_id}"
            )

        chosen_list = random.choices(operators, weights=weights, k=1)

        if not chosen_list:
            raise NoAvailableOperatorError(
                "Failed to choose operator by weight")

        return chosen_list[0]
