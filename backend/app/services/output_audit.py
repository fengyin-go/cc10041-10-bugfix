"""输出审计：审计结论只认结果写入器里真实落地的数据。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class OutputAuditService(BaseModuleService):
    MODULE = "output_audit"
    ENTITY = "审计记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["管道名称", "输出目标", "输出时间", "输出行数", "校验结果"]
    REQUIRED_FIELDS = ["管道名称", "输出目标", "输出时间"]
    STATUS_ORDER = ["已记录", "校验失败"]
    ACTION_RULES = {"重新校验": "已记录", "标记通过": "已记录"}
    NEGATIVE_ACTIONS = []

    def _landed_rows(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        """找出这条审计对应运行里，确实写入目标表的结果写入记录。"""
        target = str(entry.get("输出目标", ""))
        run_id = entry.get("run_id")
        return [
            row for row in store.rows("result_writer")
            if str(row.get("输出目标", "")) == target
            and (not run_id or row.get("run_id") == run_id)
            and int(row.get("输出行数", 0) or 0) > 0
        ]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action not in self.ACTION_RULES:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        landed = self._landed_rows(entry)
        if not landed:
            # 没有真实写入记录：不能把未产出的目标标成校验通过。
            entry["校验结果"] = entry.get("校验结果") or "未产出（上游阻塞）"
            entry["status"] = "校验失败"
            entry["pending"] = True
            entry["abnormal"] = True
            entry["校验依据"] = "无实际写入记录，校验结论保持未通过"
            return entry, f"{self.ENTITY}没有实际落地数据，无法标记为校验通过"

        row_count = sum(int(row.get("输出行数", 0) or 0) for row in landed)
        entry["校验结果"] = "校验通过"
        entry["输出行数"] = str(row_count)
        entry["status"] = "已记录"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["已落地"] = True
        entry["实际写入行数"] = row_count
        entry["校验依据"] = "结果写入器实际写入记录"
        verb = "重新校验通过" if action == "重新校验" else "已标记通过"
        return entry, f"{self.ENTITY}{verb}（实际写入 {row_count} 行）"
