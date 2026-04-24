"""
Telegram 群組成員追蹤

upsert_member()  — 任何訊息/加入事件時呼叫，自動新增或更新
list_members()   — 列出指定 chat 的所有已知成員
"""
from __future__ import annotations

import logging
from datetime import datetime

from .database import get_session
from .models import TelegramChatMember

logger = logging.getLogger(__name__)


def upsert_member(
    chat_id: int | str,
    user_id: int | str,
    *,
    username: str = "",
    full_name: str = "",
    joined_via: str = "message",
) -> bool:
    """
    新增或更新成員記錄。
    首次出現 → INSERT；再次出現 → 更新 last_seen_at / username / full_name。
    """
    chat_id = str(chat_id)
    user_id = str(user_id)
    try:
        with get_session() as sess:
            row = (
                sess.query(TelegramChatMember)
                .filter_by(chat_id=chat_id, user_id=user_id)
                .first()
            )
            now = datetime.now()
            if row:
                row.last_seen_at = now
                if username:
                    row.username = username
                if full_name:
                    row.full_name = full_name
            else:
                sess.add(TelegramChatMember(
                    chat_id=chat_id,
                    user_id=user_id,
                    username=username,
                    full_name=full_name,
                    first_seen_at=now,
                    last_seen_at=now,
                    joined_via=joined_via,
                ))
            sess.commit()
        return True
    except Exception as e:
        logger.error("upsert_member failed: %s", e)
        return False


def list_members(chat_id: int | str) -> list[dict]:
    """回傳指定 chat 所有已記錄成員，依 first_seen_at 排序"""
    chat_id = str(chat_id)
    try:
        with get_session() as sess:
            rows = (
                sess.query(TelegramChatMember)
                .filter_by(chat_id=chat_id)
                .order_by(TelegramChatMember.first_seen_at)
                .all()
            )
            return [
                {
                    "user_id":      r.user_id,
                    "username":     r.username,
                    "full_name":    r.full_name,
                    "joined_via":   r.joined_via,
                    "first_seen":   r.first_seen_at,
                    "last_seen":    r.last_seen_at,
                }
                for r in rows
            ]
    except Exception as e:
        logger.error("list_members failed: %s", e)
        return []
