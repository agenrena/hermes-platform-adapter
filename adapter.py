"""
Agenrena platform adapter for Hermes Agent.

Minimal plugin adapter:
- inbound messages arrive over Agenrena's agent WebSocket
- outbound replies are sent through Agenrena's Agent REST API

Configuration in config.yaml:

    gateway:
      platforms:
        agenrena:
          enabled: true
          extra:
            api_key: "..."
            host: "api.agenrena.com"

Or via environment variables:
    AGENRENA_API_KEY, AGENRENA_HOST
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import quote, urlsplit

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

try:
    import websockets

    WEBSOCKETS_AVAILABLE = True
except ImportError:
    websockets = None  # type: ignore[assignment]
    WEBSOCKETS_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

DEFAULT_HOST = "api.agenrena.com"
RECONNECT_DELAY_SECONDS = 5


def _normalize_host(raw: str | None) -> str:
    """Return a bare host[:port] from env/config values."""
    value = (raw or "").strip() or DEFAULT_HOST
    if "://" in value:
        parsed = urlsplit(value)
        if parsed.netloc:
            return parsed.netloc.rstrip("/")
    return value.strip().strip("/")


def _parse_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        return datetime.now()
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now()


def _get_config_value(config: Any, key: str, env_key: str, default: str = "") -> str:
    extra = getattr(config, "extra", {}) or {}
    return (os.getenv(env_key) or extra.get(key) or default or "").strip()


def check_requirements() -> bool:
    """Return True when optional runtime dependencies are installed."""
    return HTTPX_AVAILABLE and WEBSOCKETS_AVAILABLE


def validate_config(config) -> bool:
    """Return True when Agenrena credentials are configured."""
    return bool(_get_config_value(config, "api_key", "AGENRENA_API_KEY"))


def is_connected(config) -> bool:
    """Check whether Agenrena is configured (env or config.yaml)."""
    return validate_config(config)


class AgenrenaAdapter(BasePlatformAdapter):
    """Hermes adapter for Agenrena direct chats."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("agenrena"))
        self.api_key = _get_config_value(config, "api_key", "AGENRENA_API_KEY")
        self.host = _normalize_host(_get_config_value(config, "host", "AGENRENA_HOST", DEFAULT_HOST))
        reconnect_delay = _get_config_value(
            config,
            "reconnect_delay_seconds",
            "AGENRENA_RECONNECT_DELAY_SECONDS",
            str(RECONNECT_DELAY_SECONDS),
        )
        try:
            self.reconnect_delay = max(1, int(reconnect_delay))
        except ValueError:
            self.reconnect_delay = RECONNECT_DELAY_SECONDS
        self._recv_task: Optional[asyncio.Task] = None
        self._ws = None

    @property
    def name(self) -> str:
        return "Agenrena"

    def _ws_url(self) -> str:
        token = quote(self.api_key, safe="")
        return f"wss://{self.host}/ws/agent/events/?token={token}"

    def _api_url(self, path: str) -> str:
        return f"https://{self.host}{path}"

    async def connect(self) -> bool:
        if not WEBSOCKETS_AVAILABLE:
            self._set_fatal_error(
                "MISSING_DEPENDENCY",
                "The Agenrena adapter requires websockets. Run: pip install websockets",
                retryable=False,
            )
            return False

        if not HTTPX_AVAILABLE:
            self._set_fatal_error(
                "MISSING_DEPENDENCY",
                "The Agenrena adapter requires httpx. Run: pip install httpx",
                retryable=False,
            )
            return False

        if not self.api_key:
            self._set_fatal_error(
                "MISSING_CREDENTIALS",
                "AGENRENA_API_KEY is required",
                retryable=False,
            )
            return False

        self._running = True
        self._recv_task = asyncio.create_task(self._receive_loop())
        self._mark_connected()
        logger.info("[agenrena] WebSocket receiver started for %s", self.host)
        return True

    async def disconnect(self) -> None:
        self._running = False

        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
        self._recv_task = None

        self._mark_disconnected()
        logger.info("[agenrena] Disconnected")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not self.api_key:
            return SendResult(success=False, error="AGENRENA_API_KEY is not configured")

        body: Dict[str, Any] = {
            "conversation_id": chat_id,
            "message_type": "text",
            "text_format": "markdown",
            "text": content,
        }
        if reply_to:
            body["reply_to_message_id"] = reply_to

        try:
            if not HTTPX_AVAILABLE:
                return SendResult(success=False, error="httpx is not installed")

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    self._api_url("/api/agent-api/channels/messages/send/"),
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=body,
                )
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPStatusError as exc:
            error = f"Agenrena send failed: {exc.response.status_code} {exc.response.text}"
            logger.error("[agenrena] %s", error)
            return SendResult(success=False, error=error, retryable=exc.response.status_code >= 500)
        except Exception as exc:
            logger.error("[agenrena] Send failed: %s", exc)
            return SendResult(success=False, error=str(exc), retryable=True)

        return SendResult(
            success=True,
            message_id=str(data.get("message_id") or ""),
            raw_response=data,
        )

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Agenrena currently has no typing endpoint for this minimal adapter."""
        return None

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "dm"}

    async def _receive_loop(self) -> None:
        while self._running:
            try:
                async with websockets.connect(self._ws_url(), ping_interval=20, ping_timeout=20) as ws:
                    self._ws = ws
                    logger.info("[agenrena] WebSocket connected")
                    async for raw in ws:
                        await self._handle_ws_message(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._running:
                    logger.error("[agenrena] WebSocket error: %s", exc)
            finally:
                self._ws = None

            if self._running:
                logger.info("[agenrena] Reconnecting WebSocket in %ss", self.reconnect_delay)
                await asyncio.sleep(self.reconnect_delay)

    async def _handle_ws_message(self, raw: Any) -> None:
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            payload = json.loads(str(raw))
        except Exception:
            logger.warning("[agenrena] Ignoring invalid WebSocket payload")
            return

        message_id = str(payload.get("id") or "").strip()
        chat_id = str(payload.get("conversation_id") or "").strip()
        sender = payload.get("sender") if isinstance(payload.get("sender"), dict) else {}
        user_id = str(sender.get("id") or "").strip()

        if not message_id or not chat_id or not user_id:
            logger.debug("[agenrena] Ignoring incomplete WebSocket payload: %s", payload)
            return

        text = str(payload.get("text") or "")
        images = payload.get("images") if isinstance(payload.get("images"), list) else []
        if not text.strip():
            if images:
                logger.info("[agenrena] Skipping image-only inbound message %s", message_id)
            return

        sender_name = (
            str(sender.get("display_name") or sender.get("name") or user_id).strip()
            or user_id
        )
        source = self.build_source(
            chat_id=chat_id,
            chat_name=sender_name,
            chat_type="dm",
            user_id=user_id,
            user_name=sender_name,
            message_id=message_id,
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=payload,
            message_id=message_id,
            reply_to_message_id=payload.get("reply_to_id"),
            timestamp=_parse_timestamp(payload.get("created_at")),
        )
        await self.handle_message(event)


def register(ctx) -> None:
    """Plugin entry point called by the Hermes plugin system."""
    ctx.register_platform(
        name="agenrena",
        label="Agenrena",
        adapter_factory=lambda cfg: AgenrenaAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["AGENRENA_API_KEY"],
        install_hint="pip install websockets",
        allowed_users_env="AGENRENA_ALLOWED_USERS",
        allow_all_env="AGENRENA_ALLOW_ALL_USERS",
        max_message_length=4000,
        emoji="",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via Agenrena. Agenrena supports markdown text "
            "messages. Keep replies concise and suitable for a direct chat."
        ),
    )
