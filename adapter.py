"""
Agenrena platform adapter for Hermes Agent.

Minimal plugin adapter:
- inbound messages arrive over Agenrena's agent WebSocket
- outbound replies are sent through Agenrena's Agent REST API

Run ``hermes gateway setup`` to configure, or set the environment variable:
    AGENRENA_API_KEY
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import quote

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
    cache_image_from_url,
)

logger = logging.getLogger(__name__)

API_HOST = "api.agenrena.com"
RECONNECT_DELAY_SECONDS = 5


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
        self._recv_task: Optional[asyncio.Task] = None
        self._ws = None
        self._connected_event: asyncio.Event = asyncio.Event()

    @property
    def name(self) -> str:
        return "Agenrena"

    def _ws_url(self) -> str:
        token = quote(self.api_key, safe="")
        return f"wss://{API_HOST}/ws/agent/events/?token={token}"

    def _api_url(self, path: str) -> str:
        return f"https://{API_HOST}{path}"

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
        self._connected_event.clear()
        self._recv_task = asyncio.create_task(self._receive_loop())

        try:
            await asyncio.wait_for(self._connected_event.wait(), timeout=15.0)
        except asyncio.TimeoutError:
            self._set_fatal_error(
                "CONNECT_TIMEOUT",
                "WebSocket handshake timed out",
                retryable=True,
            )
            return False

        if not self._ws:
            self._set_fatal_error(
                "CONNECT_FAILED",
                "WebSocket connection failed (check API key)",
                retryable=True,
            )
            return False

        self._mark_connected()
        logger.info("[agenrena] WebSocket connected")
        await self._register_agent_info()
        return True

    async def _register_agent_info(self) -> None:
        """Report agent_type and supported slash commands to Agenrena."""
        try:
            from hermes_cli.commands import COMMAND_REGISTRY
        except ImportError:
            logger.warning("[agenrena] hermes_cli.commands not available, skipping command registration")
            return

        commands = [
            {
                "name": cmd.name,
                "description": cmd.description,
                "aliases": list(cmd.aliases),
                "args_hint": cmd.args_hint,
                "subcommands": list(cmd.subcommands),
            }
            for cmd in COMMAND_REGISTRY
            if not cmd.cli_only
        ]

        payload: Dict[str, Any] = {
            "agent_type": "hermes",
            "slash_commands": commands,
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.patch(
                    self._api_url("/api/agent-api/agents/me/"),
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
                response.raise_for_status()
                logger.info(
                    "[agenrena] Registered agent info: type=hermes, %d slash commands",
                    len(commands),
                )
        except Exception as e:
            logger.warning("[agenrena] Failed to register agent info: %s", e)

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
            error = (
                f"Agenrena send failed: {exc.response.status_code} {exc.response.text}"
            )
            logger.error("[agenrena] %s", error)
            return SendResult(
                success=False, error=error, retryable=exc.response.status_code >= 500
            )
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
                async with websockets.connect(
                    self._ws_url(), ping_interval=20, ping_timeout=20
                ) as ws:
                    self._ws = ws
                    self._connected_event.set()
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
                logger.info(
                    "[agenrena] Reconnecting WebSocket in %ss", RECONNECT_DELAY_SECONDS
                )
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)

    # -- Context item handlers keyed by metadata.message_type ---------------
    # Each handler receives (item dict) and returns (text, media_urls, media_types).
    # To support a new type, just add a ``_context_<type>`` method.

    async def _context_text(
        self, item: Dict[str, Any]
    ) -> tuple[str, list[str], list[str]]:
        label = item.get("label", "")
        content = item.get("content", "")
        text = f"{label}: {content}" if label and content else content
        return text, [], []

    async def _context_image(
        self, item: Dict[str, Any], start_index: int = 0
    ) -> tuple[str, list[str], list[str]]:
        label = item.get("label", "")
        urls: list[str] = []
        types: list[str] = []
        for media in item.get("media") or []:
            if not isinstance(media, dict):
                continue
            media_url = media.get("url", "")
            if not media_url:
                continue
            try:
                cached = await cache_image_from_url(media_url, ext=".jpg")
                urls.append(cached)
                types.append("image/jpeg")
                logger.info("[agenrena] Cached context image: %s", cached)
            except Exception as e:
                logger.warning("[agenrena] Failed to cache context image: %s", e)
                urls.append(media_url)
                types.append("image/jpeg")
        if urls:
            indices = ", ".join(
                f"#{start_index + i + 1}" for i in range(len(urls))
            )
            text = f"{label}: [referenced image {indices}]" if label else f"[referenced image {indices}]"
        else:
            text = ""
        return text, urls, types

    async def _process_context_item(
        self, item: Dict[str, Any], start_index: int = 0
    ) -> tuple[str, list[str], list[str]]:
        """Dispatch a single context item to its type-specific handler."""
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        msg_type = metadata.get("message_type", "")
        handler = getattr(self, f"_context_{msg_type}", None)
        if handler is not None:
            if msg_type == "image":
                return await handler(item, start_index=start_index)
            return await handler(item)
        # Fallback: dump as JSON so the agent still sees unknown types
        label = item.get("label", "")
        fallback = json.dumps(item, ensure_ascii=False)
        text = f"{label}: {fallback}" if label else fallback
        logger.debug("[agenrena] Unknown context type %r, using fallback", msg_type)
        return text, [], []

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
        sender = (
            payload.get("sender") if isinstance(payload.get("sender"), dict) else {}
        )
        user_id = str(sender.get("id") or "").strip()

        if not message_id or not chat_id or not user_id:
            logger.debug(
                "[agenrena] Ignoring incomplete WebSocket payload: %s", payload
            )
            return

        text = str(payload.get("text") or "")
        images = (
            payload.get("images") if isinstance(payload.get("images"), list) else []
        )

        # Download images to local cache for vision tool access
        media_urls: list[str] = []
        media_types: list[str] = []
        for img in images:
            if not isinstance(img, dict):
                continue
            img_url = img.get("url", "")
            if not img_url:
                continue
            mime_type = img.get("mime_type", "image/jpeg")
            ext = "." + mime_type.split("/")[-1].split(";")[0]
            if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
                ext = ".jpg"
            try:
                cached_path = await cache_image_from_url(img_url, ext=ext)
                media_urls.append(cached_path)
                media_types.append(mime_type)
                logger.info("[agenrena] Cached image: %s", cached_path)
            except Exception as e:
                logger.warning("[agenrena] Failed to cache image: %s", e)
                media_urls.append(img_url)
                media_types.append(mime_type)

        # Parse context: referenced conversation items
        reply_to_text: Optional[str] = None
        context = payload.get("context")
        if isinstance(context, dict):
            items = context.get("items") if isinstance(context.get("items"), list) else []
            context_parts: list[str] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                ctx_text, ctx_urls, ctx_types = await self._process_context_item(
                    item, start_index=len(media_urls)
                )
                if ctx_text:
                    context_parts.append(ctx_text)
                media_urls.extend(ctx_urls)
                media_types.extend(ctx_types)
            if context_parts:
                reply_to_text = "\n".join(context_parts)

        if not text.strip() and not media_urls:
            return

        msg_type = MessageType.PHOTO if media_urls else MessageType.TEXT

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
            message_type=msg_type,
            source=source,
            raw_message=payload,
            message_id=message_id,
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=payload.get("reply_to_id"),
            reply_to_text=reply_to_text,
            timestamp=_parse_timestamp(payload.get("created_at")),
        )
        await self.handle_message(event)


def interactive_setup() -> None:
    """Interactive ``hermes gateway setup`` flow for the Agenrena platform.

    Lazy-imports ``hermes_cli.setup`` helpers so the plugin stays importable
    in non-CLI contexts (gateway runtime, tests).
    """
    from hermes_cli.setup import (
        prompt,
        prompt_yes_no,
        save_env_value,
        get_env_value,
        print_header,
        print_info,
        print_warning,
        print_success,
    )

    print_header("Agenrena")
    existing_key = get_env_value("AGENRENA_API_KEY")
    if existing_key:
        masked = (
            existing_key[:6] + "..." + existing_key[-4:]
            if len(existing_key) > 10
            else "***"
        )
        print_info(f"Agenrena: already configured (key: {masked})")
        if not prompt_yes_no("Reconfigure Agenrena?", False):
            return

    print_info("Connect Hermes to Agenrena.")
    print_info("   You need an Agenrena agent API key (starts with agr_).")
    print_info("   Get one from your Agenrena dashboard.")
    print()

    api_key = prompt("Agenrena API key", default="", password=True)
    if not api_key:
        print_warning("API key is required — skipping Agenrena setup")
        return
    save_env_value("AGENRENA_API_KEY", api_key.strip())

    print()
    print_info("Access control: restrict who can message the bot")
    allow_all = prompt_yes_no("Allow all Agenrena users to talk to the bot?", True)
    if allow_all:
        save_env_value("AGENRENA_ALLOW_ALL_USERS", "true")
        save_env_value("AGENRENA_ALLOWED_USERS", "")
    else:
        save_env_value("AGENRENA_ALLOW_ALL_USERS", "false")
        allowed = prompt(
            "Allowed user IDs (comma-separated)",
            default=get_env_value("AGENRENA_ALLOWED_USERS") or "",
        )
        if allowed:
            save_env_value("AGENRENA_ALLOWED_USERS", allowed.replace(" ", ""))
            print_success("Allowlist configured")
        else:
            save_env_value("AGENRENA_ALLOWED_USERS", "")
            print_info(
                "No users allowed — the bot will ignore all messages until you add users."
            )

    print()
    print_success("Agenrena configuration saved to ~/.hermes/.env")
    print_info("Restart the gateway for changes to take effect: hermes gateway restart")


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
        install_hint="pip install websockets httpx",
        setup_fn=interactive_setup,
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
