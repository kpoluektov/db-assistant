"""Ship OpenAI Agents SDK traces to Yandex Monium via OTLP/gRPC.

Registers a ``TracingProcessor`` that mirrors every Agents-SDK trace and span
as an OpenTelemetry span (preserving parent/child hierarchy and timestamps),
and configures an OTLP gRPC exporter pointed at Monium's ingest endpoint.

See https://yandex.cloud/ru/docs/monium/traces/instrumentation/manual
"""
from __future__ import annotations

import atexit
import logging
from datetime import datetime, timezone
from typing import Any

from agents.tracing import Span, Trace, TracingProcessor, set_trace_processors
from opentelemetry import trace as otel_trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

_log = logging.getLogger("db_assistant")


def setup_monium_tracing(
    *,
    endpoint: str,
    api_key: str,
    folder_id: str,
    service_name: str,
) -> None:
    """Configure OTel + register the Agents-SDK ↔ OTel bridge processor.

    Replaces the SDK's default (OpenAI-backend) trace processor with the
    Monium bridge so Yandex-only deployments don't spam attempts to reach
    api.openai.com.
    """
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(
        endpoint=endpoint,
        headers=(
            ("authorization", f"Api-Key {api_key}"),
            ("x-monium-project", f"folder__{folder_id}"),
        ),
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    otel_trace.set_tracer_provider(provider)
    atexit.register(provider.shutdown)

    set_trace_processors([_MoniumProcessor(service_name)])
    _log.info(
        "Monium tracing enabled: endpoint=%s service=%s", endpoint, service_name
    )


def _ns(iso: str | None) -> int | None:
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000_000)
    except ValueError:
        return None


def _span_name(data: Any) -> str:
    if data is None:
        return "span"
    t = data.type
    name = getattr(data, "name", None)
    if t == "agent":
        return f"agent.{name}"
    if t == "function":
        return f"tool.{name}"
    if t == "generation":
        return f"llm.{getattr(data, 'model', None) or 'generation'}"
    if t == "response":
        return "llm.response"
    if t == "handoff":
        return (
            f"handoff.{getattr(data, 'from_agent', '?')}"
            f"->{getattr(data, 'to_agent', '?')}"
        )
    if t == "mcp_tools":
        return f"mcp.list_tools.{getattr(data, 'server', '?')}"
    if t in ("custom", "guardrail"):
        return f"{t}.{name}"
    return t


def _span_attrs(data: Any) -> dict[str, Any]:
    if data is None:
        return {}
    attrs: dict[str, Any] = {"agents.span.type": data.type}
    for key in ("name", "model", "from_agent", "to_agent", "server", "triggered"):
        val = getattr(data, key, None)
        if val is not None:
            attrs[f"agents.{key}"] = (
                val if isinstance(val, (str, int, float, bool)) else str(val)
            )
    for key in ("tools", "handoffs"):
        seq = getattr(data, key, None)
        if seq:
            attrs[f"agents.{key}"] = [str(x) for x in seq]
    usage = getattr(data, "usage", None)
    if isinstance(usage, dict):
        for k, v in usage.items():
            if isinstance(v, (int, float)):
                attrs[f"agents.usage.{k}"] = v
    return attrs


class _MoniumProcessor(TracingProcessor):
    """Translates Agents-SDK trace/span events into OpenTelemetry spans."""

    def __init__(self, service_name: str) -> None:
        self._tracer = otel_trace.get_tracer(service_name)
        self._trace_roots: dict[str, otel_trace.Span] = {}
        self._spans: dict[str, otel_trace.Span] = {}

    def on_trace_start(self, trace: Trace) -> None:
        try:
            root = self._tracer.start_span(
                name=trace.name or "agent_trace",
                kind=SpanKind.INTERNAL,
                attributes={"agents.trace_id": trace.trace_id},
            )
            self._trace_roots[trace.trace_id] = root
        except Exception:
            _log.exception("Monium: on_trace_start failed")

    def on_trace_end(self, trace: Trace) -> None:
        root = self._trace_roots.pop(trace.trace_id, None)
        if root is None:
            return
        try:
            root.end()
        except Exception:
            _log.exception("Monium: on_trace_end failed")

    def on_span_start(self, span: Span[Any]) -> None:
        try:
            parent = self._spans.get(span.parent_id) if span.parent_id else None
            if parent is None:
                parent = self._trace_roots.get(span.trace_id)
            ctx = otel_trace.set_span_in_context(parent) if parent else None
            attrs = _span_attrs(span.span_data)
            attrs["agents.span_id"] = span.span_id
            attrs["agents.trace_id"] = span.trace_id
            if span.parent_id:
                attrs["agents.parent_span_id"] = span.parent_id
            otel_span = self._tracer.start_span(
                name=_span_name(span.span_data),
                context=ctx,
                kind=SpanKind.INTERNAL,
                attributes=attrs,
                start_time=_ns(span.started_at),
            )
            self._spans[span.span_id] = otel_span
        except Exception:
            _log.exception("Monium: on_span_start failed")

    def on_span_end(self, span: Span[Any]) -> None:
        otel_span = self._spans.pop(span.span_id, None)
        if otel_span is None:
            return
        try:
            for k, v in _span_attrs(span.span_data).items():
                otel_span.set_attribute(k, v)
            err = span.error
            if err:
                msg = err.get("message") if isinstance(err, dict) else str(err)
                otel_span.set_status(Status(StatusCode.ERROR, msg or "error"))
                if isinstance(err, dict) and err.get("data") is not None:
                    otel_span.set_attribute("agents.error.data", str(err["data"]))
            otel_span.end(end_time=_ns(span.ended_at))
        except Exception:
            _log.exception("Monium: on_span_end failed")

    def shutdown(self) -> None:
        for otel_span in list(self._spans.values()):
            try:
                otel_span.end()
            except Exception:
                pass
        self._spans.clear()
        for root in list(self._trace_roots.values()):
            try:
                root.end()
            except Exception:
                pass
        self._trace_roots.clear()

    def force_flush(self) -> None:
        try:
            otel_trace.get_tracer_provider().force_flush()
        except Exception:
            pass
