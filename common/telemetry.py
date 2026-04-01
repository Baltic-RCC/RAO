import functools
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

_initialized = False


def init_tracing(provider):
    global _initialized
    if _initialized:
        return
    trace.set_tracer_provider(provider)
    _initialized = True


def otel_span(span_name: str = "undefined", tracer: object | None = None, attr_fn=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = span_name or func.__name__
            with tracer.start_as_current_span(name) as span:
                try:
                    if attr_fn:
                        attrs = attr_fn(*args, **kwargs) or {}
                        for k, v in attrs.items():
                            if v is not None:
                                span.set_attribute(k, v)
                    result = func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise

        return wrapper
    return decorator
