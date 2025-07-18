import time
import functools
from loguru import logger


def performance_counter(units='seconds'):
    """Counts performance of the function"""
    def decorator_performance_counter(func):
        @functools.wraps(func)
        def wrapper_performance_counter(*args, **kwargs):
            start_time = time.perf_counter()
            response = func(*args, **kwargs)
            duration = round(time.perf_counter() - start_time, 2)
            if units == 'minutes':
                duration = round(duration / 60, 2)  # counting by minutes
            _process_name = f"{func.__module__}.{func.__name__}"
            logger.bind(duration=duration, measured_function=_process_name).info(
                f"Process '{_process_name}' finished with duration: {duration} {units}")
            return response
        return wrapper_performance_counter
    return decorator_performance_counter
