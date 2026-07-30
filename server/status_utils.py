BEFORE_OPERATION = 1
IN_OPERATION = 2
AFTER_OPERATION = 3
CANCELED_OPERATION = 4


def to_int(value):
    if value is None:
        return None

    try:
        text = str(value).strip()
        if not text or text.lower() == "null":
            return None
        return int(float(text))
    except (TypeError, ValueError):
        return None


def route_operation_status(status):
    value = to_int(status)
    if value is None:
        return None
    if value >= 100:
        return value // 100
    return value


def is_canceled_route_status(status):
    value = to_int(status)
    if value is None:
        return False
    return value == CANCELED_OPERATION or 400 <= value < 500


def is_canceled_operation_status(status):
    return to_int(status) == CANCELED_OPERATION


def is_visible_route(route_status, operation_status):
    return (
        not is_canceled_route_status(route_status)
        and not is_canceled_operation_status(operation_status)
    )
