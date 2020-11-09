def substitude_keys(obj, data):
    if isinstance(obj, str):
        try:
            return data[obj]
        except KeyError:
            return obj
    if isinstance(obj, list):
        return [substitude_keys(o, data) for o in obj]
    if isinstance(obj, tuple):
        return tuple(substitude_keys(o, data) for o in obj)
    if isinstance(obj, set):
        return set(substitude_keys(o, data) for o in obj)
    if isinstance(obj, frozenset):
        return frozenset(substitude_keys(o, data) for o in obj)
    return obj
