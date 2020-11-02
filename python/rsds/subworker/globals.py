
_global_subworker = None


def _set_global_subworker(subworker):
    global _global_subworker
    _global_subworker = subworker


def _get_global_subworker():
    subworker = _global_subworker
    if subworker is None:
        raise Exception("The code is not running inside a RSDS subworker")
    return subworker


def get_worker_id():
    subworker = _get_global_subworker()
    return subworker.worker_id


def get_subworker_id():
    subworker = _get_global_subworker()
    return subworker.subworker_id