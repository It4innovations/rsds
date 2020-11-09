import pickle
import cloudpickle
import tblib.pickling_support

tblib.pickling_support.install()


def serialize_by_pickle(obj):
    try:
        return pickle.dumps(obj)
    except:
        return cloudpickle.dumps(obj)


def serialize(obj):
    if isinstance(obj, bytes):
        return "none", obj
    else:
        return "pickle", serialize_by_pickle(obj)


def deserialize_by_pickle(data):
    try:
        return pickle.loads(data)
    except:
        return cloudpickle.loads(data)


def deserialize(data, serializer):
    if serializer == "none":
        return data
    if serializer == "pickle":
        return deserialize_by_pickle(data)
    raise Exception("Unknown serializer: {}".format(serializer))
