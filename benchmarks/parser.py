import traceback

import msgpack

BYTES = []


def parse_messages(stream):
    data = stream.read(8)
    if len(data) < 8:
        return []
    chunk_count = int.from_bytes(data, "little", signed=False)
    chunk_sizes = []
    for chunk in range(chunk_count):
        size = int.from_bytes(stream.read(8), "little", signed=False)
        chunk_sizes.append(size)

    messages = []
    for id, size in enumerate(chunk_sizes):
        if size:
            data = stream.read(size)
            assert len(data) == size
            print(id, size, len(chunk_sizes))
            try:
                parsed = msgpack.unpackb(data, use_list=False)
                print(parsed)
                messages.append(parsed)
            except:
                traceback.print_exc()
    return messages


with open("stream.bin", "rb") as f:
    while True:
        messages = parse_messages(f)
        if not messages:
            break
