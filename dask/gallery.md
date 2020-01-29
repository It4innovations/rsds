# Client -> Scheduler
### Identity
```
{
  "op": "identity",
  "reply": True
}
```

### Heartbeat
```
{'op': 'heartbeat-client'}
```

### Register client
```
{
  "client": "Client-76300f78-3907-11ea-8345-3ca06793ca95",
  "op": "register-client",
  "reply": False
}
```

### Update graph
```
{
    "actors": None,
    "dependencies": { "("getitem-len-chunk-make-timeseries-len-agg-04716fd14681d389e2bee6909af649ba", 0)": [   ],
                        "("len-agg-04716fd14681d389e2bee6909af649ba", 0)": ["("getitem-len-chunk-make-timeseries-len-agg-04716fd14681d389e2bee6909af649ba", ""0)"]},
    "fifo_timeout": "60s",
    "keys": ["("len-agg-04716fd14681d389e2bee6909af649ba", 0)"],
    "loose_restrictions": None,
    "op": "update-graph",
    "priority": {"("getitem-len-chunk-make-timeseries-len-agg-04716fd14681d389e2bee6909af649ba", 0)": 0,
                 "("len-agg-04716fd14681d389e2bee6909af649ba", 0)": 1},
    "resources": None,
    "restrictions": {},
    "retries": None,
    "submitting_task": None,
    "tasks": {
        "('getitem-len-chunk-make-timeseries-len-agg-b2a0f46b6880fb4c1fa0a4d979946fbb', 0)": <Serialize>,
        "('len-agg-b2a0f46b6880fb4c1fa0a4d979946fbb', 0)": <Serialize>
    },
    "user_priority": 0
}
```

```
{
    'actors': None,
    'dependencies': {   "('make-timeseries-8ce93c0cff09d8c9400ad766ad5c97c8', 0)": [   ],
                        "('series-groupby-count-agg-3daf8bb058b0f2554555bd6af801602b', 0)": [   "('series-groupby-count-chunk-series-groupby-count-agg-3daf8bb058b0f2554555bd6af801602b', "
                                                                                                '0)'],
                        "('series-groupby-count-chunk-series-groupby-count-agg-3daf8bb058b0f2554555bd6af801602b', 0)": [   "('make-timeseries-8ce93c0cff09d8c9400ad766ad5c97c8', "
                                                                                                                           '0)'],
                        "('series-groupby-sum-agg-63fdaf47d29a5e39d184745b9cc4d1cb', 0)": [   "('series-groupby-sum-chunk-series-groupby-sum-agg-63fdaf47d29a5e39d184745b9cc4d1cb', "
                                                                                              '0)'],
                        "('series-groupby-sum-chunk-series-groupby-sum-agg-63fdaf47d29a5e39d184745b9cc4d1cb', 0)": [   "('make-timeseries-8ce93c0cff09d8c9400ad766ad5c97c8', "
                                                                                                                       '0)'],
                        "('truediv-12c262b165b683952865589ba5601f33', 0)": [   "('series-groupby-sum-agg-63fdaf47d29a5e39d184745b9cc4d1cb', "
                                                                               '0)',
                                                                               "('series-groupby-count-agg-3daf8bb058b0f2554555bd6af801602b', "
                                                                               '0)']},
    'fifo_timeout': '60s',
    'keys': ["('truediv-12c262b165b683952865589ba5601f33', 0)"],
    'loose_restrictions': None,
    'op': 'update-graph',
    'priority': {   "('make-timeseries-8ce93c0cff09d8c9400ad766ad5c97c8', 0)": 0,
                    "('series-groupby-count-agg-3daf8bb058b0f2554555bd6af801602b', 0)": 2,
                    "('series-groupby-count-chunk-series-groupby-count-agg-3daf8bb058b0f2554555bd6af801602b', 0)": 1,
                    "('series-groupby-sum-agg-63fdaf47d29a5e39d184745b9cc4d1cb', 0)": 4,
                    "('series-groupby-sum-chunk-series-groupby-sum-agg-63fdaf47d29a5e39d184745b9cc4d1cb', 0)": 3,
                    "('truediv-12c262b165b683952865589ba5601f33', 0)": 5},
    'resources': None,
    'restrictions': {},
    'retries': None,
    'submitting_task': None,
    'tasks': {   "('make-timeseries-8ce93c0cff09d8c9400ad766ad5c97c8', 0)": {'args': <BINARY>, 'function': <BINARY>},
                 "('truediv-12c262b165b683952865589ba5601f33', 0)": {'args': <BINARY>, 'function': <BINARY>}},
'user_priority': 0
},
```

### Gather
```
{
    'keys': ["('len-agg-04716fd14681d389e2bee6909af649ba', 0)"],
    'op': 'gather',
    'reply': True
}
```

### Release keys
```
{
    'client': 'Client-76300f78-3907-11ea-8345-3ca06793ca95',
    'keys': ["('len-agg-04716fd14681d389e2bee6909af649ba', 0)"],
    'op': 'client-releases-keys'
}
```

### Close client
```
{'op': 'close-client'}
```

### Close stream
```
{'op': 'close-stream'}
```

# Scheduler -> Client
### Key in memory
```
{
    'key': "('len-agg-3b9b81428d1f2322d92caa942990e5a1', 0)",
    'op': 'key-in-memory',
    'type': <BINARY>
}
```
### Task erred
```
{
    'exception': <Serialized>,
    'key': 'delayed_fn1-1978412a-058f-4863-8577-bb04be7c3a03',
    'op': 'task-erred',
    'traceback': <Serialized>
}
```

# Scheduler -> Worker
### Compute task
```
{
    'duration': 0.5,
    'key': "('getitem-len-chunk-make-timeseries-len-agg-3b9b81428d1f2322d92caa942990e5a1', '0)'",
    'op': 'compute-task',
    'priority': (0, 1, 0),
    'task': <Serialized>
}
```
```
{
    'duration': 0.5,
    'key': "('len-agg-3b9b81428d1f2322d92caa942990e5a1', 0)",
    'nbytes': {   "('getitem-len-chunk-make-timeseries-len-agg-3b9b81428d1f2322d92caa942990e5a1', 0)": 32},
    'op': 'compute-task',
    'priority': (0, 1, 1),
    'task': <Serialized>,
    'who_has': {"('getitem-len-chunk-make-timeseries-len-agg-3b9b81428d1f2322d92caa942990e5a1', 0)": ['tcp://127.0.0.1:40621']}
}
```
```
{  
    'args': <BINARY> or <SERIALIZED>,
    'duration': 0.5,
    'function': <BINARY> or <SERIALIZED>,
    'kwargs': <BINARY> or <SERIALIZED> (optional),
    'key': "('make-timeseries-59e73ba154d963cad608e5c0f1b23be3', 0)",
    'op': 'compute-task',
    'priority': (0, 1, 0)
}
```

### Get data
```
{
    'keys': ["('len-agg-3b9b81428d1f2322d92caa942990e5a1', 0)"],
    'max_connections': False,
    'op': 'get_data',
    'reply': True,
    'who': None
}
```
```
{
    'keys': ["('getitem-len-chunk-make-timeseries-len-agg-3b9b81428d1f2322d92caa942990e5a1', '0)'"],
    'op': 'delete-data',
    'report': False
}
```

# Worker -> scheduler
### Task finished
```
{
    'key': "('getitem-ddff2daae92083ec832f9b2e27c5877d', 0)",
    'nbytes': 6144501,
    'op': 'task-finished',
    'startstops': [{'action': 'compute', 'start': 1579254237.0394356, 'stop': 1579254237.0397553}],
    'status': 'OK',
    'thread': 140172621432576,
    'type': <BINARY>,
    'typename': 'pandas.core.frame.DataFrame'
}
```
### Task erred
```
{
    'exception': <Serialized>,
    'status': 'error',
    'thread': '...',
    'key': 'delayed_fn1-1978412a-058f-4863-8577-bb04be7c3a03',
    'op': 'task-erred',
    'traceback': <Serialized>
}
```
(other stream)
```
{
    'data': {"('getitem-ddff2daae92083ec832f9b2e27c5877d', 0)": <SERIALIZE>},
    'status': 'OK'
}
```

### Heartbeat
```
{
    'address': 'tcp://127.0.0.1:43817',
    'metrics': {
        'bandwidth': {'total': 100000000, 'types': {}, 'workers': {}},
        'cpu': 50.0,
        'executing': 0,
        'in_flight': 0,
        'in_memory': 0,
        'memory': 140099584,
        'num_fds': 28,
        'read_bytes': 10578621.95969689,
        'ready': 0,
        'time': 1579254237.331997,
        'write_bytes': 10578621.95969689
    },
    'now': 1579254237.831306,
    'op': 'heartbeat_worker',
    'reply': True
}
```

WTF:
stream-start -> msg must be an array of one
sometimes -> must be a simple msg
