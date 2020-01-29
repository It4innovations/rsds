// Opaque serialized data (MesagePack header and frames)
type Serialized = {};

// Binary MessagePack array
type Bytes = [number];


//---------------------//
// CLIENT -> SCHEDULER //
//---------------------//
type IdentityMsg = {
    "op": "identity",
    "reply": boolean
};
type HeartbeatClientMsg = {
    "op": "heartbeat-client"
};
type RegisterClientMsg = {
    "op": "register-client",
    "client": string,
    "reply": boolean
};

type TaskDef = Serialized | {
    "func": Bytes | Serialized,
    "args": Bytes | Serialized,
    "kwargs": Bytes | Serialized
};
type UpdateGraphMsg = {
    "op": "update-graph",
    "keys": [string],
    "dependencies": { [key: string]: [string] },
    "tasks": { [key: string]: TaskDef },
    "priority": { [key: string]: number },
    "user_priority": number,
    "fifo_timeout": string,
    "loose_restrictions": null,
    "restrictions": {},
    "retries": null,
    "submitting_task": null,
    "resources": null
};
type GatherMsg = {
    "op": "gather",
    "keys": [string],
    "reply": boolean
};
type ReleaseKeysMsg = {
    "op": "client-releases-keys",
    "keys": [string],
    "client": string
};
type CloseClientMsg = {
    "op": "close-client"
};
type CloseStreamMsg = {
    "op": "close-stream"
};


//---------------------//
// SCHEDULER -> CLIENT //
//---------------------//
type IdentityResponseMsg = {
    "op": "identity-response",
    "type": "Scheduler",
    "id": number,
    "workers": { [address: string]: {
        // TODO (worker info)
    }}
};
type KeyInMemoryMsg = {
    "op": "key-in-memory",
    "key": string,
    "type": Bytes
};
type ClientTaskErredMsg = {
    "op": "task-erred",
    "key": string,
    "exception": Serialized,
    "traceback": Serialized
};
/**
 * This message must be inside a message array and the array must have length 1!!!
 */
type RegisterClientResponseMsg = {
    "op": "stream-start"
};


//---------------------//
// SCHEDULER -> WORKER //
//---------------------//
/**
 * Either `task` is present OR `func` + `args` + (optionally) `kwargs` is present.
 */
type ComputeTaskMsg = {
    "op": "compute-task",
    "key": string,
    "duration": number,
    "priority": [number],
    "nbytes?": { [task: string]: number },
    "who_has?": { [task: string]: string },
    "task?": Serialized,
    "func?": Bytes | Serialized,
    "args?": Bytes | Serialized,
    "kwargs?": Bytes | Serialized
};
type GetDataMsg = {
    "op": "get_data",
    "keys": [string],
    "reply?": boolean,
    "report?": boolean,
    "who?": string,
    "max_connections?": number | boolean
};


//---------------------//
// WORKER -> SCHEDULER //
//---------------------//
type TaskFinishedMsg = {
    "op": "task-finished",
    "key": string,
    "nbytes": number,
    "status": "OK",
    "startstops": [{"action": string, "start": number, "stop": number}],
    "thread": number,
    "type": Bytes,
    "typename": string
};
type WorkerTaskErredMsg = {
    "op": "task-erred",
    "key": string,
    "status": "error",
    "exception": Serialized,
    "traceback": Serialized,
    "thread": number
};
type GetDataResponse = {
    "data": { [key: string]: Serialized },
    "status": "OK"
};
type HeartbeatWorkerMsg = {
    "op": "heartbeat_worker",
    "reply": boolean,
    "now": number,
    "address": string,
    "metrics": {
        "bandwidth": {"total": number, "types": {}, "workers": {}},
        "cpu": number,
        "executing": number,
        "in_flight": number,
        "in_memory": number,
        "memory": number,
        "num_fds": number,
        "read_bytes": number,
        "ready": number,
        "time": number,
        "write_bytes": number
    }
};
