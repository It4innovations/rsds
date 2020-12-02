// Opaque serialized data (MessagePack header and frames)
type Serialized = {};

// Binary MessagePack array
type Bytes = [number];

type AnyMsg = {};

// Global types
type WorkerMetrics = {
    "bandwidth": {
        "total": number,
        "types": {},
        "workers": {}
    },
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
};
type TaskDef = Serialized | {
    "func": Bytes | Serialized,
    "args": Bytes | Serialized,
    "kwargs": Bytes | Serialized
};
type DataMap = { [key: string]: Serialized };
type GetDataResponseType = {
    "data": DataMap,
    "status": "OK"
};
type WorkerTaskState =
    "waiting"
    | "ready"
    | "executing"
    | "memory"
    | "error"
    | "rescheduled"
    | "constrained"
    | "long-running";

type CloseStreamMsg = {
    "op": "close-stream"
};

/**
 * UNINITIATED MESSAGES
 *
 * The following messages are sent on a connection that was not initiated with RegisterClient/RegisterWorker yet.
 */

//---------------------//
// CLIENT -> SCHEDULER //
//---------------------//
type IdentityMsg = {
    "op": "identity",
    "reply": boolean
};
type RegisterClientMsg = {
    "op": "register-client",
    "client": string,
    "reply": boolean
};
type GatherMsg = {
    "op": "gather",
    "keys": [string],
    "reply": boolean
};
type ScatterMsg = {
    "op": "scatter",
    "client": string,
    "broadcast": boolean,
    "data": DataMap,
    "reply": boolean,
    "timeout": number,
    "workers": [string] | null
};
type NcoresMsg = {
    "op": "ncores",
    "reply": boolean,
    "workers": {}
};
type CancelMsg = {
    "op": "cancel",
    "keys": [string],
    "client": string,
    "force": boolean,
    "reply": boolean
};
type WhoHasMsg = {
    "op": "who_has",
    "keys": [string] | null,
    "reply": boolean
};
type ProxyMsg = {
    "op": "proxy",
    "worker": string,
    "msg": AnyMsg,
    "reply": boolean
};


//---------------------//
// SCHEDULER -> CLIENT //
//---------------------//
type IdentityMsgResponse = {
    "op": "identity-response",
    "type": "Scheduler",
    "id": number,
    "workers": {
        [address: string]: {
            'host': string,
            'id': string,
            'last_seen': number,
            'local_directory': string,
            'memory_limit': number,
            'metrics': WorkerMetrics,
            'name': string,
            'nanny': string,
            'nthreads': number,
            'resources': {},
            'services': { 'dashboard': number },
            'type': 'Worker'
        }
    }
};
/**
 * This message must be inside a message array and the array must have length 1!!!
 */
type RegisterClientMsgResponse = {
    "op": "stream-start"
};
type NcoresMsgResponse = {
    [worker: string]: number
};
type SchedulerGetDataMsgResponse = GetDataResponseType;
type ScatterMsgResponse = [string];
type WhoHasMsgResponse = {
    [key: string]: [string]
};


//---------------------//
// WORKER -> SCHEDULER /
//---------------------//
type HeartbeatWorkerMsg = {
    "op": "heartbeat_worker",
    "reply": boolean,
    "now": number,
    "address": string,
    "metrics": WorkerMetrics
};
type RegisterWorkerMsg = {
    'op': 'register-worker',
    'address': string,
    'extra': {},
    'keys': [string],
    'local_directory': string,
    'memory_limit': number,
    'metrics': WorkerMetrics,
    'name': string,
    'nanny': string,
    'nbytes': {},
    'now': number,
    'nthreads': number,
    'pid': number,
    'reply': boolean,
    'resources': {},
    'services': { 'dashboard': number },
    'types': {}
};
type WorkerGetDataResponse = GetDataResponseType;
type UpdateDataMsgResponse = {
    "status": "OK",
    "nbytes": { [key: string]: number },
};


//---------------------//
// SCHEDULER -> WORKER //
//---------------------//
type RegisterWorkerResponseMsg = {
    status: String,
    time: number,
    heartbeat_interval: number,
    worker_plugins: [string]
};
type GetDataMsg = {
    "op": "get_data",
    "keys": [string],
    "reply?": boolean,
    "report?": boolean,
    "who?": string,
    "max_connections?": number | boolean
};
type GetDataResponseConfirm = "OK";
type UpdateDataMsg = {
    "op": "update_data",
    "data": DataMap,
    "reply": boolean,
    "report": boolean,
};
type ActorExecuteMsg = {
    "op": "actor_execute",
    "function": string,
    "actor": string,
    "args": [Serialized],
    "kwargs": { [key: string]: Serialized }
};
type ActorAttributeMsg = {
    "op": "actor_attribute",
    "attribute": string,
    "actor": string,
};


/**
 * INITIATED MESSAGES
 *
 * The following messages are sent after RegisterClient/RegisterWorker.
 */

//---------------------//
// CLIENT -> SCHEDULER //
//---------------------//
type HeartbeatClientMsg = {
    "op": "heartbeat-client"
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
    "resources": null,
    "actors": boolean
};
type ClientReleasesKeysMsg = {
    "op": "client-releases-keys",
    "keys": [string],
    "client": string
};
type ClientDesiresKeysMsg = {
    "op": "client-desires-keys",
    "keys": [string],
    "client": string
}
type CloseClientMsg = {
    "op": "close-client"
};
type ClientCloseStreamMsg = CloseStreamMsg;


//---------------------//
// SCHEDULER -> CLIENT //
//---------------------//
type KeyInMemoryMsg = {
    "op": "key-in-memory",
    "key": string,
    "type?": Bytes
};
type ClientTaskErredMsg = {
    "op": "task-erred",
    "key": string,
    "exception": Serialized,
    "traceback": Serialized
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
    "actor": boolean,
    "nbytes?": { [task: string]: number },
    "who_has?": { [task: string]: string },
    "task?": Serialized,
    "func?": Bytes | Serialized,
    "args?": Bytes | Serialized,
    "kwargs?": Bytes | Serialized
};
type StealRequestMsg = {
    "op": "steal-request",
    "key": string
};


//---------------------//
// WORKER -> SCHEDULER //
//---------------------//
type TaskFinishedMsg = {
    "op": "task-finished",
    "key": string,
    "nbytes": number,
    "status": "OK",
    "startstops": [{ "action": string, "start": number, "stop": number }],
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
type KeepAliveMsg = {
    "op": "keep-alive",
};
type CloseGracefullyMsg = {
    "op": "close_gracefully",
    "reply": boolean
};
type StealResponseMsg = {
    "op": "steal-response",
    "key": string,
    "state": WorkerTaskState
};
type ReleaseMsg = {
    "op": "release",
    "key": string,
    "cause": null
};
type WorkerCloseStreamMsg = CloseStreamMsg;
