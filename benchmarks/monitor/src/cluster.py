import json
import logging
import os
import socket
import subprocess

HOSTNAME = socket.gethostname()
CLUSTER_FILENAME = "cluster.json"


class Cluster:
    @staticmethod
    def deserialize(file):
        data = json.load(file)
        return Cluster(data["workdir"], data["nodes"])

    def __init__(self, workdir, nodes=None):
        if nodes is None:
            nodes = {}

        self.workdir = workdir
        self.nodes = nodes

    def add(self, node, pid, cmd, key=None, **kwargs):
        data = {
            "cmd": cmd,
            "pid": pid
        }
        if key:
            data["key"] = key
        data.update(kwargs)

        self.nodes.setdefault(node, []).append(data)

    def processes(self):
        for (node, processes) in self.nodes.items():
            for process in processes:
                yield (node, process)

    def kill(self, kill_fn=None):
        if kill_fn is None:
            kill_fn = lambda node, process: kill_process(node, process["pid"])

        for (node, process) in self.processes():
            kill_fn(node, process)

    def get_processes_by_key(self, key):
        def gen():
            for (node, process) in self.processes():
                if process.get("key") == key:
                    yield (node, process)
        return list(gen())

    def get_monitor_for_node(self, node):
        processes = self.nodes.get(node, [])
        for process in processes:
            if process.get("key") == "monitor":
                return process
        return None

    def serialize(self, file):
        json.dump({
            "workdir": self.workdir,
            "nodes": self.nodes
        }, file, indent=2)


def is_local(host):
    return host == HOSTNAME or host == "localhost"


def start_process(commands, host=None, workdir=None, modules=(), name=None, env=None, pyenv=None, init_cmd=()):
    if not workdir:
        workdir = os.getcwd()
    workdir = os.path.abspath(workdir)

    init_cmd = list(init_cmd)
    if modules:
        init_cmd += [f"ml {' '.join(modules)}"]

    if pyenv:
        assert os.path.isabs(pyenv)
        init_cmd += [f"source {pyenv}/bin/activate"]

    args = []
    if env:
        args += ["env"]
        for (key, val) in env.items():
            args += [f"{key}={val}"]

    args += [str(cmd) for cmd in commands]

    if not name:
        name = "process"
    output = os.path.join(workdir, name)

    stdout_file = f"{output}.out"
    stderr_file = f"{output}.err"
    command = f"""
cd {workdir}
{' && '.join(init_cmd)}
ulimit -c unlimited
{' '.join(args)} > {stdout_file} 2> {stderr_file} &
ps -ho pgid $!
""".strip()

    cmd_args = []
    if host:
        cmd_args += ["ssh", host]
    else:
        cmd_args += ["setsid"]
    cmd_args += ["/bin/bash"]
    process = subprocess.Popen(cmd_args, cwd=workdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    out, err = process.communicate(command.encode())
    pid = out.strip()

    if not pid:
        logging.error(f"Process startup failed with status: {process.returncode}, stderr: {err.decode()}, stdout: {out.decode()}")
        if os.path.isfile(stderr_file):
            with open(stderr_file) as f:
                logging.error("".join(f.readlines()))
        raise Exception(f"Process startup failed on {host if host else 'localhost'}: {command}")
    return (int(pid), command)


def kill_process(host, pid, signal="TERM"):
    assert signal in ("TERM", "KILL", "INT")
    logging.info(f"Killing PGID {pid} on {host}")
    args = ["kill", f"-{signal}", f"-{pid}"]
    if not is_local(host):
        args = ["ssh", host, "--"] + args
    res = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res.returncode != 0:
        logging.error(f"error: {res.returncode} {res.stdout.decode().strip()} {res.stderr.decode().strip()}")
        return False
    return True
