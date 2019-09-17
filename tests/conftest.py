import os
import os.path
import signal
import subprocess
import time
import sys

import psutil
import pytest

PYTEST_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(PYTEST_DIR)
RSDS_BIN = os.path.join(ROOT, "target", "debug", "rsds")


def check_free_port(port):
    assert isinstance(port, int)
    for conn in psutil.net_connections('tcp'):
        if conn.laddr.port == port and conn.status == "LISTEN":
            return False
    return True


class Env:

    def __init__(self, work_path):
        self.processes = []
        self.cleanups = []
        self.work_path = work_path

    def start_process(self, name, args, env=None, catch_io=True):
        logfile = (self.work_path / name).with_suffix(".out")
        if catch_io:
            with open(logfile, "w") as out:
                p = subprocess.Popen(args,
                                     preexec_fn=os.setsid,
                                     stdout=out,
                                     stderr=subprocess.STDOUT,
                                     cwd=self.work_path,
                                     env=env)
        else:
            p = subprocess.Popen(args,
                                 cwd=str(self.work_path),
                                 env=env)
        self.processes.append((name, p))
        return p

    def kill_all(self):
        for fn in self.cleanups:
            fn()
        for n, p in self.processes:
            # Kill the whole group since the process may spawn a child
            if not p.poll():
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)


class RsdsEnv(Env):
    default_listen_port = 17070

    def __init__(self, work_dir):
        Env.__init__(self, work_dir)
        self.server = None
        self.workers = {}
        self.id_counter = 0
        self.do_final_check = True

    def no_final_check(self):
        self.do_final_check = False

    def start_worker(self, ncpus, port):
        worker_id = self.id_counter
        self.id_counter += 1
        name = "worker{}".format(worker_id)
        args = ["dask-worker", "localhost:{}".format(port)]
        env = os.environ.copy()
        env["PYTHONPATH"] = PYTEST_DIR + ":" + env.get("PYTHONPATH", "")
        self.workers[name] = self.start_process(name, args, env)

    def start(self,
              workers=(),
              port=None):
        print("Starting rsds env in ", self.work_path)

        """
        Start infrastructure: server & n governors
        """

        if self.server:
            raise Exception("Server is already running")

        port = port or self.default_listen_port

        if not check_free_port(port):
            raise Exception("Trying to spawn server on port {}, but it is not free".format(port))

        env = os.environ.copy()
        env["RUST_LOG"] = "trace"
        env["RUST_BACKTRACE"] = "1"

        args = (RSDS_BIN, "--port", str(port))
        self.server = self.start_process("server", args, env=env)
        assert self.server is not None

        it = 0
        while check_free_port(port):
            time.sleep(0.05)
            self.check_running_processes()
            it += 1
            if it > 100:
                raise Exception("Server not started after 5")

        for cpus in workers:
            self.start_worker(cpus, port=port)

        time.sleep(0.2)  # TODO: Replace with real check that worker is

        self.check_running_processes()
        return "127.0.0.1:{}".format(port)

    def check_running_processes(self):
        """Checks that everything is still running"""
        for worker_name, worker in self.workers.items():
            if worker.poll() is not None:
                raise Exception(
                    "{0} crashed (log in {1}/{0}.out)".format(worker_name, self.work_path))

        if self.server and self.server.poll() is not None:
            self.server = None
            raise Exception(
                "Server crashed (log in {}/server.out)".format(self.work_path))

    def new_client(self):
        pass

    def final_check(self):
        pass

    def close(self):
        pass


@pytest.yield_fixture(autouse=False, scope="function")
def rsds_env(tmp_path):
    """Fixture that allows to start Rain test environment"""
    os.chdir(tmp_path)
    env = RsdsEnv(tmp_path)
    yield env
    time.sleep(0.2)
    try:
        env.final_check()
        env.check_running_processes()
    finally:
        env.close()
        env.kill_all()
        # Final sleep to let server port be freed, on some slow computers
        # a new test is starter before the old server is properly cleaned
        time.sleep(0.02)
