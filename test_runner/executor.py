import subprocess
import logging
import os
import signal
import platform
from uuid import uuid4
from typing import Iterable

logger: logging = logging.getLogger()


class Processor:
    def __init__(self, cmd: str, output_file_path: str, cwd: str):
        self.cmd: str = cmd
        self.file_path: str = output_file_path
        self.cwd = cwd

    def kill_process(self, pid):
        logger.warning(f"killing process {pid} manualy")
        code = (
            signal.SIGINT if platform.system() == "Windows" else 9
        )  # send kill -9 on linux
        os.kill(pid, code)

    def abort(self):
        logger.warning("aborting process execution")
        try:
            self.sub_process.kill()
        except Exception as ex:
            logger.error(f"uncaugth exception {ex}")
            logger.debug(f"uncaugth exception {ex}", exc_info=True)
            self.kill_process(self.sub_process.pid)
        try:
            self.sub_process.wait(
                10
            )  # 10 seconds for process to terminate or sending SIGKILL
        except subprocess.TimeoutExpired:
            logger.warning("process resisted termination using kill")
            self.kill_process(self.sub_process.pid)

    def check_done(self) -> bool:
        try:
            self.sub_process.wait(0.01)
            return True
        except subprocess.TimeoutExpired:
            return False

    def start(self):
        output_file = open(self.file_path, "wb")
        self.sub_process = subprocess.Popen(
            self.cmd,
            shell=True,
            stdout=output_file,
            stderr=subprocess.STDOUT,
            cwd=self.cwd,
        )


class Executor:
    colector_process = None
    process_id: str = ""
    output_file_path: str = None
    processor: Processor = None

    def ___init__(self):
        raise Exception("static class")

    @staticmethod
    def exec(cmd, output_file_dir: str, cwd: str) -> None:
        Executor.process_id = str(uuid4())
        logger.info("test")
        logger.info(f"starting process id {Executor.process_id}")
        logger.debug(f"cmd [ {cmd} ] title [ {Executor.process_id} ]")
        file_name = output_file_dir
        Executor.output_file_path = file_name
        Executor.processor = Processor(
            cmd=cmd, output_file_path=Executor.output_file_path, cwd=cwd
        )
        Executor.processor.start()

    @staticmethod
    def is_done() -> bool:
        return Executor.processor.check_done()

    @staticmethod
    def get_exec_file_path() -> str:
        return Executor.output_file_path

    @staticmethod
    def cancell_execution() -> None:
        Executor.processor.abort()

    @staticmethod
    def get_process_id() -> str:
        return Executor.process_id

    @staticmethod
    def get_stdout() -> Iterable:
        def reader():
            with open(Executor.output_file_path, "rb") as file:
                while not Executor.is_done():
                    try:
                        line = file.readline()
                        try:
                            line = line.decode("utf-8")
                        except Exception as ex:
                            pass
                        if line != "" and line != b"":
                            yield line
                    except StopIteration:
                        break
                # exaust remaing lines after process is done
                for line in file:
                    try:
                        line = line.decode("utf-8")
                    except Exception as ex:
                        pass
                    if line != "" and line != b"":
                        yield line

        return iter(reader())
