import sys
import json
from executor import Executor
from tabulate import tabulate
import os

cmd = "go test -v -json {args}"


def process_result(line_data):
    if "Elapsed" in line_data and "Test" in line_data:
        test_summary = {
            "Test": line_data["Test"],
            "State": line_data["Action"],
            "Elapsed": line_data["Elapsed"],
        }
        return test_summary


def main():
    global cmd
    cmd = cmd.format(args="".join([f"{i} " for i in sys.argv[1:]]))
    Executor.exec(cmd, "./tests_results.txt", os.getcwd())
    tests_summary = {}
    for line in Executor.get_stdout():
        line2 = str(line).replace("\n", "")
        try:
            line_data = json.loads(line2)
        
            if line_data["Action"] == "output":
                sys.stdout.write(line_data["Output"])
            if line_data["Action"] == "pass" or line_data["Action"] == "fail":
                resp = process_result(line_data)
                if resp:
                    tests_summary[resp["Test"]] = resp
        except Exception as ex:
            print(ex)
            print(line)
    print(tabulate([tests_summary[i] for i in tests_summary], headers="keys"))


if __name__ == "__main__":
    main()
