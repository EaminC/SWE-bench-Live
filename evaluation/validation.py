import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), "launch"))
from launch.core.runtime import SetupRuntime
from launch.scripts.parser import run_parser
import json
from typing import Literal, TypedDict
from fire import Fire
from concurrent.futures import TimeoutError as FutureTimeoutError
import multiprocessing
import time
import psutil
import subprocess

TIMEOUT = 20*60  # 20 minutes per instance (reduced from 90 minutes to skip stalled instances)

def _kill_docker_containers():
    """Kill any running docker containers that may be stuck."""
    try:
        result = subprocess.run(
            ["docker", "ps", "-q"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.stdout.strip():
            container_ids = result.stdout.strip().split('\n')
            for container_id in container_ids:
                try:
                    subprocess.run(
                        ["docker", "kill", container_id],
                        capture_output=True,
                        timeout=5
                    )
                except Exception:
                    pass
    except Exception:
        pass

def _kill_process_tree(pid):
    """Recursively kill a process and all its children."""
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        # Kill children first
        for child in children:
            try:
                child.terminate()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        # Kill parent
        try:
            parent.terminate()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass

class ExecutionResult(TypedDict):
    instance_id: str
    pre_patch_status: dict[str, Literal['pass', 'fail', 'skip']]
    post_patch_status: dict[str, Literal['pass', 'fail', 'skip']]

class ValidationResult(ExecutionResult):
    PASS_TO_PASS: list[str]
    FAIL_TO_PASS: list[str]

def compare(execution_res: ExecutionResult) -> ValidationResult:
    pre_pass = set()
    post_pass = set()
    for test in execution_res["pre_patch_status"].keys():
        assert execution_res["pre_patch_status"][test].lower() in {'pass', 'fail', 'skip'}
        if execution_res["pre_patch_status"][test].lower() == 'pass':
            pre_pass.add(test)
    for test in execution_res["post_patch_status"].keys():
        if execution_res["post_patch_status"][test].lower() == 'pass':
            post_pass.add(test)
    return {
        **execution_res,
        "PASS_TO_PASS": list(pre_pass&post_pass),
        "FAIL_TO_PASS": list(post_pass-pre_pass)
    }

def validate_instance(  
                    instance_id: str,
                    image: str, 
                    rebuild_cmd: str, 
                    test_cmd: str, 
                    print_cmd: str,
                    test_patch: str, 
                    solution_patch: str,
                    parser: str,
                    platform: Literal["windows", "linux"],
                    output_dir: str,
                    ) -> ValidationResult:
    container = None
    try:
        container = SetupRuntime.from_launch_image(image, instance_id, platform, command_timeout=TIMEOUT)
        container.apply_patch(test_patch, verbose=True)
        # Remember to rebuild after modifications to source codes !!!
        container.send_command(rebuild_cmd)
        container.send_command(test_cmd)
        pre_patch_log: str = container.send_command(print_cmd).output
        with open(os.path.join(output_dir, "pre_patch_log.txt"), "w", encoding="utf-8") as f:
            f.write(pre_patch_log)
        pre_patch_status: dict[str, Literal['pass', 'fail', 'skip']] = run_parser(parser, pre_patch_log)
        container.cleanup()
        container = None
    except Exception as e:
        if container:
            try:
                container.cleanup()
            except Exception:
                pass
        raise e

    try:
        container = SetupRuntime.from_launch_image(image, instance_id, platform, command_timeout=TIMEOUT)
        container.apply_patch(test_patch, verbose=True)
        container.apply_patch(solution_patch, verbose=True)
        container.send_command(rebuild_cmd)
        post_patch_status: dict[str, Literal['pass', 'fail', 'skip']] = {}
        post_patch_status_under_inspect: dict[int, dict[str, Literal['pass', 'fail', 'skip']]] = {}
        post_patch_log_accumulate: str = ""
        # 3 validation for stable states
        for check in range(3):
            container.send_command(test_cmd)
            post_patch_log: str = container.send_command(print_cmd).output
            post_patch_log_accumulate += f"eval No.{check} \n\n========  \n\n{post_patch_log} \n\n"
            post_patch_status_under_inspect[check] = run_parser(parser, post_patch_log)
        all_tests = set(post_patch_status_under_inspect[0].keys()) | set(post_patch_status_under_inspect[1].keys()) | set(post_patch_status_under_inspect[2].keys())
        for test in all_tests:
            all_status = [
                post_patch_status_under_inspect[0].get(test, "skip").lower(),
                post_patch_status_under_inspect[1].get(test, "skip").lower(),
                post_patch_status_under_inspect[2].get(test, "skip").lower(),
            ]
            assert all_status[0] in {'pass', 'fail', 'skip'}
            assert all_status[1] in {'pass', 'fail', 'skip'}
            assert all_status[2] in {'pass', 'fail', 'skip'}
            if 'fail' in all_status:
                post_patch_status[test] = 'fail'
            elif 'skip' in all_status:
                post_patch_status[test] = 'skip'
            else:
                post_patch_status[test] = 'pass'
        with open(os.path.join(output_dir, "post_patch_log.txt"), "w", encoding="utf-8") as f:
            f.write(post_patch_log_accumulate)
        container.cleanup()
        container = None
    except Exception as e:
        if container:
            try:
                container.cleanup()
            except Exception:
                pass
        raise e
    
    res: ValidationResult = compare({
        "instance_id": instance_id,
        "pre_patch_status": pre_patch_status,
        "post_patch_status": post_patch_status,
    })
    print(instance_id, ": num of fail_to_pass:", len(res["FAIL_TO_PASS"]), flush=True)
    if len(res["FAIL_TO_PASS"]) > 0:
        print("Find one valid instance:", instance_id, flush=True)
    with open(os.path.join(output_dir, "status.json"), "w", encoding="utf-8") as f:
        json.dump(res, f, indent = True)
    return res

def run_instance(instance: dict[str, str], 
                    platform: Literal["windows", "linux"],
                    output_dir: str,
                    overwrite: bool) -> ValidationResult:
    instance_output_dir = os.path.join(output_dir, instance["instance_id"])
    if (not overwrite) and os.path.exists(os.path.join(instance_output_dir, "status.json")):
        with open(os.path.join(instance_output_dir, "status.json"), encoding="utf-8") as f:
            try:
                report = json.load(f)
                print("Skipping", instance["instance_id"], "num of fail_to_pass:", len(report["FAIL_TO_PASS"]), flush=True)
                return report
            except Exception as e:
                print(e, flush=True)
                pass
    os.makedirs(instance_output_dir, exist_ok=True)
    return validate_instance(
                instance["instance_id"],
                instance["docker_image"],
                " ; ".join(instance["rebuild_cmds"]),
                " ; ".join(instance["test_cmds"]),
                " ; ".join(instance["print_cmds"]),
                instance["test_patch"],
                instance["patch"],
                instance.get("log_parser", instance.get("parser", "")),
                platform,
                instance_output_dir
            )

def run_instances(instances: list[dict[str, str]], 
                    platform: Literal["windows", "linux"], 
                    workers: int,
                    output_dir: str,
                    overwrite: bool) -> list[dict[str, str]]:
    results = []
    INSTANCE_TIMEOUT = 20 * 60  # 20 minutes per instance

    def _proc_target(instance, platform, output_dir, overwrite, q):
        try:
            res = run_instance(instance, platform, output_dir, overwrite)
            q.put((True, res))
        except Exception as e:
            q.put((False, str(e)))

    running: list[tuple[multiprocessing.Process, multiprocessing.Queue, dict, float]] = []

    for instance in instances:
        # Wait until there's a free worker slot
        while len(running) >= workers:
            # check running processes and collect finished ones
            new_running = []
            for p, q, inst, start_ts in running:
                if p.is_alive():
                    new_running.append((p, q, inst, start_ts))
                else:
                    # finished; retrieve result
                    try:
                        ok, payload = q.get_nowait()
                    except Exception:
                        ok, payload = (False, "No result from worker")
                    if ok:
                        results.append({**inst, **payload})
                    else:
                        print(f"Error processing instance {inst.get('instance_id', '<unknown>')}: {payload}", flush=True)
            running = new_running
            if len(running) >= workers:
                time.sleep(1)

        q = multiprocessing.Queue()
        p = multiprocessing.Process(target=_proc_target, args=(instance, platform, output_dir, overwrite, q))
        p.start()
        running.append((p, q, instance, time.time()))

    # wait for remaining processes with timeout handling
    for p, q, inst, start_ts in running:
        elapsed = time.time() - start_ts
        remaining = max(0, INSTANCE_TIMEOUT - int(elapsed))
        p.join(timeout=remaining)
        if p.is_alive():
            try:
                p.terminate()
            except Exception:
                pass
            try:
                p.join(timeout=5)
            except Exception:
                pass
            print(f"TIMEOUT: Instance {inst.get('instance_id')} exceeded {INSTANCE_TIMEOUT} seconds. Skipping...", flush=True)
            try:
                _kill_docker_containers()
            except Exception as e:
                print(f"Error during cleanup: {e}", flush=True)
        else:
            try:
                ok, payload = q.get_nowait()
            except Exception:
                ok, payload = (False, "No result from worker")
            if ok:
                results.append({**inst, **payload})
            else:
                print(f"Error processing instance {inst.get('instance_id', '<unknown>')}: {payload}", flush=True)

    return results


def main(
            input_dir: str, 
            platform: Literal["windows", "linux"], 
            workers: int, 
            output_dir: str, 
            overwrite: int,
        ):
    with open(input_dir, encoding="utf-8") as f:
        instances = [json.loads(i) for i in f]
    print(f"Loaded {len(instances)} instances.")
    results = run_instances(instances, platform, workers, output_dir, overwrite != 0)
    filtered_res = [i for i in results if len(i["FAIL_TO_PASS"]) > 0]
    print(f"Saved {len(filtered_res)} valid instances.")
    with open(os.path.join(output_dir, "validated_instances.jsonl"), "w", encoding="utf-8") as f:
        for i in filtered_res:
            f.write(json.dumps(i)+"\n")

if __name__ == "__main__":
    Fire(main)
