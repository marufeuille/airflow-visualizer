from importlib import import_module
import pathlib
import os
import sys
import json
from importlib import import_module
from importlib.abc import MetaPathFinder
from importlib.util import spec_from_file_location

def _get_pyfile_list(PATH, prefix=""):
    lists = os.listdir(PATH)
    pyfiles = list(
        map(
            lambda x: "{}{}".format(prefix, x),
            filter(
                lambda x: os.path.isfile(os.path.join(PATH,x)) and x.endswith(".py"),
                lists
            )
        )
    )
    dirs = list(filter(lambda x:  os.path.isdir(os.path.join(PATH,x)), lists))
    for dir in dirs:
        print(dir)
        pyfiles += _get_pyfile_list("{}/{}".format(PATH,dir), "{}{}/".format(prefix, dir))
    print(pyfiles)
    return pyfiles

gid_map = {}

def _get_operator_info(dag_id, op, memo,id):

    if dag_id not in gid_map.keys():
        if len(gid_map.keys()) == 0:
            val = 1
        else:
            val = max(gid_map.values()) + 1
        gid_map[dag_id] = val
    gid = gid_map[dag_id]
    info = {
        "id": id,
        "group": gid,
        "dag_id": dag_id,
        "task_id": op.task_id,
        "type": op.task_type,
        "next_dag_id": op.trigger_dag_id if op.task_type == "TriggerDagRunOperator" else dag_id,
        "nexts": []
    }

    if info["task_id"] in memo.keys():
        return None, -1

    memo[info["task_id"]] = info

    nodes = [info]

    n = id + 1
    for op_down in op.downstream_list:
        down_streams, nn = _get_operator_info(dag_id, op_down, memo, n)
        if down_streams is None:
            continue
        n = nn
        nodes += down_streams
        info["nexts"].append(down_streams[0]["id"])

    return nodes, n

def _get_dag_info(filename, id=1):
    dags_info = []
    try:
        py_module = import_module(filename.replace("/", ".")[:-3])
        dag = py_module.dag
        for op in dag.roots:
            nodes, id = _get_operator_info(dag.dag_id, op, {}, id)
            dags_info += nodes
        return dag.dag_id, dags_info, id
    except AttributeError:
        dags_info = None
        pass
    return "", dags_info, id

def _create_vis_dataset(operators):
    nodes = []
    edges = []
    for op in operators:
        for to in op["nexts"]:
            edges.append({"from": op["id"], "to": to, "arrows": "to"})
        nodes.append({
            "id": op["id"],
            "group": op["group"],
            "label": "{}\n{}".format(op["task_id"], op["type"]),
            "mass": 2
        })

        if op["type"] == "TriggerDagRunOperator":
            edges.append({"from": op["id"], "to": dag_dict[op["next_dag_id"]][0]["id"], "arrows": "to"})

    return nodes, edges

if __name__ == "__main__":
    DAG_HOME = sys.argv[1]
    p = pathlib.Path(DAG_HOME)
    ABS_DAG_HOME = str(p.resolve())
    sys.path.append(ABS_DAG_HOME)
    candidate_dags = _get_pyfile_list(DAG_HOME)
    id = 1
    all_dags = []
    dag_dict = {}
    for filename in candidate_dags:
        print(filename)
        dag_id, nodes, id = _get_dag_info(filename, id)
        if nodes is None:
            continue
        all_dags += nodes
        dag_dict[dag_id] = nodes

    nodes, edges = _create_vis_dataset(all_dags)
    with open("./output/nodes.json", "w") as f:
        f.write(json.dumps(nodes))
    with open("./output/edges.json", "w") as f:
        f.write(json.dumps(edges))

