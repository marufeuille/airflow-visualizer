from importlib import import_module
import pathlib
import os
import sys
import json
from importlib import import_module
from importlib.abc import MetaPathFinder
from importlib.util import spec_from_file_location

from airflow import DAG

def _get_pyfile_list(PATH, prefix=""):
    '''指定されたディレクトリ内を再帰的に調べ、その中にあるpythonファイルの一覧を作る
    :param str PATH: 検索したいパス
    :param str prefix: 起点からたどったディレクトリを/で連結した文字列
    :return: 指定したディレクトリ内にあったPythonファイルの一覧
    :rtype: list
    '''
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
    '''渡されたオペレータの情報を取り出し、再帰的に検索する
    :param str dag_id: このオペレータが存在するDAGのID
    :param Operator op: Airflow Operator
    :param dict memo: これまでに発見したOperatorの情報
    :param int id: id
    :return: このオペレータより後ろにあるオペレータの情報をつないだ連結リスト
    :rtype: list
    '''

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
        return None, memo[info["task_id"]]["id"]

    memo[info["task_id"]] = info

    nodes = [info]

    n = id + 1
    for op_down in op.downstream_list:
        down_streams, nn = _get_operator_info(dag_id, op_down, memo, n)
        if down_streams is None:
            info["nexts"].append(nn)
            continue
        n = nn
        nodes += down_streams
        info["nexts"].append(down_streams[0]["id"])

    return nodes, n

def _get_dag_info(filename, id=1):
    '''指定されたPythonファイルにDagの定義があるかを調べ、あれば解析を行う
    仕組み上、globalにあるDAGオブジェクトしか解析できない。
    :param str filename: 解析対象のファイル名
    :param int id: オペレータにつけるID（通し番号）
    :return: dag_id, dagから抽出したオペレータ情報の連結リスト, 最後のID
    :rtype: tuple(str, list, int)
    '''
    dags_info = []
    py_module = import_module(filename.replace("/", ".")[:-3])

    for item in py_module.__dict__.items():
        if not isinstance(item[1], DAG):
            continue
        dag = item[1]
        for op in dag.roots:
            nodes, id = _get_operator_info(dag.dag_id, op, {}, id)
            dags_info += nodes
        return dag.dag_id, dags_info, id

    return "", dags_info, id

def _create_vis_dataset(operators_info):
    '''オペレータの情報をもとに、visjs向けのdataset(json)を作成する
    :param list operators_info: _get_dag_infoで生成されるリスト
    :return: visjsのnode, visjsのedges
    :rtype: tuple(list, list)
    '''
    nodes = []
    edges = []
    for op in operators_info:
        for to in op["nexts"]:
            edges.append({"from": op["id"], "to": to, "arrows": "to"})
        nodes.append({
            "id": op["id"],
            "group": op["group"],
            "label": "{}\n{}".format(op["task_id"], op["type"]),
            "mass": 2
        })

        if op["type"] == "TriggerDagRunOperator":
            edges.append({"from": op["id"], "to": dag_dict[op["next_dag_id"]][0]["id"], "arrows": "to", "dashes": "true"})

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

