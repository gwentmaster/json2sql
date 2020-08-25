#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2020-06-30 10:19:04
# @Author  : gwentmaster(1950251906@qq.com)
# I regret in my life


"""summary
"""


import json
import os
import re
from collections import defaultdict
from typing import Any, Dict, List, Generator, Union

import click


TEMPLATE = "INSERT INTO {table} ({columns})\nVALUES ({values});\n\n"


def judge_file(file: str, type_: str) -> bool:

    if not os.path.isfile(file):
        return False
    if not file.endswith(f".{type_}"):
        return False
    return True


def judge_type(data: str) -> Union[int, float, str]:

    try:
        result = int(data)
        return result
    except ValueError:
        pass
    try:
        result = float(data)
        return result
    except ValueError:
        pass
    if (data[0] in ("'", "\"")) and (data[-1] == data[0]):
        return data[1:-1]
    return data


def gen_value(dic: Dict, keys: List[str], need_repr: bool = True) -> Generator:
    for key in keys:
        data = dic[key]
        if data is None:
            yield "NULL"
            continue
        if data is True:
            yield "1"
            continue
        if data is False:
            yield "0"
            continue
        if need_repr:
            yield repr(dic[key])
        else:
            yield str(dic[key])


def json2sql(json_file: str, target: str = "data.sql") -> None:

    if not judge_file(json_file, "json"):
        print("not a json file")
        return

    with open(json_file, encoding="utf8") as f:
        data = json.load(f)

    sql = open(target, "wb")
    for table in data:
        for obj_dic in data[table]:
            keys = sorted(obj_dic)
            columns = ", ".join(keys)
            values = ", ".join(gen_value(obj_dic, keys))
            sql.write(TEMPLATE.format(table=table,
                                      columns=columns,
                                      values=values).encode("utf8"))
    sql.close()


def sql2json(sql_file: str, target: str = "data.json") -> None:

    if not judge_file(sql_file, "sql"):
        print("not a sql file")
        return

    pattern = re.compile(r"INSERT\sINTO\s(.+)\s\((.+)\)[\s\n]+VALUES\s\((.+)\)")

    data_dic = defaultdict(list)

    with open(sql_file, encoding="utf8") as f:
        sqls = f.read().split(";")
        for sql in sqls:
            search = pattern.search(sql)
            if search:
                table = search.group(1)
                columns = search.group(2).split(", ")
                raw_values = search.group(3).split(", ")

                values = []
                for v in raw_values:
                    values.append(judge_type(v))
                data_dic[table].append(dict(zip(columns, values)))

    with open(target, "wb") as t:
        t.write(json.dumps(data_dic, ensure_ascii=False, indent=2).encode("utf8"))


def _json2csv(data: Dict, key: str, target: str):

    columns = set()
    data_li = []
    for d in data[key]:
        columns.update(d.keys())
        temp_dic = defaultdict(lambda: "")  # type: Dict[str, Any]
        temp_dic.update(d)
        data_li.append(temp_dic)
    columns = sorted(columns)
    t = open(target, "wb")
    t.write(f'{",".join(columns)}\n'.encode("utf-8"))
    for d in data_li:
        t.write(
            f'{",".join(gen_value(d, columns, need_repr=False))}\n'
            .encode("utf-8")
        )
    t.close()


def json2csv(json_file: str, target: str = "data.csv") -> None:

    if not judge_file(json_file, "json"):
        print("not a json file")
        return

    with open(json_file, encoding="utf8") as f:
        data = json.load(f)

    keys = list(data.keys())
    if len(keys) > 1:
        while True:
            try:
                for i, key in enumerate(keys):
                    print(f"{i}. {key}")
                print(f"{len(keys)}. 全部")
                choose = int(input("请选择想转换的数据: "))
            except ValueError:
                pass
            else:
                if choose in range(len(keys) + 1):
                    break
        if choose == len(keys):
            for key in keys:
                _json2csv(data, key, f"{key}.csv")
            return None
    else:
        choose = 0

    _json2csv(data, keys[choose], target)


def csv2json(csv_file: str, target: str = "data.json", table: str = "data") -> None:

    if not judge_file(csv_file, "csv"):
        print("not a csv file")
        return

    data = {table: []}  # type: Dict[str, List]
    with open(csv_file, encoding="utf8") as f:
        for i, line in enumerate(f.readlines()):
            line = line.strip()
            if i == 0:
                columns = line.split(",")
                continue
            values = []
            for v in line.split(","):
                values.append(judge_type(v))
            data[table].append(dict(zip(columns, values)))

    with open(target, "wb") as t:
        t.write(json.dumps(data, ensure_ascii=False, indent=2).encode("utf8"))


@click.command()
@click.argument("file")
@click.option(
    "--reverse",
    is_flag=True,
    default=False,
    help="是否将其他文件转成json"
)
@click.option(
    "--csv",
    is_flag=True,
    default=False,
    help="json与csv转化"
)
@click.option(
    "--out",
    type=str,
    default=None,
    help="输出文件名",
    show_default=False
)
def main(
    file: str,
    reverse: bool,
    csv: bool,
    out: str
):
    if out is None:
        out = "data"

    if (reverse is False) and (csv is False):
        json2sql(file, out.rstrip(".sql") + ".sql")
    elif (reverse is True) and (csv is False):
        sql2json(file, out.rstrip(".json") + ".json")
    elif (reverse is False) and (csv is True):
        json2csv(file, out.rstrip(".csv") + ".csv")
    else:
        csv2json(file, out.rstrip(".json") + ".json")


if __name__ == "__main__":

    main()
