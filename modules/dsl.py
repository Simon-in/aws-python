"""
Transforms a string containing expressions and renders it using SQLite functions.

Built-In Scalar SQL Functions:
    > https://www.sqlite.org/lang_corefunc.html

Examples:
    >>> dsl.render("s3://landing/appddm_{{date('now', 'localtime')}}/{{strftime('%Y%m%d','now', 'localtime')}}.xlsx")
    's3://landing/appddm_2023-01-16/20230116.xlsx'

"""

import re
import sqlite3
from typing import List, Dict, Any

__module__ = ["render"]
__all__ = ["render"]

_PAT = re.compile(r"{{[^{}]*}}")


def run_query(query) -> Dict[str, Any]:
    with sqlite3.connect(":memory:") as connection:
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            row = cursor.fetchone()
        except sqlite3.OperationalError as e:
            raise ValueError(f"Execute query failed: {query}, error: {e}")
        if row:
            return dict(row)
        return dict()


def expression_parse(string) -> List[str]:
    exps = re.findall(_PAT, string)
    return exps


def expression_query(exp):
    function = exp.replace("{", "").replace("}", "").strip()
    function = _alias.get(function.upper(), function)
    try:
        query = f"SELECT {function} AS result"
        ret = run_query(query)
        if ret:
            return ret["result"]
        raise ValueError(f"Invalid expression: {exp}, result is None")
    except Exception:
        return exp


_alias = {
    "CURRENT_TIME": "time('now')",   # 09:33:18
    "CURRENT_TIMESTAMP": "unixepoch('now')",   # 1675848798
    "CURRENT_DATE": "strftime('%Y%m%d', 'now')",  # 2023-02-08
    "CURRENT_DATETIME": "strftime('%Y%m%d%H%M%S', 'now')",   # 2023-02-08 09:33:18
}


def render(string: str):
    """A string containing expressions can be dynamically rendered based on SQLite functions.
    To ensure proper evaluation, expressions must be encapsulated within double curly braces '{{}}'.

    # >>> render("s3://landing/appddm_{{date('now', 'localtime')}}/{{strftime('%Y%m%d','now', 'localtime')}}.xlsx")
    # 's3://landing/appddm_2023-02-08/20230208.xlsx'

    :param string:
    """
    exps = expression_parse(string)
    results = {exp: expression_query(exp) for exp in exps}
    for k, v in results.items():
        string = string.replace(k, str(v))
    return string


if __name__ == "__main__":
    print(render("s3://bucket/{{demo}}.csv"))
    print(render("s3://bucket/demo/{{current_date}}.csv"))
    print(render("s3://bucket/demo/{{current_datetime}}.csv"))
    print(render("s3://bucket/demo/{{current_time}}.csv"))
    print(render("s3://bucket/demo/{{current_timestamp}}.csv"))
    s = """s3://bucket/demo/{{date('now', 'localtime')}}/{{strftime('%Y%m%d%H%M%S', 'now')}}.xlsx"""
    t = """{"meatadata_{{strftime('%Y_%m_%d', 'now')}}.xlsx": {'audit_{{current_date}}': 'select * from metadata.audit_log', 'ddl_{{current_date}}': 'select * from metadata.ddl_history_log'},  "metadata.csv": "select * from metadata.data_load_history_log"}"""
    print(render(t))
