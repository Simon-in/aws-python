import copy
import json
from typing import Dict, Any, List

_escape_table = [chr(x) for x in range(128)]
_escape_table[0] = "\\0"
_escape_table[ord("\\")] = "\\\\"
_escape_table[ord("\n")] = "\\n"
_escape_table[ord("\r")] = "\\r"
_escape_table[ord("\032")] = "\\Z"
_escape_table[ord('"')] = '\\"'
_escape_table[ord("'")] = "\\'"


def escape_string(value, mapping=None):
    """escapes *value* without adding quote.

    Value should be unicode
    """
    return value.translate(_escape_table)


class SQLBuilder:
    @staticmethod
    def _kv_exp(items: Dict[str, Any], where=False, joint=", ") -> str:
        _items = copy.deepcopy(items)
        exps = []
        for k, v in _items.items():
            if isinstance(v, str):
                v = escape_string(v)
                exps.append(f"{k}='{v}'")
            elif v is None:
                if where:
                    exps.append(f"{k} IS NULL")
                else:
                    exps.append(f"{k}=NULL")
            else:
                exps.append(f"{k}={json.dumps(v)}")
        return joint.join(exps)

    @staticmethod
    def _where_exp(items: Dict[str, Any]) -> str:
        return SQLBuilder._kv_exp(items, where=True, joint=" AND ")

    @staticmethod
    def insert(table: str, items: Dict[str, Any]) -> str:
        for k, v in items.items():
            if isinstance(v, str):
                v = escape_string(v)
                items[k] = f"'{v}'"
            elif v is None:
                items[k] = "NULL"
            else:
                items[k] = json.dumps(v)
        columns = ", ".join(items.keys())
        values = ", ".join(items.values())
        sql = f"INSERT INTO {table} ({columns}) VALUES ({values})"
        return sql

    @staticmethod
    def update(table: str, keys: Dict[str, Any], items: Dict[str, Any]) -> str:
        sql = f"UPDATE {table} SET {SQLBuilder._kv_exp(items)} WHERE {SQLBuilder._where_exp(keys)}"
        return sql

    @staticmethod
    def delete(table: str, keys: Dict[str, Any]) -> str:
        sql = f"DELETE FROM {table} WHERE {SQLBuilder._where_exp(keys)}"
        return sql

    @staticmethod
    def select(
        table: str, keys: Dict[str, Any], columns: List[str] = None, limit=1000
    ) -> str:
        if columns:
            columns = ", ".join(columns)
        else:
            columns = "*"
        sql = f"SELECT {columns} FROM {table} WHERE {SQLBuilder._where_exp(keys)} LIMIT {limit}"
        return sql


if __name__ == "__main__":
    print(SQLBuilder.insert("table", {"a": 1, "b": 'apple"', "c": None}))
    print(SQLBuilder.update("table", {"a": 1, "c": None}, {"b": 'apple"', "c": None}))
    print(SQLBuilder.delete("table", {"a": 1}))
