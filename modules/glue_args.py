"""
https://stackoverflow.com/a/58083506/7700479
"""
import json
import sys
from copy import deepcopy
from typing import Dict, List, Any, Union
from modules.dsl import render
from awsglue.utils import getResolvedOptions



def render_args(kwargs: dict) -> dict:
    return dict([(k, render(v)) if type(v) == str else (k, v) for k, v in kwargs.items()])


def get_glue_args(
        positional: List[str], optional: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    This is a wrapper of the glue function getResolvedOptions to take care of the following case :
    * Handling optional arguments and/or mandatory arguments
    * Optional arguments with default value
    NOTE:
        * DO NOT USE '-' while defining args as the getResolvedOptions with replace them with '_'
        * All fields would be return as a string type with getResolvedOptions

    Arguments:
        positional {list} -- list of mandatory fields for the job
        optional {dict} -- dict for optional fields with their default value

    Returns:
        dict -- given args with default value of optional args not filled
    """
    # The glue args are available in sys.argv with an extra '--'
    optional_args = list(
        set([i[2:] for i in sys.argv]).intersection([i for i in optional])
    )

    args = getResolvedOptions(sys.argv, positional + optional_args)

    # Overwrite default value if optional args are provided
    optional_ = deepcopy(optional)
    optional_.update(args)
    return render_args(optional_)


class OptionValue:
    def not_string(val: str):
        if not isinstance(val, str):
            return True
        else:
            return False

    def get_string(val: str) -> str:
        if OptionValue.not_string(val):
            return val
        try:
            return str(val)
        except ValueError:
            raise ValueError(f"Invalid value for string: {repr(val)}")

    def get_int(val: str) -> int:
        if OptionValue.not_string(val):
            return val
        try:
            return int(val)
        except ValueError:
            raise ValueError(f"Invalid value for integer: {repr(val)}")

    def get_float(val: str) -> float:
        if OptionValue.not_string(val):
            return val
        try:
            return float(val)
        except ValueError:
            raise ValueError(f"Invalid value for float: {repr(val)}")

    def get_bool(val: str) -> bool:
        if OptionValue.not_string(val):
            return val
        if val == "true":
            return True
        elif val == "false":
            return False
        else:
            raise ValueError(f"Invalid value for boolean: {repr(val)}")

    def _loads(val: str, return_type) -> Union[List, Dict]:
        if OptionValue.not_string(val):
            return val
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            raise ValueError(f"Invalid value for {return_type.__name__}: {repr(val)}")

    def get_list(val: str) -> List[Any]:
        return OptionValue._loads(val, list)

    def get_dict(val: str) -> Dict[str, Any]:
        return OptionValue._loads(val, dict)


def params_split(params: str, delimiter: str):
    return params.split(delimiter) if not params.endswith(delimiter) else params[:-1].split(delimiter)



