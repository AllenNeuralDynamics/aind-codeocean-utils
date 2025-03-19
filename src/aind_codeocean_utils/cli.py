import argparse
import json
import os
from pathlib import Path
from typing import Any, Callable

APP_PANEL_JSON = "/root/capsule/.codeocean/app-panel.json"


def parse_bool(v: str) -> bool:
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected")


def parse_path(file_path: str) -> Path:
    path = Path(file_path)
    if not path.is_file():
        raise argparse.ArgumentTypeError(f"File does not exist: {file_path}")
    if not os.access(path, os.R_OK):
        raise argparse.ArgumentTypeError(f"File is not readable: {file_path}")
    return path


def create_parser(
    json_file: str = APP_PANEL_JSON, description: str = "app-panel CLI"
    ) -> argparse.ArgumentParser:
    """
    Creates an argument parser from a JSON configuration file.

    Parameters
    ----------
    json_file : str, optional
        Path to JSON config file. Default: APP_PANEL_JSON
    description : str, optional
        CLI description. Default: "app-panel CLI"

    Returns
    -------
    argparse.ArgumentParser
        Configured argument parser

    Raises
    ------
    NotImplementedError
        For non-named parameters or version != 1
    FileNotFoundError
        If JSON file not found
    json.JSONDecodeError
        If invalid JSON
    KeyError
        If missing required JSON keys 
    ValueError
        If invalid default value type

    Notes
    -----
    JSON must have:
    - "named_parameters": true
    - "version": 1
    - "parameters" list with param_name, value_type
    Supports string, integer, number, boolean, file, list types
    """

    # Read and parse JSON
    with open(json_file) as f:
        config = json.load(f)

    if not config["named_parameters"]:
        raise NotImplementedError(
            "Dynamic arg parsing only enabled for named parameters"
        )
    if config["version"] != 1:
        raise NotImplementedError(
            "Dynamic arg parsing only enabled for app-panel version 1"
        )

    parser = argparse.ArgumentParser(description=description)

    # Type mapping
    type_map: dict[str, Callable[[str], Any]] = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": parse_bool,
        "file": parse_path,
    }

    # Add arguments based on JSON config
    for param in config["parameters"]:
        param_type = param.get("value_type") or param.get("type")
        param_help = f"{param.get("description")} ({param.get("help_text")})"
        kwargs: dict[str, Any] = {
            "help": param_help,
            "type": type_map.get(param_type, str),
            "required": param.get("required", False),
        }

        # Handle default value if present
        if "default_value" in param:
            kwargs["default"] = kwargs["type"](param["default_value"])
            kwargs["required"] = False

        # Handle list type parameters
        if param.get("type") == "list":
            if param["value_type"] == "boolean":
                kwargs["choices"] = [True, False]
            elif "extra_data" in param:
                kwargs["choices"] = param["extra_data"]

        # Add argument using param_name
        name = f"--{param['param_name']}"
        parser.add_argument(name, **kwargs)

    return parser


def call_function_with_cli_args(func, json_file=APP_PANEL_JSON):
    """
    Calls a given function with arguments parsed from the command line interface (CLI).
    This function uses a parser to extract CLI arguments, converts them into a dictionary,
    and passes them as keyword arguments to the provided function.
    
    Parameters
    ----------
    func : callable
        The function to be called with the parsed CLI arguments.
        
    Returns
    -------
    Any
        The return value of the called function.
    """
    
    # Parse arguments
    parser = create_parser(json_file=json_file)
    parsed_args = parser.parse_args()

    # Convert Namespace to dictionary
    kwargs = vars(parsed_args)

    # Call the function with parsed arguments
    return func(**kwargs)
