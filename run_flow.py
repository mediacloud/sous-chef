#!/usr/bin/env python3
"""
General flow runner for testing flows locally.

This script allows you to run any registered flow using Prefect's .fn() method,
which executes flows as plain Python functions without Prefect orchestration.

Usage:
    # List available flows
    python run_flow.py --list
    
    # Run a flow with parameters
    python run_flow.py keywords_demo --query "climate change" --start-date 2024-01-01 --end-date 2024-01-07
    
    # Run interactively (will prompt for parameters)
    python run_flow.py keywords_demo --interactive
    
    # Run with JSON parameters file
    python run_flow.py keywords_demo --params params.json
"""
import sys
import json
import argparse
from pathlib import Path
from datetime import date, datetime
from typing import Dict, Any, Optional

# Add sous-chef to path
sys.path.insert(0, str(Path(__file__).parent))

# Import flows to register them
from sous_chef.flows import *  # This imports and registers all flows

from sous_chef.flow import list_flows, get_flow, get_flow_schema


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def parse_parameter_value(value: str, param_type: str) -> Any:
    """Parse a parameter value based on its type."""
    if param_type == "integer" or param_type == "int":
        return int(value)
    elif param_type == "number" or param_type == "float":
        return float(value)
    elif param_type == "boolean" or param_type == "bool":
        return value.lower() in ("true", "1", "yes", "on")
    elif param_type == "array" or param_type == "list":
        # Assume comma-separated or JSON array
        if value.startswith("[") and value.endswith("]"):
            return json.loads(value)
        return [v.strip() for v in value.split(",")]
    elif param_type == "date":
        return parse_date(value)
    else:
        return value


def get_params_from_cli(args, schema: Dict[str, Any]) -> Dict[str, Any]:
    """Extract parameters from CLI arguments."""
    params = {}
    
    for param_name, param_info in schema.items():
        param_type = param_info.get("type", "string")
        
        # Check for CLI argument (using --param-name)
        cli_name = param_name.replace("_", "-")
        value = getattr(args, param_name.replace("-", "_"), None)
        
        if value is not None:
            params[param_name] = parse_parameter_value(str(value), param_type)
        elif "default" in param_info and param_info["default"] is not None:
            # Use default value
            params[param_name] = param_info["default"]
        elif param_type != "boolean":
            # Required parameter missing
            raise ValueError(f"Required parameter '{param_name}' not provided")
    
    return params


def get_params_interactive(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Prompt user for parameters interactively."""
    params = {}
    
    print("\nEnter parameters (press Enter for defaults):")
    print("-" * 60)
    
    for param_name, param_info in schema.items():
        param_type = param_info.get("type", "string")
        default = param_info.get("default")
        description = param_info.get("description", "")
        
        prompt = f"{param_name}"
        if param_type:
            prompt += f" ({param_type})"
        if default is not None:
            prompt += f" [default: {default}]"
        prompt += ": "
        
        if description:
            print(f"\n{description}")
        
        value = input(prompt).strip()
        
        if not value:
            if default is not None:
                params[param_name] = default
            elif param_type == "boolean":
                params[param_name] = False
            elif param_type == "array":
                params[param_name] = []
            else:
                print(f"Warning: No value provided for '{param_name}', using None")
                params[param_name] = None
        else:
            try:
                params[param_name] = parse_parameter_value(value, param_type)
            except Exception as e:
                print(f"Error parsing {param_name}: {e}")
                if default is not None:
                    params[param_name] = default
                    print(f"Using default: {default}")
    
    return params


def get_params_from_file(filepath: str) -> Dict[str, Any]:
    """Load parameters from JSON file."""
    with open(filepath, 'r') as f:
        params = json.load(f)
    
    # Convert date strings to date objects
    for key, value in params.items():
        if isinstance(value, str) and len(value) == 10 and value.count("-") == 2:
            try:
                params[key] = parse_date(value)
            except ValueError:
                pass
    
    return params




def format_result(result: Any, indent: int = 0) -> str:
    """Format flow result for display."""
    indent_str = "  " * indent
    
    if isinstance(result, dict):
        lines = ["{"]
        for key, value in result.items():
            if isinstance(value, (dict, list)):
                lines.append(f"{indent_str}  {key}:")
                lines.append(format_result(value, indent + 1))
            elif hasattr(value, "shape"):  # DataFrame
                lines.append(f"{indent_str}  {key}: DataFrame({value.shape[0]} rows, {value.shape[1]} columns)")
                if not value.empty:
                    lines.append(f"{indent_str}    Columns: {', '.join(value.columns.tolist()[:5])}")
                    if len(value.columns) > 5:
                        lines.append(f"{indent_str}    ... and {len(value.columns) - 5} more")
            else:
                lines.append(f"{indent_str}  {key}: {value}")
        lines.append(f"{indent_str}}}")
        return "\n".join(lines)
    elif isinstance(result, list):
        if len(result) == 0:
            return "[]"
        lines = ["["]
        for i, item in enumerate(result[:5]):  # Show first 5
            lines.append(f"{indent_str}  {i}: {item}")
        if len(result) > 5:
            lines.append(f"{indent_str}  ... and {len(result) - 5} more")
        lines.append(f"{indent_str}]")
        return "\n".join(lines)
    else:
        return f"{indent_str}{result}"


def main():
    """Main entry point for flow runner."""
    parser = argparse.ArgumentParser(
        description="Run sous-chef flows locally (without Prefect server)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "flow_name",
        nargs="?",
        help="Name of flow to run (use --list to see available flows)"
    )
    
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available flows"
    )
    
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Prompt for parameters interactively"
    )
    
    parser.add_argument(
        "--params",
        type=str,
        help="Path to JSON file with parameters"
    )
    
    # Parse just enough to see if --list or flow_name
    args, remaining = parser.parse_known_args()
    
    # List flows if requested
    if args.list or not args.flow_name:
        flows = list_flows()
        if not flows:
            print("No flows registered. Make sure flows are imported.")
            return
        
        print("=" * 80)
        print("Available Flows")
        print("=" * 80)
        for name, info in flows.items():
            print(f"\n{name}")
            print(f"  Description: {info['description']}")
        
        print("\n" + "=" * 80)
        print("\nUsage:")
        print("  python run_flow.py <flow_name> [--interactive] [--params file.json] [--param-name value ...]")
        print("\nExamples:")
        print("  python run_flow.py keywords_demo --interactive")
        return
    
    # Get flow
    flow_meta = get_flow(args.flow_name)
    if not flow_meta:
        print(f"Error: Flow '{args.flow_name}' not found.")
        print("Use --list to see available flows.")
        return
    
    flow_func = flow_meta["func"]
    params_model = flow_meta.get("params_model")
    schema = get_flow_schema(args.flow_name)
    
    print("=" * 80)
    print(f"Running Flow: {args.flow_name}")
    print("=" * 80)
    print(f"Description: {flow_meta['description']}")
    print("-" * 80)
    
    # Get parameters
    params_dict = {}
    
    # Parse remaining arguments for --interactive and --params
    use_interactive = "--interactive" in remaining or len(remaining) == 0
    params_file = None
    
    if "--interactive" in remaining:
        remaining.remove("--interactive")
    
    if "--params" in remaining:
        idx = remaining.index("--params")
        if idx + 1 < len(remaining):
            params_file = remaining[idx + 1]
            remaining = remaining[:idx] + remaining[idx+2:]
    
    # Load parameters
    if params_file:
        params_dict = get_params_from_file(params_file)
    elif use_interactive:
        # Prompt for parameters
        params_dict = get_params_interactive(schema)
    else:
        # Try to parse simple --key value pairs from remaining args
        i = 0
        while i < len(remaining):
            arg = remaining[i]
            if arg.startswith("--"):
                param_name = arg[2:].replace("-", "_")
                if i + 1 < len(remaining) and not remaining[i + 1].startswith("--"):
                    value = remaining[i + 1]
                    param_info = schema.get(param_name, {})
                    param_type = param_info.get("type", "string")
                    params_dict[param_name] = parse_parameter_value(value, param_type)
                    i += 2
                else:
                    # Boolean flag
                    params_dict[param_name] = True
                    i += 1
            else:
                i += 1
    
    # Validate and create params model
    if params_model:
        try:
            params = params_model(**params_dict)
        except Exception as e:
            print(f"\n❌ Error validating parameters: {e}")
            print(f"\nExpected parameters:")
            for param_name, param_info in schema.items():
                param_type = param_info.get("type", "string")
                default = param_info.get("default")
                req = "required" if default is None and param_type != "boolean" else "optional"
                print(f"  {param_name} ({param_type}): {req}")
            return
    else:
        params = params_dict
    
    print(f"\nParameters:")
    for key, value in params_dict.items():
        print(f"  {key}: {value}")
    print("-" * 80)
    print("\nRunning flow...\n")
    
    try:
        # Run flow using .fn() - this bypasses Prefect orchestration
        result = flow_func(params)
        
        print("=" * 80)
        print("Flow Execution Complete!")
        print("=" * 80)
        print("\nResults:")
        print("-" * 80)
        print(format_result(result))
        print("-" * 80)
        
        # Special handling for DataFrames in results
        if isinstance(result, dict):
            for key, value in result.items():
                if hasattr(value, "shape"):  # DataFrame
                    print(f"\n{key} (DataFrame):")
                    print(f"  Shape: {value.shape[0]} rows × {value.shape[1]} columns")
                    if not value.empty:
                        print(f"\n  First few rows:")
                        print(value.head(10).to_string())
        
        return result
        
    except Exception as e:
        print(f"\n❌ Error running flow: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
