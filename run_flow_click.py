#!/usr/bin/env python3
"""
General flow runner for testing flows locally (Click-based version).

This script allows you to run any registered flow using Prefect's .fn() method,
which executes flows as plain Python functions without Prefect orchestration.

When --test is used, prefect_test_harness() provides a minimal Prefect context
using a temporary SQLite database, which is useful for testing flows that use
Prefect features like logging or blocks without requiring a Prefect server.

Usage:
    # List available flows
    python run_flow_click.py --list
    
    # Run a flow with parameters (no Prefect test harness)
    python run_flow_click.py keywords_demo --query "climate change" --start-date 2024-01-01 --end-date 2024-01-07
    
    # Run with test harness (recommended for testing)
    python run_flow_click.py keywords_demo --test --query "climate change" --start-date 2024-01-01 --end-date 2024-01-07
    
    # Run interactively (will prompt for parameters)
    python run_flow_click.py keywords_demo --interactive
    
    # Run with test harness and interactive mode
    python run_flow_click.py keywords_demo --test --interactive
    
    # Run with JSON parameters file
    python run_flow_click.py keywords_demo --params params.json
"""
import sys
import json
import os
from pathlib import Path
from datetime import date, datetime
from typing import Dict, Any, Optional, Tuple

import click

# Add sous-chef to path
sys.path.insert(0, str(Path(__file__).parent))

# Import Prefect testing utilities (optional)
try:
    from prefect.testing.utilities import prefect_test_harness
    PREFECT_TESTING_AVAILABLE = True
except ImportError:
    PREFECT_TESTING_AVAILABLE = False
    prefect_test_harness = None

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


def parse_dynamic_params(remaining_args: Tuple[str, ...], schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse dynamic flow parameters from remaining command-line arguments.
    
    Args:
        remaining_args: Tuple of remaining arguments after fixed options
        schema: Flow parameter schema
        
    Returns:
        Dictionary of parsed parameters
    """
    params_dict = {}
    i = 0
    
    while i < len(remaining_args):
        arg = remaining_args[i]
        if arg.startswith("--"):
            param_name = arg[2:].replace("-", "_")
            if i + 1 < len(remaining_args) and not remaining_args[i + 1].startswith("--"):
                value = remaining_args[i + 1]
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
    
    return params_dict


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


@click.command()
@click.argument("flow_name", required=False)
@click.option(
    "--list",
    "list_flows_flag",
    is_flag=True,
    default=False,
    help="List all available flows"
)
@click.option(
    "--test",
    is_flag=True,
    default=False,
    help="Use prefect_test_harness() to provide Prefect context. "
         "Recommended when testing flows that use Prefect features like "
         "logging or blocks. Creates a temporary SQLite database but "
         "doesn't require a Prefect server."
)
@click.option(
    "--interactive",
    is_flag=True,
    default=False,
    help="Prompt for parameters interactively"
)
@click.option(
    "--params",
    type=click.Path(exists=True, readable=True, path_type=Path),
    help="Path to JSON file with parameters"
)
@click.argument("flow_params", nargs=-1)
def main(flow_name: Optional[str], list_flows_flag: bool, test: bool, interactive: bool, 
         params: Optional[Path], flow_params: Tuple[str, ...]) -> None:
    """
    Run sous-chef flows locally (without Prefect server).
    
    FLOW_NAME: Name of flow to run (use --list to see available flows)
    
    FLOW_PARAMS: Dynamic flow parameters as --param-name value pairs
    """
    # List flows if requested or no flow name provided
    if list_flows_flag or not flow_name:
        flows = list_flows()
        if not flows:
            click.echo("No flows registered. Make sure flows are imported.")
            return
        
        click.echo("=" * 80)
        click.echo("Available Flows")
        click.echo("=" * 80)
        for name, info in flows.items():
            click.echo(f"\n{name}")
            click.echo(f"  Description: {info['description']}")
        
        click.echo("\n" + "=" * 80)
        click.echo("\nUsage:")
        click.echo("  python run_flow_click.py <flow_name> [--test] [--interactive] [--params file.json] [--param-name value ...]")
        click.echo("\nExamples:")
        click.echo("  python run_flow_click.py keywords_demo --interactive")
        click.echo("  python run_flow_click.py keywords_demo --test --query 'climate change' --start-date 2024-01-01")
        return
    
    # Check if --test was requested but not available
    if test and not PREFECT_TESTING_AVAILABLE:
        click.echo("⚠️  Warning: --test flag requested but prefect.testing.utilities is not available.")
        click.echo("   Install Prefect to use the test harness, or run without --test flag.")
        click.echo("   Continuing without test harness...\n")
        use_test_harness = False
    else:
        use_test_harness = test
    
    # Get flow
    flow_meta = get_flow(flow_name)
    if not flow_meta:
        click.echo(f"Error: Flow '{flow_name}' not found.")
        click.echo("Use --list to see available flows.")
        return
    
    flow_func = flow_meta["func"]
    params_model = flow_meta.get("params_model")
    schema = get_flow_schema(flow_name)
    
    click.echo("=" * 80)
    click.echo(f"Running Flow: {flow_name}")
    click.echo("=" * 80)
    click.echo(f"Description: {flow_meta['description']}")
    if use_test_harness:
        click.echo("Mode: Test (using prefect_test_harness)")
    else:
        click.echo("Mode: Direct execution (no Prefect context)")
    click.echo("-" * 80)
    
    # Get parameters
    params_dict = {}
    
    # Determine parameter source: --params file, interactive, or CLI args
    if params:
        # Load from JSON file
        params_dict = get_params_from_file(str(params))
    elif interactive or len(flow_params) == 0:
        # Prompt for parameters interactively
        params_dict = get_params_interactive(schema)
    else:
        # Parse dynamic parameters from remaining args
        params_dict = parse_dynamic_params(flow_params, schema)
    
    # Validate and create params model
    if params_model:
        try:
            params = params_model(**params_dict)
        except Exception as e:
            click.echo(f"\n❌ Error validating parameters: {e}")
            click.echo(f"\nExpected parameters:")
            for param_name, param_info in schema.items():
                param_type = param_info.get("type", "string")
                default = param_info.get("default")
                req = "required" if default is None and param_type != "boolean" else "optional"
                click.echo(f"  {param_name} ({param_type}): {req}")
            return
    else:
        params = params_dict
    
    click.echo(f"\nParameters:")
    for key, value in params_dict.items():
        click.echo(f"  {key}: {value}")
    click.echo("-" * 80)
    click.echo("\nRunning flow...\n")
    
    try:
        # Get the underlying function using .fn() to bypass Prefect orchestration
        if hasattr(flow_func, 'fn'):
            flow_func_fn = flow_func.fn
        else:
            flow_func_fn = flow_func
        
        # Set test mode environment variable when --test flag is used
        # This enables automatic dry_run for B2 tasks and other test behaviors
        test_mode_was_set = False
        if use_test_harness:
            os.environ["SOUS_CHEF_TEST_MODE"] = "1"
            test_mode_was_set = True
            click.echo("ℹ️  Test mode enabled - B2 uploads will be skipped (dry_run)")
        
        # Run flow with or without test harness
        if use_test_harness:
            with prefect_test_harness():
                result = flow_func_fn(params)
        else:
            result = flow_func_fn(params)
        
        # Clean up environment variable after execution (optional, but cleaner)
        if test_mode_was_set:
            os.environ.pop("SOUS_CHEF_TEST_MODE", None)
        
        click.echo("=" * 80)
        click.echo("Flow Execution Complete!")
        click.echo("=" * 80)
        click.echo("\nResults:")
        click.echo("-" * 80)
        click.echo(format_result(result))
        click.echo("-" * 80)
        
        # Special handling for DataFrames in results
        if isinstance(result, dict):
            for key, value in result.items():
                if hasattr(value, "shape"):  # DataFrame
                    click.echo(f"\n{key} (DataFrame):")
                    click.echo(f"  Shape: {value.shape[0]} rows × {value.shape[1]} columns")
                    if not value.empty:
                        click.echo(f"\n  First few rows:")
                        click.echo(value.head(10).to_string())
        
        return result
        
    except Exception as e:
        click.echo(f"\n❌ Error running flow: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
