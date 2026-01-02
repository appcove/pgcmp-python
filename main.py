#!/usr/bin/env python3
"""pgcmp - PostgreSQL Schema Comparison Tool."""

import argparse
import sys

from rich.console import Console
from rich.table import Table

from pgcmp import (
    Database,
    analyze_databases,
    Action,
    AnalysisResult,
    SchemaAnalysis,
    TableAnalysis,
    ColumnAnalysis,
    TriggerAnalysis,
    IndexAnalysis,
    ConstraintAnalysis,
    FunctionAnalysis,
    ViewAnalysis,
    MaterializedViewAnalysis,
    generate_xml_report,
)

console = Console()


def print_comparison_table(result: AnalysisResult) -> None:
    """Display a unified comparison table from analysis results."""
    table = Table(
        title="Database Comparison",
        show_header=True,
        header_style="bold",
        show_lines=True,
    )
    table.add_column("Type", style="cyan", no_wrap=True)
    table.add_column("Item", style="white", no_wrap=True)
    table.add_column("Left", justify="right", no_wrap=True)
    table.add_column("Right", justify="right", no_wrap=True)
    table.add_column("Detail", overflow="fold")

    # Summary rows
    for row in result.summary:
        table.add_row(
            "Summary",
            row.object_type,
            str(row.left_count),
            str(row.right_count),
            "",
        )

    # Add a separator
    table.add_section()

    # Schema rows (all schemas first)
    for schema in result.schemas:
        left_mark = "[green]✓[/green]" if schema.in_left else ""
        right_mark = "[green]✓[/green]" if schema.in_right else ""
        detail = _format_schema_action(schema)

        table.add_row("Schema", schema.name, left_mark, right_mark, detail)

    # Add a separator before tables
    table.add_section()

    # Table rows (all tables grouped together)
    for schema in result.schemas:
        for tbl in schema.tables:
            left_mark = "[green]✓[/green]" if tbl.in_left else ""
            right_mark = "[green]✓[/green]" if tbl.in_right else ""
            detail = _format_table_action(tbl)

            table.add_row("Table", tbl.full_name, left_mark, right_mark, detail)

    # Add a separator before columns
    table.add_section()

    # Column rows (all columns grouped together)
    for schema in result.schemas:
        for tbl in schema.tables:
            for col in tbl.columns:
                left_mark = "[green]✓[/green]" if col.in_left else ""
                right_mark = "[green]✓[/green]" if col.in_right else ""
                detail = _format_column_detail(col)

                table.add_row("Column", col.full_name, left_mark, right_mark, detail)

    # Add a separator before views
    table.add_section()

    # View rows (all views grouped together)
    for schema in result.schemas:
        for view in schema.views:
            left_mark = "[green]✓[/green]" if view.in_left else ""
            right_mark = "[green]✓[/green]" if view.in_right else ""
            detail = _format_view_detail(view)

            table.add_row("View", view.full_name, left_mark, right_mark, detail)

    # Add a separator before materialized views
    table.add_section()

    # Materialized view rows (all materialized views grouped together)
    for schema in result.schemas:
        for matview in schema.materialized_views:
            left_mark = "[green]✓[/green]" if matview.in_left else ""
            right_mark = "[green]✓[/green]" if matview.in_right else ""
            detail = _format_materialized_view_detail(matview)

            table.add_row("Matview", matview.full_name, left_mark, right_mark, detail)

    # Add a separator before indexes
    table.add_section()

    # Index rows (all indexes grouped together)
    for schema in result.schemas:
        for tbl in schema.tables:
            for index in tbl.indexes:
                left_mark = "[green]✓[/green]" if index.in_left else ""
                right_mark = "[green]✓[/green]" if index.in_right else ""
                detail = _format_index_detail(index)

                table.add_row("Index", index.full_name, left_mark, right_mark, detail)

    # Add a separator before constraints
    table.add_section()

    # Constraint rows (all constraints grouped together)
    for schema in result.schemas:
        for tbl in schema.tables:
            for constraint in tbl.constraints:
                left_mark = "[green]✓[/green]" if constraint.in_left else ""
                right_mark = "[green]✓[/green]" if constraint.in_right else ""
                detail = _format_constraint_detail(constraint)

                table.add_row(
                    "Constraint", constraint.full_name, left_mark, right_mark, detail
                )

    # Add a separator before triggers
    table.add_section()

    # Trigger rows (all triggers grouped together)
    for schema in result.schemas:
        for tbl in schema.tables:
            for trigger in tbl.triggers:
                left_mark = "[green]✓[/green]" if trigger.in_left else ""
                right_mark = "[green]✓[/green]" if trigger.in_right else ""
                detail = _format_trigger_detail(trigger)

                table.add_row(
                    "Trigger", trigger.full_name, left_mark, right_mark, detail
                )

    # Add a separator before functions
    table.add_section()

    # Function rows (all functions grouped together)
    for schema in result.schemas:
        for func in schema.functions:
            left_mark = "[green]✓[/green]" if func.in_left else ""
            right_mark = "[green]✓[/green]" if func.in_right else ""
            detail = _format_function_detail(func)

            table.add_row("Function", func.full_name, left_mark, right_mark, detail)

    console.print(table)


def print_row_counts(
    left_db: "Database", right_db: "Database", xml_output: bool
) -> int:
    """Print row count comparison for tables in both databases."""
    from xml.etree.ElementTree import Element, SubElement, tostring
    from xml.dom import minidom

    # Find tables that exist in both databases
    common_tables = sorted(set(left_db.tables.keys()) & set(right_db.tables.keys()))

    differences = 0

    if xml_output:
        root = Element("row_count_comparison")

        # Add connection info
        connections = SubElement(root, "connections")
        left_conn = SubElement(connections, "left")
        left_conn.text = left_db.connection_string
        right_conn = SubElement(connections, "right")
        right_conn.text = right_db.connection_string

        tables_elem = SubElement(root, "tables")
        for table_key in common_tables:
            left_table = left_db.tables[table_key]
            right_table = right_db.tables[table_key]
            left_count = left_table.row_count or 0
            right_count = right_table.row_count or 0
            differs = left_count != right_count
            if differs:
                differences += 1

            table_elem = SubElement(tables_elem, "table")
            SubElement(table_elem, "name").text = table_key
            SubElement(table_elem, "left_count").text = str(left_count)
            SubElement(table_elem, "right_count").text = str(right_count)
            SubElement(table_elem, "differs").text = str(differs).lower()

        SubElement(root, "number_of_differences").text = str(differences)

        rough_string = tostring(root, encoding="unicode")
        reparsed = minidom.parseString(rough_string)
        print(reparsed.toprettyxml(indent="  "))
    else:
        table = Table(
            title="Row Count Comparison",
            show_header=True,
            header_style="bold",
            show_lines=True,
        )
        table.add_column("Table", style="white", no_wrap=True)
        table.add_column("Left", justify="right", no_wrap=True)
        table.add_column("Right", justify="right", no_wrap=True)
        table.add_column("Status", no_wrap=True)

        for table_key in common_tables:
            left_table = left_db.tables[table_key]
            right_table = right_db.tables[table_key]
            left_count = left_table.row_count or 0
            right_count = right_table.row_count or 0
            differs = left_count != right_count
            if differs:
                differences += 1
                status = "[cyan]differs[/cyan]"
            else:
                status = "[green]match[/green]"

            table.add_row(table_key, str(left_count), str(right_count), status)

        console.print()
        console.print(table)
        console.print(f"\n[bold]Tables with different row counts: {differences}[/bold]")

    return 0 if differences == 0 else 2


def _format_schema_action(schema: SchemaAnalysis) -> str:
    """Format detail for a schema."""
    if schema.action == Action.ADD:
        return f"[yellow]{schema.action_description}[/yellow]"
    elif schema.action == Action.REMOVE:
        return f"[red]{schema.action_description}[/red]"
    return ""


def _format_table_action(tbl: TableAnalysis) -> str:
    """Format detail for a table."""
    if tbl.action == Action.ADD:
        return f"[yellow]{tbl.action_description}[/yellow]"
    elif tbl.action == Action.REMOVE:
        return f"[red]{tbl.action_description}[/red]"
    return ""


def _format_column_detail(col: ColumnAnalysis) -> str:
    """Format detail for a column."""
    if col.action == Action.ADD:
        return f"[yellow]{col.action_description}[/yellow]"
    elif col.action == Action.REMOVE:
        return f"[red]{col.action_description}[/red]"
    elif col.action == Action.MODIFY:
        return f"[cyan]{col.modification_detail}[/cyan]"
    return ""


def _format_view_detail(view: ViewAnalysis) -> str:
    """Format detail for a view."""
    if view.action == Action.ADD:
        return f"[yellow]{view.action_description}[/yellow]"
    elif view.action == Action.REMOVE:
        return f"[red]{view.action_description}[/red]"
    elif view.action == Action.MODIFY:
        return f"[cyan]{view.modification_detail}[/cyan]"
    return ""


def _format_materialized_view_detail(matview: MaterializedViewAnalysis) -> str:
    """Format detail for a materialized view."""
    if matview.action == Action.ADD:
        return f"[yellow]{matview.action_description}[/yellow]"
    elif matview.action == Action.REMOVE:
        return f"[red]{matview.action_description}[/red]"
    elif matview.action == Action.MODIFY:
        return f"[cyan]{matview.modification_detail}[/cyan]"
    return ""


def _format_trigger_detail(trigger: TriggerAnalysis) -> str:
    """Format detail for a trigger."""
    if trigger.action == Action.ADD:
        return f"[yellow]{trigger.action_description}[/yellow]"
    elif trigger.action == Action.REMOVE:
        return f"[red]{trigger.action_description}[/red]"
    elif trigger.action == Action.MODIFY:
        return f"[cyan]{trigger.modification_detail}[/cyan]"
    return ""


def _format_index_detail(index: IndexAnalysis) -> str:
    """Format detail for an index."""
    if index.action == Action.ADD:
        return f"[yellow]{index.action_description}[/yellow]"
    elif index.action == Action.REMOVE:
        return f"[red]{index.action_description}[/red]"
    elif index.action == Action.MODIFY:
        return f"[cyan]{index.modification_detail}[/cyan]"
    return ""


def _format_constraint_detail(constraint: ConstraintAnalysis) -> str:
    """Format detail for a constraint."""
    if constraint.action == Action.ADD:
        return f"[yellow]{constraint.action_description}[/yellow]"
    elif constraint.action == Action.REMOVE:
        return f"[red]{constraint.action_description}[/red]"
    elif constraint.action == Action.MODIFY:
        return f"[cyan]{constraint.modification_detail}[/cyan]"
    return ""


def _format_function_detail(func: FunctionAnalysis) -> str:
    """Format detail for a function."""
    if func.action == Action.ADD:
        return f"[yellow]{func.action_description}[/yellow]"
    elif func.action == Action.REMOVE:
        return f"[red]{func.action_description}[/red]"
    elif func.action == Action.MODIFY:
        return f"[cyan]{func.modification_detail}[/cyan]"
    return ""


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Compare two PostgreSQL database schemas.  The first argument is the schema you want it to be.  The second argument is the schema you have.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    %(prog)s "postgresql://user:pass@host1:5432/db1" "postgresql://user:pass@host2:5432/db2"
    %(prog)s --xml "postgres://localhost/db1" "postgres://localhost/db2"
        """,
    )
    parser.add_argument(
        "NEW_CONNECTION_STRING",
        help="Connection string for the left (source) database",
    )
    parser.add_argument(
        "OLD_CONNECTION_STRING",
        help="Connection string for the right (target) database",
    )
    parser.add_argument(
        "--apply-sql-file",
        type=argparse.FileType("r"),
        help="SQL file to apply to old database in a transaction before comparing (rolled back after)",
    )
    parser.add_argument(
        "--row-counts",
        action="store_true",
        default=False,
        help="Only compare row counts (skip schema comparison)",
    )
    parser.add_argument(
        "--xml",
        action="store_true",
        default=False,
        help="Output results in XML format instead of table format",
    )
    return parser.parse_args()


def main() -> int:
    """Main entry point for pgcmp."""
    args = parse_args()

    # Step 1: Gather all data from both databases
    console.print("[bold]Connecting to left database...[/bold]")
    try:
        left_db = Database.from_connection_string(args.NEW_CONNECTION_STRING)
    except Exception as e:
        console.print(f"[red]Error connecting to left database: {e}[/red]")
        return 1

    console.print("[bold]Connecting to right database...[/bold]")
    try:
        apply_sql = args.apply_sql_file.read() if args.apply_sql_file else None
        right_db = Database.from_connection_string(args.OLD_CONNECTION_STRING, apply_sql=apply_sql)
    except Exception as e:
        console.print(f"[red]Error connecting to right database: {e}[/red]")
        return 1

    # Handle --row-counts mode separately
    if args.row_counts:
        console.print("[bold]Comparing row counts...[/bold]")
        return print_row_counts(left_db, right_db, args.xml)

    # Step 2 & 3: Analyze databases (schemas, then tables for matching schemas)
    console.print("[bold]Analyzing differences...[/bold]")
    result = analyze_databases(left_db, right_db)

    # Display version warning if applicable
    if result.version_warning:
        console.print(f"[yellow]{result.version_warning}[/yellow]")

    # Step 4: Display results in requested format
    console.print()
    if args.xml:
        # XML output - print directly without rich formatting
        xml_output = generate_xml_report(result)
        print(xml_output)
    else:
        # Table output using rich
        print_comparison_table(result)

    return 0 if not result.has_differences() else 2


if __name__ == "__main__":
    sys.exit(main())
