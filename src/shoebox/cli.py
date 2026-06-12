"""Command-line interface.

``shoebox`` with no arguments opens the interactive TUI; ``shoebox organize``
and ``shoebox verify`` are scriptable subcommands with live progress bars.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from . import __version__, exiftool
from .engine import Cancelled, Event, Options, Organizer, Report
from .verify import verify

STAGE_LABELS = {
    "scan": "Scanning sources",
    "hash": "Hashing (dedupe)",
    "dates": "Reading dates",
    "transfer": "Transferring",
    "verify-source": "Hashing sources",
    "verify-dest": "Hashing destination",
}


class ProgressRenderer:
    """Maps engine events onto a rich multi-bar progress display."""

    def __init__(self, console: Console) -> None:
        self.console = console
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        )
        self._tasks: dict[str, int] = {}

    def __enter__(self) -> "ProgressRenderer":
        self.progress.__enter__()
        return self

    def __exit__(self, *exc) -> None:
        self.progress.__exit__(*exc)

    def __call__(self, event: Event) -> None:
        if event.level == "error":
            self.progress.console.print(f"[red]✗ {event.message}[/red]")
        elif event.level == "warn":
            self.progress.console.print(f"[yellow]⚠ {event.message}[/yellow]")

        if event.stage in ("done",) or event.stage not in STAGE_LABELS:
            return
        if event.stage not in self._tasks:
            self._tasks[event.stage] = self.progress.add_task(
                STAGE_LABELS[event.stage], total=event.total or None
            )
        task_id = self._tasks[event.stage]
        self.progress.update(
            task_id,
            completed=event.done,
            total=event.total or None,
        )


def _print_report(console: Console, report: Report) -> None:
    table = Table(title="Shoebox Run Summary", show_header=False, min_width=44)
    table.add_column(style="bold")
    table.add_column(justify="right")
    rows = [
        ("Files scanned", report.scanned),
        ("Transferred" + (" (dry run)" if report.dry_run else ""), report.transferred),
        ("Duplicates skipped", report.duplicates),
        ("Already organized", report.already_present),
        ("No date found", report.no_date),
        ("Errors", report.errors),
    ]
    for label, value in rows:
        style = "red" if label == "Errors" and value else ""
        table.add_row(label, f"[{style}]{value}[/{style}]" if style else str(value))
    table.add_row("Duration", f"{report.duration_seconds:.1f}s")
    console.print()
    console.print(table)


def _write_report(path: str, payload: dict, console: Console) -> None:
    report_path = Path(path).expanduser().resolve()
    report_path.write_text(json.dumps(payload, indent=2))
    console.print(f"\nReport written to [bold]{report_path}[/bold]")


def cmd_organize(args: argparse.Namespace, console: Console) -> int:
    mode = "move" if args.move else "hardlink" if args.hardlink else "copy"
    options = Options(
        sources=[Path(s) for s in args.sources],
        output=Path(args.output),
        mode=mode,
        dry_run=args.dry_run,
        workers=args.workers,
        use_exiftool=not args.no_exiftool,
        keep_undated=args.keep_undated,
        verify_writes=args.verify_writes,
    )

    if options.use_exiftool and not exiftool.available():
        console.print(
            "[yellow]exiftool not found - using built-in metadata parsers "
            "(install exiftool for maximum format coverage)[/yellow]"
        )

    try:
        with ProgressRenderer(console) as renderer:
            report = Organizer(options, on_event=renderer).run()
    except (Cancelled, KeyboardInterrupt):
        console.print("[red]Cancelled.[/red]")
        return 130

    _print_report(console, report)
    if args.report:
        _write_report(args.report, report.to_dict(), console)
    return 1 if report.errors else 0


def cmd_verify(args: argparse.Namespace, console: Console) -> int:
    try:
        with ProgressRenderer(console) as renderer:
            result = verify(
                sources=[Path(s) for s in args.sources],
                dest=Path(args.dest),
                workers=args.workers,
                on_event=renderer,
            )
    except KeyboardInterrupt:
        console.print("[red]Cancelled.[/red]")
        return 130

    summary = result["summary"]
    table = Table(title="Verification Summary", show_header=False, min_width=44)
    table.add_column(style="bold")
    table.add_column(justify="right")
    for key, value in summary.items():
        table.add_row(key.replace("_", " "), str(value))
    console.print()
    console.print(table)

    if summary["verified_complete"]:
        console.print("[bold green]✓ Destination contains every source file.[/bold green]")
    else:
        console.print(
            f"[bold red]✗ {summary['missing_in_dest']} unique files missing "
            "from destination.[/bold red]"
        )
    if args.report:
        _write_report(args.report, result, console)
    return 0 if summary["verified_complete"] else 1


def cmd_tui(args: argparse.Namespace, console: Console) -> int:
    from .tui import ShoeboxApp

    ShoeboxApp().run()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shoebox",
        description="Organize photos and videos into YYYY/YYYY-MM folders, "
        "deduplicated by content hash.",
    )
    parser.add_argument("--version", action="version", version=f"shoebox {__version__}")
    sub = parser.add_subparsers(dest="command")

    org = sub.add_parser("organize", help="organize media into the archive")
    org.add_argument("sources", nargs="+", help="source folders or .photoslibrary bundles")
    org.add_argument("-o", "--output", required=True, help="archive root directory")
    mode = org.add_mutually_exclusive_group()
    mode.add_argument("--move", action="store_true", help="move files instead of copying")
    mode.add_argument(
        "--hardlink", action="store_true",
        help="hardlink files (instant, zero extra space; same filesystem only)",
    )
    org.add_argument("--dry-run", action="store_true", help="plan everything, write nothing")
    org.add_argument("-w", "--workers", type=int, default=8, help="parallel workers (default 8)")
    org.add_argument("--no-exiftool", action="store_true",
                     help="skip exiftool even if installed")
    org.add_argument("--keep-undated", action="store_true",
                     help="put undated files in Unsorted/ instead of skipping them")
    org.add_argument("--verify-writes", action="store_true",
                     help="re-hash every file after copying (paranoid mode)")
    org.add_argument("--report", help="write a JSON report to this path")
    org.set_defaults(func=cmd_organize)

    ver = sub.add_parser("verify", help="verify an archive contains all source content")
    ver.add_argument("sources", nargs="+", help="original source folders")
    ver.add_argument("-o", "--dest", required=True, help="archive root to check")
    ver.add_argument("-w", "--workers", type=int, default=8)
    ver.add_argument("--report", help="write a JSON report to this path")
    ver.set_defaults(func=cmd_verify)

    tui = sub.add_parser("tui", help="open the interactive interface")
    tui.set_defaults(func=cmd_tui)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    console = Console()

    if not getattr(args, "func", None):
        if sys.stdout.isatty():
            return cmd_tui(args, console)
        parser.print_help()
        return 0

    try:
        return args.func(args, console)
    except ValueError as e:
        console.print(f"[red]error: {e}[/red]")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
