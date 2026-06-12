"""Interactive terminal UI (Textual).

Two screens: a setup form, then a live run dashboard with per-stage progress
bars, a streaming event log, and a final summary.
"""

from __future__ import annotations

from pathlib import Path

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import Screen
from textual.widgets import (
    Button,
    Footer,
    Header,
    Input,
    Label,
    ProgressBar,
    RichLog,
    Select,
    Static,
    Switch,
    TextArea,
)

from . import __version__, exiftool
from .engine import Cancelled, Event, Options, Organizer, Report

BANNER = r"""
 ███████ ██   ██  ██████  ███████ ██████   ██████  ██   ██
 ██      ██   ██ ██    ██ ██      ██   ██ ██    ██  ██ ██
 ███████ ███████ ██    ██ █████   ██████  ██    ██   ███
      ██ ██   ██ ██    ██ ██      ██   ██ ██    ██  ██ ██
 ███████ ██   ██  ██████  ███████ ██████   ██████  ██   ██
""".strip("\n")

STAGES = [
    ("scan", "Scan"),
    ("hash", "Dedupe"),
    ("dates", "Dates"),
    ("transfer", "Transfer"),
]


class SetupScreen(Screen):
    """Collects sources, output, and options, then launches a run."""

    def compose(self) -> ComposeResult:
        yield Header()
        with VerticalScroll(id="setup"):
            yield Static(BANNER, id="banner")
            yield Static(
                f"v{__version__} · organize a lifetime of photos into "
                "YYYY/YYYY-MM, deduplicated",
                id="tagline",
            )
            yield Label("Source folders (one per line, .photoslibrary bundles OK)")
            yield TextArea(id="sources")
            yield Label("Output folder")
            yield Input(placeholder="~/Pictures/Archive", id="output")
            with Horizontal(classes="row"):
                with Vertical(classes="opt"):
                    yield Label("Mode")
                    yield Select(
                        [("Copy (safe)", "copy"), ("Move", "move"),
                         ("Hardlink (instant)", "hardlink")],
                        value="copy",
                        allow_blank=False,
                        id="mode",
                    )
                with Vertical(classes="opt"):
                    yield Label("Dry run")
                    yield Switch(value=False, id="dry_run")
                with Vertical(classes="opt"):
                    yield Label("Keep undated → Unsorted/")
                    yield Switch(value=False, id="keep_undated")
                with Vertical(classes="opt"):
                    yield Label("Verify writes")
                    yield Switch(value=False, id="verify_writes")
            yield Static("", id="form-error")
            yield Button("Start  ▶", variant="success", id="start")
            if not exiftool.available():
                yield Static(
                    "⚠ exiftool not found — using built-in parsers "
                    "(JPEG/PNG/HEIC/MP4/MOV/WebP/TIFF still covered)",
                    id="exiftool-note",
                )
        yield Footer()

    @on(Button.Pressed, "#start")
    def start(self) -> None:
        sources = [
            Path(line.strip()).expanduser()
            for line in self.query_one("#sources", TextArea).text.splitlines()
            if line.strip()
        ]
        output_raw = self.query_one("#output", Input).value.strip()
        error = self.query_one("#form-error", Static)

        if not sources:
            error.update("[red]Add at least one source folder.[/red]")
            return
        missing = [s for s in sources if not s.exists()]
        if missing:
            error.update(f"[red]Source not found: {missing[0]}[/red]")
            return
        if not output_raw:
            error.update("[red]Choose an output folder.[/red]")
            return

        options = Options(
            sources=sources,
            output=Path(output_raw).expanduser(),
            mode=self.query_one("#mode", Select).value,
            dry_run=self.query_one("#dry_run", Switch).value,
            keep_undated=self.query_one("#keep_undated", Switch).value,
            verify_writes=self.query_one("#verify_writes", Switch).value,
        )
        self.app.push_screen(RunScreen(options))


class RunScreen(Screen):
    """Live dashboard for a run; the engine executes in a worker thread."""

    BINDINGS = [("escape", "cancel_or_back", "Cancel / Back")]

    def __init__(self, options: Options) -> None:
        super().__init__()
        self.options = options
        self.organizer: Organizer | None = None
        self.finished = False

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="run"):
            mode = "DRY RUN" if self.options.dry_run else self.options.mode.upper()
            yield Static(f"[b]{mode}[/b] → {self.options.output}", id="run-title")
            for stage_id, label in STAGES:
                with Horizontal(classes="stage-row"):
                    yield Label(f"{label:<10}", classes="stage-label")
                    yield ProgressBar(id=f"bar-{stage_id}", show_eta=False)
            yield RichLog(id="log", markup=True, max_lines=400)
            yield Static("", id="summary")
        yield Footer()

    def on_mount(self) -> None:
        self.run_worker(self._run_engine, thread=True, exclusive=True)

    def _run_engine(self) -> None:
        self.organizer = Organizer(self.options, on_event=self._on_engine_event)
        try:
            report = self.organizer.run()
        except Cancelled:
            self.app.call_from_thread(self._show_cancelled)
            return
        except Exception as e:  # surface unexpected failures in the UI
            self.app.call_from_thread(self._show_error, str(e))
            return
        self.app.call_from_thread(self._show_report, report)

    def _on_engine_event(self, event: Event) -> None:
        self.app.call_from_thread(self._render_event, event)

    def _render_event(self, event: Event) -> None:
        log = self.query_one("#log", RichLog)
        if event.level == "error":
            log.write(f"[red]✗ {event.message}[/red]")
        elif event.level == "warn":
            log.write(f"[yellow]⚠ {event.message}[/yellow]")
        elif event.message:
            log.write(event.message)

        try:
            bar = self.query_one(f"#bar-{event.stage}", ProgressBar)
        except Exception:
            return
        bar.update(total=event.total or None, progress=event.done)

    def _show_report(self, report: Report) -> None:
        self.finished = True
        lines = [
            "",
            "[bold green]✓ Complete[/bold green]"
            + (" [dim](dry run — nothing written)[/dim]" if report.dry_run else ""),
            f"  Scanned: [b]{report.scanned}[/b]   Transferred: [b]{report.transferred}[/b]   "
            f"Duplicates: [b]{report.duplicates}[/b]",
            f"  Already organized: [b]{report.already_present}[/b]   "
            f"No date: [b]{report.no_date}[/b]   "
            + (f"[red]Errors: {report.errors}[/red]" if report.errors
               else f"Errors: [b]0[/b]"),
            f"  Took {report.duration_seconds:.1f}s — press [b]Esc[/b] to go back",
        ]
        self.query_one("#summary", Static).update("\n".join(lines))

    def _show_cancelled(self) -> None:
        self.finished = True
        self.query_one("#summary", Static).update(
            "\n[red]Cancelled.[/red] Press [b]Esc[/b] to go back."
        )

    def _show_error(self, message: str) -> None:
        self.finished = True
        self.query_one("#summary", Static).update(
            f"\n[red]Failed: {message}[/red] Press [b]Esc[/b] to go back."
        )

    def action_cancel_or_back(self) -> None:
        if self.finished:
            self.app.pop_screen()
        elif self.organizer:
            self.organizer.cancel()


class ShoeboxApp(App):
    TITLE = "Shoebox"
    SUB_TITLE = "photo archive organizer"

    CSS = """
    #banner { color: $accent; text-style: bold; width: auto; }
    #tagline { color: $text-muted; margin-bottom: 1; }
    #setup { padding: 1 2; }
    #setup Label { margin-top: 1; }
    #sources { height: 6; }
    .row { height: auto; margin-top: 1; }
    .opt { width: 1fr; padding-right: 2; height: auto; }
    #start { margin-top: 1; width: 20; }
    #form-error { height: auto; margin-top: 1; }
    #exiftool-note { color: $warning; margin-top: 1; }
    #run { padding: 1 2; }
    #run-title { margin-bottom: 1; }
    .stage-row { height: 1; margin-bottom: 1; }
    .stage-label { color: $accent; }
    #log { height: 1fr; border: round $primary; margin-top: 1; }
    #summary { height: auto; }
    """

    BINDINGS = [("q", "quit", "Quit")]

    def on_mount(self) -> None:
        self.push_screen(SetupScreen())


def run() -> None:
    ShoeboxApp().run()


if __name__ == "__main__":
    run()
