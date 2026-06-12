import asyncio

from textual.widgets import Button, Input, TextArea

from shoebox.tui import SetupScreen, ShoeboxApp

from fixtures import build_jpeg


def test_tui_validation_and_dry_run(tmp_path):
    src = tmp_path / "photos"
    src.mkdir()
    (src / "a.jpg").write_bytes(build_jpeg("2019:07:04 12:30:00"))

    async def scenario():
        app = ShoeboxApp()
        async with app.run_test(size=(100, 40)) as pilot:
            assert isinstance(app.screen, SetupScreen)

            # Empty form shows a validation error instead of crashing.
            app.screen.query_one("#start", Button).press()
            await pilot.pause()
            error = str(app.screen.query_one("#form-error").render())
            assert "source" in error.lower()

            # A dry run completes and renders the summary.
            app.screen.query_one("#sources", TextArea).text = str(src)
            app.screen.query_one("#output", Input).value = str(tmp_path / "out")
            app.screen.query_one("#dry_run").value = True
            app.screen.query_one("#start", Button).press()
            await pilot.pause(1.5)
            summary = str(app.screen.query_one("#summary").render())
            assert "Complete" in summary
            assert not (tmp_path / "out").exists()

    asyncio.run(scenario())
