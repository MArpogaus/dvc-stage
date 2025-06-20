"""Tests for the CLI module."""

from unittest.mock import patch

from dvc_stage.cli import _print_stage_definition, cli


class TestCLI:
    """Test cases for CLI functionality."""

    def test_cli_run_command_parsing(self):
        """Test CLI run command argument parsing."""
        with patch("sys.argv", ["dvc-stage", "run", "test_stage"]):
            with patch("dvc_stage.cli._run_stage") as mock_run:
                try:
                    cli()
                except SystemExit:
                    pass
                mock_run.assert_called_once_with(
                    stage="test_stage", validate=True, item=None
                )

    def test_cli_run_command_with_item(self):
        """Test CLI run command with item parameter."""
        with patch("sys.argv", ["dvc-stage", "run", "test_stage", "--item", "item1"]):
            with patch("dvc_stage.cli._run_stage") as mock_run:
                try:
                    cli()
                except SystemExit:
                    pass
                mock_run.assert_called_once_with(
                    stage="test_stage", validate=True, item="item1"
                )

    def test_cli_run_command_skip_validation(self):
        """Test CLI run command with skip validation."""
        with patch("sys.argv", ["dvc-stage", "run", "test_stage", "--skip-validation"]):
            with patch("dvc_stage.cli._run_stage") as mock_run:
                try:
                    cli()
                except SystemExit:
                    pass
                mock_run.assert_called_once_with(
                    stage="test_stage", validate=False, item=None
                )

    def test_cli_run_command_both_flags(self):
        """Test CLI run command with both skip validation and item."""
        with patch(
            "sys.argv",
            ["dvc-stage", "run", "test_stage", "--skip-validation", "--item", "item1"],
        ):
            with patch("dvc_stage.cli._run_stage") as mock_run:
                try:
                    cli()
                except SystemExit:
                    pass
                mock_run.assert_called_once_with(
                    stage="test_stage", validate=False, item="item1"
                )

    @patch("dvc_stage.cli.get_stage_definition")
    @patch("builtins.print")
    def test_print_stage_definition(self, mock_print, mock_get_stage):
        """Test printing stage definition."""
        mock_get_stage.return_value = {"test": "config"}
        _print_stage_definition("test_stage")
        mock_get_stage.assert_called_once_with("test_stage")
        mock_print.assert_called_once()
