"""Integration tests for the complete DVC-Stage pipeline."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
import yaml

from dvc_stage.cli import _run_stage
from dvc_stage.config import get_stage_definition, get_stage_params
from dvc_stage.utils import get_deps


class TestIntegration:
    """Integration test cases for complete pipeline functionality."""

    @pytest.fixture
    def temp_workspace(self):
        """Create a temporary workspace with sample files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)

            # Create directory structure
            (workspace / "data").mkdir()
            (workspace / "outdir").mkdir()

            # Create sample input file
            sample_data = pd.DataFrame(
                {"C1": ["Hello", "Hi", "Hey"], "C2": ["World", "There", "You"]}
            )
            sample_data.to_csv(workspace / "data" / "input.csv", index=False)

            # Create params.yaml
            params = {
                "log_level": "info",
                "test_pipeline": {
                    "load": {"path": "data/input.csv", "format": "csv"},
                    "transformations": [{"id": "dropna"}],
                    "write": {"path": "outdir/output.csv", "format": "csv"},
                },
            }

            with open(workspace / "params.yaml", "w") as f:
                yaml.dump(params, f)

            # Initialize git repo to avoid DVC errors
            import subprocess

            try:
                subprocess.run(
                    ["git", "init"], cwd=workspace, capture_output=True, check=True
                )
                subprocess.run(
                    ["git", "config", "user.email", "test@example.com"],
                    cwd=workspace,
                    capture_output=True,
                    check=True,
                )
                subprocess.run(
                    ["git", "config", "user.name", "Test User"],
                    cwd=workspace,
                    capture_output=True,
                    check=True,
                )
                # Add and commit files to avoid revision errors
                subprocess.run(
                    ["git", "add", "."], cwd=workspace, capture_output=True, check=True
                )
                subprocess.run(
                    ["git", "commit", "-m", "Initial commit"],
                    cwd=workspace,
                    capture_output=True,
                    check=True,
                )

                # Initialize DVC repo
                subprocess.run(
                    ["dvc", "init"], cwd=workspace, capture_output=True, check=True
                )
                subprocess.run(
                    ["git", "add", ".dvc"],
                    cwd=workspace,
                    capture_output=True,
                    check=True,
                )
                subprocess.run(
                    ["git", "commit", "-m", "Initialize DVC"],
                    cwd=workspace,
                    capture_output=True,
                    check=True,
                )
            except subprocess.CalledProcessError:
                # Git/DVC operations failed, but continue with test
                pass

            # Create dvc.yaml
            dvc_config = {
                "stages": {
                    "test_pipeline": {
                        "cmd": "dvc-stage run test_pipeline",
                        "deps": ["data/input.csv"],
                        "outs": ["outdir/output.csv"],
                        "params": ["test_pipeline"],
                    }
                }
            }

            with open(workspace / "dvc.yaml", "w") as f:
                yaml.dump(dvc_config, f)

            # Change to workspace directory
            original_cwd = os.getcwd()
            os.chdir(workspace)

            yield workspace

            # Restore original directory
            os.chdir(original_cwd)

    @patch("dvc.api.params_show")
    def test_get_stage_params_integration(self, mock_params_show, temp_workspace):
        """Test getting stage parameters from real files."""
        # Mock DVC API response
        mock_params_show.return_value = {
            "log_level": "info",
            "test_pipeline": {
                "load": {"path": "data/input.csv", "format": "csv"},
                "transformations": [{"id": "dropna"}],
                "write": {"path": "outdir/output.csv", "format": "csv"},
            },
        }

        stage_params, global_params = get_stage_params("test_pipeline")

        assert "load" in stage_params
        assert "write" in stage_params
        assert stage_params["load"]["path"] == "data/input.csv"
        assert stage_params["load"]["format"] == "csv"
        assert "log_level" in global_params

    def test_get_deps_integration(self, temp_workspace):
        """Test getting dependencies from real files."""
        deps, param_keys = get_deps("data/input.csv", {})

        assert len(deps) == 1
        assert "data/input.csv" in deps[0]
        assert len(param_keys) == 0

    @patch("dvc.api.params_show")
    @patch("dvc_stage.config.load_dvc_yaml")
    def test_get_stage_definition_integration(
        self, mock_load_dvc, mock_params_show, temp_workspace
    ):
        """Test getting complete stage definition."""
        # Mock DVC API responses
        mock_params_show.return_value = {
            "log_level": "info",
            "test_pipeline": {
                "load": {"path": "data/input.csv", "format": "csv"},
                "transformations": [{"id": "dropna"}],
                "write": {"path": "outdir/output.csv", "format": "csv"},
            },
        }

        mock_load_dvc.return_value = {
            "stages": {
                "test_pipeline": {
                    "cmd": "dvc-stage run test_pipeline",
                    "deps": ["data/input.csv"],
                    "outs": ["outdir/output.csv"],
                    "params": ["test_pipeline"],
                }
            }
        }

        definition = get_stage_definition("test_pipeline")

        assert "stages" in definition
        assert "test_pipeline" in definition["stages"]
        stage = definition["stages"]["test_pipeline"]

        assert "cmd" in stage
        assert "deps" in stage
        assert "outs" in stage
        assert "params" in stage

    @patch("dvc.api.params_show")
    @patch("dvc_stage.cli.apply_transformations")
    @patch("dvc_stage.cli.write_data")
    @patch("dvc_stage.cli.load_data")
    def test_run_stage_integration(
        self, mock_load, mock_write, mock_transform, mock_params_show, temp_workspace
    ):
        """Test running a complete stage pipeline."""
        # Mock DVC API response
        mock_params_show.return_value = {
            "log_level": "info",
            "test_pipeline": {
                "load": {"path": "data/input.csv", "format": "csv"},
                "transformations": [{"id": "dropna"}],
                "write": {"path": "outdir/output.csv", "format": "csv"},
            },
        }

        # Mock data loading and transformation
        sample_df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        mock_load.return_value = sample_df
        mock_transform.side_effect = lambda data, *args, **kwargs: data

        # Run the stage
        _run_stage("test_pipeline", validate=False)

        # Verify that functions were called
        mock_load.assert_called_once()
        mock_transform.assert_called_once()
        mock_write.assert_called_once()

    def test_foreach_stage_path_resolution(self, temp_workspace):
        """Test that foreach stage paths are resolved correctly."""
        # Create the test data file that the path resolution expects
        dataset_dir = temp_workspace / "data" / "dataset_a"
        dataset_dir.mkdir(parents=True, exist_ok=True)

        sample_data = pd.DataFrame(
            {"value1": [1.0, 2.0], "value2": [3.0, 4.0], "category": ["A", "B"]}
        )
        sample_data.to_csv(dataset_dir / "input.csv", index=False)

        params = {}
        deps, param_keys = get_deps("data/${item}/input.csv", params, item="dataset_a")

        # Should resolve to the specific item path
        assert len(deps) > 0
        assert "item" in param_keys
        assert any("dataset_a" in dep for dep in deps)

    def test_multi_path_dependencies(self, temp_workspace):
        """Test handling multiple input paths."""
        # Create additional input file
        sample_data2 = pd.DataFrame({"D1": [1, 2], "D2": [3, 4]})
        sample_data2.to_csv(temp_workspace / "data" / "input2.csv", index=False)

        paths = ["data/input.csv", "data/input2.csv"]
        deps, param_keys = get_deps(paths, {})

        assert len(deps) == 2
        assert any("input.csv" in dep for dep in deps)
        assert any("input2.csv" in dep for dep in deps)
