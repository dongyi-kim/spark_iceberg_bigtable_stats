"""XCom 없는 파일 프로토콜·nullable Param의 경량 단위 테스트.

Airflow·PySpark를 설치하지 않은 개발 환경에서도 JSON 계약과 DAG import
경로를 검증할 수 있게 최소 stub을 사용한다.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1]


class _Task:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __rshift__(self, other):
        return other


class _DAG:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class _Param:
    def __init__(self, default=None, **schema):
        self.default = default
        self.schema = schema


def _install_stubs():
    airflow = types.ModuleType("airflow")
    airflow.DAG = _DAG
    airflow_operators = types.ModuleType("airflow.operators")
    airflow_models = types.ModuleType("airflow.models")
    airflow_param = types.ModuleType("airflow.models.param")
    airflow_param.Param = _Param
    airflow_bash = types.ModuleType("airflow.operators.bash")
    airflow_bash.BashOperator = _Task
    airflow_python = types.ModuleType("airflow.operators.python")
    airflow_python.PythonOperator = _Task
    airflow_python.ShortCircuitOperator = _Task
    sys.modules.update({
        "airflow": airflow,
        "airflow.models": airflow_models,
        "airflow.models.param": airflow_param,
        "airflow.operators": airflow_operators,
        "airflow.operators.bash": airflow_bash,
        "airflow.operators.python": airflow_python,
    })

    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = object
    sys.modules.update({"pyspark": pyspark, "pyspark.sql": pyspark_sql})


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class FileProtocolTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_stubs()
        os.environ["STATS_SUBMIT_MODE"] = "docker"
        os.environ["STATS_BENCH_DIR"] = "/tmp/stats-bench-test"
        cls.refresh_dag = _load(
            "test_stats_refresh_dag", JOBS_DIR / "stats_refresh" / "stats_refresh_dag.py"
        )
        cls.big_dag = _load(
            "test_bigtable_dag",
            JOBS_DIR / "stats_bigtable_sketch" / "bigtable_sketch_dag.py",
        )
        cls.refresh_job = _load(
            "test_stats_refresh_job", JOBS_DIR / "stats_refresh" / "spark_stats_job.py"
        )
        cls.big_job = _load(
            "test_bigtable_job",
            JOBS_DIR / "stats_bigtable_sketch" / "sketch_stats_job.py",
        )

    def test_bigtable_request_defaults_and_nullable_params(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            request = root / "run" / "request.json"
            result = root / "run" / "result.json"
            result.parent.mkdir(parents=True)
            result.write_text('{"stale": true}', encoding="utf-8")

            self.big_dag.prepare_request(
                self.big_dag.JOB, str(request), str(result), "req-1",
                max_partitions=None, force_partitions=None, no_publish=None,
            )
            value = json.loads(request.read_text(encoding="utf-8"))
            self.assertEqual(30, value["max_partitions"])
            self.assertEqual([], value["force_partitions"])
            self.assertTrue(value["publish"])
            self.assertEqual({
                "max_partitions": None,
                "force_partitions": None,
                "no_publish": None,
            }, value["optional_params"])
            self.assertFalse(result.exists())

            self.big_dag.prepare_request(
                self.big_dag.JOB, str(request), str(result), "req-2",
                max_partitions=7,
                force_partitions="2026-06-01, 2026-06-02,2026-06-01",
                no_publish=True,
            )
            value = json.loads(request.read_text(encoding="utf-8"))
            self.assertEqual(7, value["max_partitions"])
            self.assertEqual(["2026-06-01", "2026-06-02"], value["force_partitions"])
            self.assertFalse(value["publish"])

    def test_bigtable_params_reject_invalid_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(ValueError, "max_partitions must be integer"):
                self.big_dag.prepare_request(
                    self.big_dag.JOB,
                    str(root / "request.json"),
                    str(root / "result.json"),
                    "req-bad",
                    max_partitions="7",
                )

    def test_job_request_contracts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            refresh = root / "refresh.json"
            refresh.write_text(json.dumps({
                "protocol_version": 1,
                "kind": "stats_refresh",
                "request_id": "r1",
                "catalog": "ice",
                "table": "bench.t",
                "snapshot_id": 123,
                "columns": ["id"],
            }), encoding="utf-8")
            self.assertEqual("r1", self.refresh_job.load_request(str(refresh))["request_id"])

            big = root / "big.json"
            big.write_text(json.dumps({
                "protocol_version": 1,
                "kind": "stats_bigtable_sketch",
                "request_id": "r2",
                "catalog": "ice",
                "table": "bench.t",
                "partition_col": "dt",
                "columns": ["id"],
                "state_prefix": "s3://bucket/state",
                "max_partitions": 3,
                "force_partitions": [],
                "publish": True,
            }), encoding="utf-8")
            self.assertEqual("r2", self.big_job.load_request(str(big)).request_id)

    def test_verify_rejects_stale_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            request = root / "request.json"
            result = root / "result.json"
            request.write_text('{"request_id":"new"}', encoding="utf-8")
            result.write_text(
                '{"protocol_version":1,"request_id":"old","ok":true}',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "request_id mismatch"):
                self.big_dag.verify_and_metrics(str(request), str(result))

    def test_sources_have_no_xcom_and_params_are_nullable(self):
        for path in (
            JOBS_DIR / "stats_refresh" / "stats_refresh_dag.py",
            JOBS_DIR / "stats_bigtable_sketch" / "bigtable_sketch_dag.py",
        ):
            source = path.read_text(encoding="utf-8")
            for forbidden in (".xcom_pull", ".xcom_push", "do_xcom_push=True"):
                self.assertNotIn(forbidden, source, f"{forbidden} in {path}")
        source = (JOBS_DIR / "stats_bigtable_sketch" / "bigtable_sketch_dag.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('type=["null", "string"]', source)
        self.assertIn('type=["null", "integer"]', source)
        self.assertIn('type=["null", "boolean"]', source)

        dag_params = self.big_dag.dag.kwargs["params"]
        self.assertEqual(None, dag_params["force_partitions"].default)
        self.assertEqual(["null", "string"], dag_params["force_partitions"].schema["type"])
        self.assertEqual(None, dag_params["max_partitions"].default)
        self.assertEqual(["null", "integer"], dag_params["max_partitions"].schema["type"])
        self.assertEqual(1, dag_params["max_partitions"].schema["minimum"])
        self.assertEqual(None, dag_params["no_publish"].default)
        self.assertEqual(["null", "boolean"], dag_params["no_publish"].schema["type"])
        self.assertTrue(self.big_dag.dag.kwargs["render_template_as_native_obj"])


if __name__ == "__main__":
    unittest.main()
