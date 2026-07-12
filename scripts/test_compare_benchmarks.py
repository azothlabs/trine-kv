import tempfile
import unittest
from pathlib import Path

from compare_benchmarks import REQUIRED_ROWS, compare, read_summary


class CompareBenchmarksTests(unittest.TestCase):
    def test_reads_grouped_csv_after_benchmark_preamble(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "summary.csv"
            path.write_text(
                "trine-kv v1 benchmark\n"
                "group,name,runs,iterations,elapsed_us_min,elapsed_us_median,"
                "elapsed_us_max,units_per_sec_median,value_min,value_median,value_max\n"
                "writes-flush,single-key put,3,256,900,1000,1100,1,1,1,1\n",
                encoding="utf-8",
            )
            self.assertEqual(read_summary(path), {"single-key put": 1000})

    def test_requires_both_relative_and_absolute_thresholds(self) -> None:
        baseline = {name: 1_000 for name in REQUIRED_ROWS}
        current = dict(baseline)
        current[REQUIRED_ROWS[0]] = 1_300
        current[REQUIRED_ROWS[1]] = 1_100

        comparisons = compare(
            baseline,
            current,
            REQUIRED_ROWS,
            max_regression_percent=20.0,
            absolute_noise_us=200,
        )

        self.assertTrue(comparisons[0].regressed)
        self.assertFalse(comparisons[1].regressed)

    def test_missing_required_row_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing required benchmark rows"):
            compare({}, {}, REQUIRED_ROWS, 20.0, 500)


if __name__ == "__main__":
    unittest.main()
