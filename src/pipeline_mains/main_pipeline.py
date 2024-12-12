import subprocess
import sys
import logging
from pathlib import Path
import time
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_checkpoint_dirs():
    """Create checkpoint directories for each pipeline."""
    base_path = Path("/tmp/checkpoint")
    checkpoints = {
        "games": base_path / "games",
        "odds": base_path / "odds",
        "weather": base_path / "weather",
    }

    for path in checkpoints.values():
        path.mkdir(parents=True, exist_ok=True)

    return checkpoints


def create_spark_session(app_name: str):
    """Create a single SparkSession to be used across all streams."""
    if SparkSession._instantiatedSession is None:
        return (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.sql.streaming.schemaInference", "true")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            )
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .getOrCreate()
        )


def start_pipelines():
    spark = create_spark_session("sports_pipeline")

    pipelines = {
        "games": {
            "script": "games_pipeline.py",
            "spark": spark,
            "restart_delay": 60,
            "dependencies": ["kafka"],
        },
        "odds": {
            "script": "odds_pipeline.py",
            "spark": spark,
            "restart_delay": 60,
            "dependencies": ["kafka"],
        },
        "weather": {
            "script": "weather_pipeline.py",
            "spark": spark,
            "restart_delay": 60,
            "dependencies": ["kafka"],
        },
    }

    processes = {}
    restart_times = {}

    try:
        for name, config in pipelines.items():
            try:
                process = subprocess.Popen(
                    [sys.executable, config["script"]],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                    bufsize=1,  # Line buffered
                )
                processes[name] = process
                restart_times[name] = 0
                logger.info(f"Started {name} pipeline with PID {process.pid}")
            except Exception as e:
                logger.error(f"Failed to start {name} pipeline: {e}")

        while True:
            for name, process in processes.items():
                try:
                    # Read output without blocking
                    output = process.stdout.readline()
                    if output:
                        print(f"[{name}] {output.strip()}")

                    error = process.stderr.readline()
                    if error:
                        print(f"[{name}] ERROR: {error.strip()}")

                    # Check process status
                    if process.poll() is not None:
                        current_time = time.time()
                        last_restart = restart_times[name]

                        if (
                            current_time - last_restart
                            > pipelines[name]["restart_delay"]
                        ):
                            logger.warning(f"{name} pipeline died, restarting...")

                            # Restart process
                            process = subprocess.Popen(
                                [sys.executable, pipelines[name]["script"]],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                universal_newlines=True,
                                bufsize=1,
                            )
                            processes[name] = process
                            restart_times[name] = current_time
                            logger.info(
                                f"Restarted {name} pipeline with PID {process.pid}"
                            )

                except Exception as e:
                    logger.error(f"Error monitoring {name} pipeline: {e}")

            time.sleep(0.1)  # Small delay to prevent CPU overuse

    except KeyboardInterrupt:
        logger.info("Shutting down pipelines...")
        for name, process in processes.items():
            try:
                process.terminate()
                logger.info(f"Terminated {name} pipeline")
            except Exception as e:
                logger.error(f"Error terminating {name} pipeline: {e}")

    except Exception as e:
        logger.error(f"Main process error: {e}")
        raise
    finally:
        # Ensure all processes are terminated
        for process in processes.values():
            try:
                process.kill()
                spark.stop()
            except Exception as e:
                print(e)
                pass


if __name__ == "__main__":
    start_pipelines()
