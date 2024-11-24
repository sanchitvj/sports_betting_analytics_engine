import subprocess
import sys
import logging
from pathlib import Path
import time

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


def start_pipelines():
    # Define pipeline scripts with their configurations
    pipelines = {
        "games": {
            "script": "games_pipeline.py",
            "restart_delay": 60,  # Delay before restart in seconds
            "dependencies": ["kafka"],
        },
        "odds": {
            "script": "odds_pipeline.py",
            "restart_delay": 60,
            "dependencies": ["kafka"],
        },
        "weather": {
            "script": "weather_pipeline.py",
            "restart_delay": 60,
            "dependencies": ["kafka"],
        },
    }

    processes = {}
    restart_times = {}

    try:
        # Create checkpoint directories
        # checkpoints = create_checkpoint_dirs()
        # logger.info("Created checkpoint directories")

        # Start each pipeline as a separate process
        for name, config in pipelines.items():
            try:
                process = subprocess.Popen(
                    [sys.executable, config["script"]],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )
                processes[name] = process
                restart_times[name] = 0
                logger.info(f"Started {name} pipeline with PID {process.pid}")
            except Exception as e:
                logger.error(f"Failed to start {name} pipeline: {e}")

        # Monitor processes
        while True:
            for name, process in processes.items():
                try:
                    # Check process status
                    if process.poll() is not None:
                        current_time = time.time()
                        last_restart = restart_times[name]

                        # Check if enough time has passed since last restart
                        if (
                            current_time - last_restart
                            > pipelines[name]["restart_delay"]
                        ):
                            logger.warning(f"{name} pipeline died, restarting...")

                            # Get any error output
                            _, stderr = process.communicate()
                            if stderr:
                                logger.error(f"{name} pipeline error: {stderr}")

                            # Restart process
                            process = subprocess.Popen(
                                [sys.executable, pipelines[name]["script"]],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                universal_newlines=True,
                            )
                            processes[name] = process
                            restart_times[name] = current_time
                            logger.info(
                                f"Restarted {name} pipeline with PID {process.pid}"
                            )

                except Exception as e:
                    logger.error(f"Error monitoring {name} pipeline: {e}")

            time.sleep(10)  # Check every 10 seconds

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
            except Exception as e:
                print(e)
                pass


if __name__ == "__main__":
    start_pipelines()
