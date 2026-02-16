"""Simple pipeline to run all extract scripts in `etl.extract`.

Usage:
    python -m etl.extract.pipeline

The module assumes `backend` is importable (e.g. `PYTHONPATH=backend`).
When run from the repository root, prefer the orchestrator which adds `backend`
to `sys.path` automatically.
"""
import logging

from . import extract_adzuna, extract_the_muse


def run_all():
    """Run all extract scripts in a reasonable order.

    Order:
      1. The Muse (jobs + companies)
      2. Adzuna (company-based search, reads from Muse jobs output)
    """
    logging.info("Starting full extract pipeline...")

    try:
        logging.info("Running The Muse extractor...")
        extract_the_muse.main()
    except Exception as exc:  # keep going even if one step fails
        logging.exception("The Muse extractor failed: %s", exc)

    try:
        logging.info("Running Adzuna extractor...")
        extract_adzuna.main()
    except Exception as exc:
        logging.exception("Adzuna extractor failed: %s", exc)

    logging.info("Extract pipeline finished.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    run_all()
