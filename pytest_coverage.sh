#!/usr/bin/env bash
# The script runs all tests in the directory, outputs a coverage report and then updates a coverage
# badge for display on the main page.
# NOTE: this will overwrite the coverage.svg file which must then be committed to Git.
pytest --cov-report term-missing --cov=.
coverage-badge -f -o coverage.svg
