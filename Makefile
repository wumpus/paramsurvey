.PHONY: unit unit_coverage unit_coverage_verbose scripts integration integration_coverage integration_coverage_verbose clean_coverage test test_coverage test_coverage_verbose distclean distcheck dist

unit:
	# hint: PYTEST_STDERR_VISIBLE=-s
	PYTHONPATH=. pytest test/unit ${PYTEST_STDERR_VISIBLE}

unit_coverage:
	PYTHONPATH=. pytest --cov-append --cov-branch --cov paramsurvey -v -v test/unit

unit_coverage_verbose:
	PYTHONPATH=. pytest --cov-append --cov-branch --cov paramsurvey -v -v test/unit

scripts:
	# crash testing only
	PYTHONPATH=.:scripts python scripts/paramsurvey-readme-example.py
	PYTHONPATH=.:scripts python scripts/paramsurvey-greedy-example.py
	PYTHONPATH=.:scripts python scripts/paramsurvey-multistage-example.py
	PYTHONPATH=.:scripts python scripts/paramsurvey-cli.py
	PYTHONPATH=.:scripts python scripts/pset-creation-example.py
	PYTHONPATH=.:scripts python scripts/paramsurvey-rerun-missing.py
	PYTHONPATH=.:scripts python scripts/paramsurvey-stats-example.py
	#paramsurvey-carbon-example.py  # needs a carbon server

integration:
	# hint: PYTEST_STDERR_VISIBLE=-s works for these, too
	PYTHONPATH=. test/integration/test-multiprocessing.sh test/integration
	PYTHONPATH=.:test/integration test/integration/test-ray.sh test/integration

integration_coverage:
	COVERAGE=1 PYTHONPATH=. test/integration/test-multiprocessing.sh test/integration
	COVERAGE=1 PYTHONPATH=.:test/integration test/integration/test-ray.sh test/integration

integration_coverage_verbose:
	PARAMSURVEY_VERBOSE=3 PARAMSURVEY_VSTATS=3 COVERAGE=1 PYTHONPATH=. test/integration/test-multiprocessing.sh test/integration
	PARAMSURVEY_VERBOSE=3 PARAMSURVEY_VSTATS=3 COVERAGE=1 PYTHONPATH=.:test/integration test/integration/test-ray.sh test/integration

clean_coverage:
	rm -f .coverage

test: unit scripts integration

test_coverage: clean_coverage unit_coverage scripts integration_coverage

test_coverage_verbose: clean_coverage unit_coverage_verbose scripts integration_coverage_verbose

check_action:
	python -c 'import yaml, sys; print(yaml.safe_load(sys.stdin))' < .github/workflows/test-all.yml > /dev/null

distclean:
	rm -rf dist/

distcheck: distclean
	python ./setup.py sdist
	twine check dist/*

dist: distclean
	echo "reminder, you must have tagged this commit or you'll end up failing"
	echo "  finish the CHANGELOG, commit it, push it"
	echo "  git tag --list"
	echo "  git tag v0.x.x"
	echo "  git push --tags"
	python ./setup.py sdist
	twine check dist/*
	twine upload dist/* -r pypi
