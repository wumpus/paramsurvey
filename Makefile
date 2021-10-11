.PHONY: unit scripts generic test clean_coverage test_coverage distclean dist_check dist

unit:
	# hint: PYTEST_STDERR_VISIBLE=-s
	PYTHONPATH=. pytest test/unit ${PYTEST_STDERR_VISIBLE}

scripts:
	PYTHONPATH=.:scripts python scripts/paramsurvey-readme-example.py
	PYTHONPATH=.:scripts python scripts/paramsurvey-greedy-example.py
	PYTHONPATH=.:scripts python scripts/paramsurvey-multistage-example.py
	PYTHONPATH=.:scripts python scripts/paramsurvey-cli.py
	PYTHONPATH=.:scripts python scripts/pset-creation-example.py
	PYTHONPATH=.:scripts python scripts/paramsurvey-rerun-missing.py
	PYTHONPATH=.:scripts python scripts/paramsurvey-stats-example.py
	#paramsurvey-carbon-example.py  # needs a carbon server

generic:
	# hint: PYTEST_STDERR_VISIBLE=-s works for these, too
	PYTHONPATH=. test/integration/test-multiprocessing.sh test/integration
	PYTHONPATH=.:test/integration test/integration/test-ray.sh test/integration

test: unit scripts generic

clean_coverage:
	rm -f .coverage

test_coverage: clean_coverage
	PYTHONPATH=. pytest --cov-append --cov-branch --cov paramsurvey -v -v test/unit
	COVERAGE=1 PYTHONPATH=. test/integration/test-multiprocessing.sh test/integration
	COVERAGE=1 PYTHONPATH=.:test/integration test/integration/test-ray.sh test/integration

test_coverage_verbose:
	PARAMSURVEY_VERBOSE=3 PARAMSURVEY_VSTATS=3 COVERAGE=1 PYTHONPATH=. test/integration/test-multiprocessing.sh test/integration
	PARAMSURVEY_VERBOSE=3 PARAMSURVEY_VSTATS=3 COVERAGE=1 PYTHONPATH=.:test/integration test/integration/test-ray.sh test/integration

distclean:
	rm -rf dist/

distcheck: distclean
	python ./setup.py sdist
	twine check dist/*

dist: distclean
	echo "reminder, you must have tagged this commit or you'll end up failing"
	echo "  finish the CHANGELOG"
	echo "  git tag v0.x.x"
	echo "  git push --tags"
	python ./setup.py sdist
	twine check dist/*
	twine upload dist/* -r pypi
