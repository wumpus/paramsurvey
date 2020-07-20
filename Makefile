.PHONY: test clean_coverage test_coverage

test:
	PYTHONPATH=. pytest test/unit
	PYTHONPATH=. test/integration/test-multiprocessing.sh
	PYTHONPATH=.:test/integration test/integration/test-ray.sh

clean_coverage:
	rm -f .coverage

test_coverage: clean_coverage
	PYTHONPATH=. pytest --cov-report= --cov-append --cov paramsurvey -v -v test/unit
	COVERAGE=1 PYTHONPATH=. test/integration/test-multiprocessing.sh test/integration
	COVERAGE=1 PYTHONPATH=.:test/integration test/integration/test-ray.sh test/integration

test_coverage_verbose:
	PARAMSURVEY_VERBOSE=2 COVERAGE=1 PYTHONPATH=. test/integration/test-multiprocessing.sh test/integration
	PARAMSURVEY_VERBOSE=2 COVERAGE=1 PYTHONPATH=.:test/integration test/integration/test-ray.sh test/integration
