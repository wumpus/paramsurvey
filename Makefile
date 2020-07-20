.PHONY: test clean_coverage test_coverage

test:
	PYTHONPATH=. test/test-multiprocessing.sh
	PYTHONPATH=.:test test/test-ray.sh

clean_coverage:
	rm -f .coverage

test_coverage: clean_coverage
	COVERAGE=1 PYTHONPATH=. test/test-multiprocessing.sh
	COVERAGE=1 PYTHONPATH=.:test test/test-ray.sh

test_coverage_verbose:
	PARAMSURVEY_VERBOSE=2 COVERAGE=1 PYTHONPATH=. test/test-multiprocessing.sh
	PARAMSURVEY_VERBOSE=2 COVERAGE=1 PYTHONPATH=.:test test/test-ray.sh
