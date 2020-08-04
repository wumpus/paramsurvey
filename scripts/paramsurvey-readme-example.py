import time
import paramsurvey


# sleep_worker() must be declared before the paramsurvey.init() call (multiprocessing)
# sleep_worker() cannot be a nested procedure (pickle)
def sleep_worker(pset, system_kwargs, user_kwargs, raw_stats):
    time.sleep(pset['duration'])
    return {'slept': pset['duration']}


def main():
    paramsurvey.init(backend='multiprocessing')  # or 'ray', if you installed it

    psets = [{'duration': 0.3}] * 5

    results = paramsurvey.map(sleep_worker, psets, verbose=2)

    for r in results:
        print(r.duration, r.slept)


# for Windows, you must have a __name__ == __main__' guard on all executable code in the main program (multiprocessing)
if __name__ == '__main__':
    main()
