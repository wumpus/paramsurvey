import time
import paramsurvey


# note that sleep_worker() must be declared before the paramsurvey.init() call!
# (thanks to the weird way multiprocessing works)
def sleep_worker(pset, system_kwargs, user_kwargs, raw_stats):
    time.sleep(pset['duration'])
    return {'slept': pset['duration']}


# Windows, you must have a __name__ == __main__' guard on all executable code in the main program
paramsurvey.init(backend='multiprocessing')  # or 'ray', if you installed it

psets = [{'duration': 0.3}] * 5

results = paramsurvey.map(sleep_worker, psets, verbose=2)

for r in results.results:
    print(r['pset'], r['result'])
