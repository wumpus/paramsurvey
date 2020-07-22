import paramsurvey
from paramsurvey.examples import sleep_worker

paramsurvey.init(backend='multiprocessing')  # or 'ray', if you	installed it

psets = [{'duration': 0.3}] * 5
ret = paramsurvey.map(sleep_worker, psets, verbose=2)

for r in ret:
    print(repr(r))
