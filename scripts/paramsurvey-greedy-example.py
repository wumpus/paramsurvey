import random

import paramsurvey


# metric() must be declared before the paramsurvey.init() call (multiprocessing)
# metric() cannot be a nested procedure (pickle)
def metric(pset, system_kwargs, user_kwargs, raw_stats):

    existing_stations = pset['existing_stations']
    new_station = pset['station']

    # given existing telescopes existing_stations and a new station,
    # compute a metric of how good this combination is:
    metric = random.randrange(0, 1000)

    return {'metric': metric}


def main():
    paramsurvey.init(backend='multiprocessing')  # or 'ray', if you installed it

    stations = dict((x, {}) for x in 'ABCDEFGHIJ')
    picked_stations = []

    '''
    Given a list of potential radio telescope stations, named A-J, which
    combine together in very non-linear ways when used together,
    iteratively pick the next best station to add to the array.
    '''

    while stations:
        psets = list({'station': s, 'existing_stations': picked_stations} for s in stations)
        results = paramsurvey.map(metric, psets, verbose=0)

        best_metric = max(r.metric for r in results)
        best_stations = [r.station for r in results if r.metric == best_metric]
        print('picking station(s)', *best_stations)
        picked_stations.extend(best_stations)
        [stations.pop(bs) for bs in best_stations]

    print('the final station order is', *picked_stations)


# for Windows, you must have a __name__ == __main__' guard on all executable code in the main program (multiprocessing)
if __name__ == '__main__':
    main()
