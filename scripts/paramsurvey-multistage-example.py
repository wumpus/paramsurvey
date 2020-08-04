import itertools

import paramsurvey
import paramsurvey.params


def first_stage(pset, system_kwargs, user_kwargs, raw_stats):
    # compute something expensive related to 'a' and 'b'
    expensive = pset['a'] + pset['b']
    return {'expensive': expensive}


def second_stage(pset, system_kwargs, user_kwargs, raw_stats):
    # compute something using the precomputed 'expensive' and 'c'
    final = pset['expensive'] + pset['c']
    return {'final': final}


def main():
    paramsurvey.init(backend='multiprocessing')  # or 'ray', if you installed it

    psets = paramsurvey.params.product({'a': [1, 2, 3], 'b': [4, 5, 6]})

    results = paramsurvey.map(first_stage, psets, verbose=2)

    psets = paramsurvey.params.product(results, {'c': [7, 8, 9]})

    results = paramsurvey.map(second_stage, psets, verbose=2)

    print(results.df)


# for Windows, you must have a __name__ == __main__' guard on all executable code in the main program (multiprocessing)
if __name__ == '__main__':
    main()
