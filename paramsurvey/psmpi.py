from mpi4py import MPI


def init():  # pragma: no cover
    pass


def finalize():  # pragma: no cover
    pass


def current_core_count():  # pragma: no cover
    return MPI.COMM_WORLD.Get_size()


def map():  # pragma: no cover
    raise NotImplementedError('Sorry, the MPI backend is incomplete')
