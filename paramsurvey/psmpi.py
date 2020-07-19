from mpi4py import MPI


def init():
    pass


def finalize():
    pass


def current_core_count():
    return MPI.COMM_WORLD.Get_size()


def map():
    raise NotImplementedError('Sorry, the MPI backend is incomplete')
