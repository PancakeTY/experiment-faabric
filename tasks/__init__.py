from invoke import Collection

from . import docker
from . import format_code

import logging

from tasks.kernels_mpi import ns as kernels_mpi_ns
from tasks.kernels_omp import ns as kernels_omp_ns
from tasks.lammps import ns as lammps_ns
from tasks.lulesh import ns as lulesh_ns
from tasks.makespan import ns as makespan_ns
from tasks.migration import ns as migration_ns
from tasks.motivation import ns as motivation_ns
from tasks.polybench import ns as polybench_ns
from tasks.stream_hello import ns as stream_hello_ns

logging.getLogger().setLevel(logging.DEBUG)

ns = Collection(
    docker,
    format_code,
)

ns.add_collection(kernels_mpi_ns, name="kernels-mpi")
ns.add_collection(kernels_omp_ns, name="kernels-omp")
ns.add_collection(lammps_ns, name="lammps")
ns.add_collection(lulesh_ns, name="lulesh")
ns.add_collection(makespan_ns, name="makespan")
ns.add_collection(migration_ns, name="migration")
ns.add_collection(motivation_ns, name="motivation")
ns.add_collection(polybench_ns, name="polybench")
ns.add_collection(stream_hello_ns, name="stream_hello")
