from invoke import Collection

from . import run
from . import run_mo
from . import run_sd
from . import run_test

ns = Collection(
    run,
    run_mo,
    run_sd,
    run_test,
    )
