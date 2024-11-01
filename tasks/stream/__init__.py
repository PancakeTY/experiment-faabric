from invoke import Collection

from . import run_wc
from . import run_mo
from . import run_sd
from . import faasm_mo
from . import faasm_sd
from . import faasm_wc

ns = Collection(
    run_wc,
    run_mo,
    run_sd,
    faasm_mo,
    faasm_sd,
    faasm_wc,
    )
