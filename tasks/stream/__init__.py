from invoke import Collection

from . import run_wc
from . import run_mo
from . import run_sd
from . import faasm_mo
from . import faasm_sd
from . import faasm_wc
from . import my_wc
from . import my_sd
from . import my_mo
from . import run_test
from . import exp_sd

ns = Collection(
    run_wc,
    run_mo,
    run_sd,
    faasm_mo,
    faasm_sd,
    faasm_wc,
    run_test,
    my_wc,
    my_sd,
    my_mo,
    exp_sd,
    )
