from invoke import Collection

from . import run_wc
from . import run_mo
from . import run_sd
from . import run_wcnew
from . import faasm_mo
from . import faasm_sd
from . import faasm_wc
from . import custom_plot

ns = Collection(
    run_wc,
    run_mo,
    run_sd,
    run_wcnew,
    faasm_mo,
    faasm_sd,
    faasm_wc,
    custom_plot,
    )
