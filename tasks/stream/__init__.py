from invoke import Collection

from . import run_aa
from . import run_pl
from . import run_nwm
from . import run_etl
from . import run_wc
from . import run_mo
from . import run_sd
from . import run_state
from . import run_wcnew
from . import faasm_mo
from . import faasm_sd
from . import faasm_wc
from . import custom_plot
from . import stats

ns = Collection(
    run_aa,
    run_pl,
    run_nwm,
    run_etl,
    run_wc,
    run_mo,
    run_sd,
    run_state,
    run_wcnew,
    faasm_mo,
    faasm_sd,
    faasm_wc,
    custom_plot,
    stats,
)
