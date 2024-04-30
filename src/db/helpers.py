import numpy as np
import pandas as pd
import pyarrow as pa

__all__ = ['is_null']


def is_null(x):
    if isinstance(x, pd._libs.missing.NAType):
        return True
    if isinstance(x, pa.lib.NullScalar):
        return True
    if x in {np.nan, np.NAN}:
        return True
    return x is None
