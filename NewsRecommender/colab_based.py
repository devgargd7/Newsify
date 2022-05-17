import logging

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from scipy.sparse.linalg import svds


def colab_based(pivoted):
    logging.info("creating collaborative-based similarity scores")
    mat = csr_matrix(pivoted.values)
    u, s, v = svds(mat, k=3)
    s = np.diag(s)

    pred_ratings = np.dot(np.dot(u, s), v)
    pred_ratings = (pred_ratings - pred_ratings.min()) / (pred_ratings.max() - pred_ratings.min())

    pred_df = pd.DataFrame(
        pred_ratings,
        columns=pivoted.columns,
        index=list(pivoted.index)
    ).transpose()

    return pred_df
