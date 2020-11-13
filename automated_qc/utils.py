import numpy as np
import math

def parse_bounds(bounds):
    # parse upper left bounds to pull S3 tiles
    x1 = math.floor(bounds[0] / 10) * 10
    y1 = math.ceil(bounds[1] / 10) * 10
    x2 = math.floor(bounds[2] / 10) * 10
    y2 = math.ceil(bounds[3] / 10) * 10
    # check if bounds cover multiple S3 tiles
    if x1 != x2:
        Xs = [x1, x2]
    else:
        Xs = [x1]
    if y1 != y2:
        Ys = [y1, y2]
    else:
        Ys = [y1]
    # convert to string
    X_list, Y_list = [], []
    for X in Xs:
        if bounds[0] > 0:
            X = "{:03d}E".format(X)
        else:
            X = "{:03d}W".format(X * -1)
        X_list.append(X)
    for Y in Ys:
        if bounds[1] > 0:
            Y = "{:02d}N".format(Y)
        else:
            Y = "{:02d}S".format(Y * -1)
        Y_list.append(Y)

    return X_list, Y_list

def concatenate_windows(win_arrs, X_list, Y_list):
    # if there are 4 tiles, concatenate on both axes
    if (len(X_list) > 1) and (len(Y_list) > 1):
        win_arr = np.concatenate(
                (
                    np.concatenate((win_arrs[2], win_arrs[3]), axis=1),
                    np.concatenate((win_arrs[0], win_arrs[1]), axis=1)
                ),
            axis=0
        )
    # otherwise, concatenate on one axis
    elif (len(X_list) > 1) and (len(Y_list) == 0):
        win_arr = np.concatenate((win_arrs[0], win_arrs[1]), axis=0)
    elif (len(X_list) > 1) and (len(Y_list) == 0):
        win_arr = np.concatenate((win_arrs[0], win_arrs[1]), axis=1)
    else:
        win_arr = win_arrs[0]

    return win_arr
