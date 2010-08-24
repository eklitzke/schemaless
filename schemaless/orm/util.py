def is_type_list(t, xs):
    try:
        return all(isinstance(x, t) for x in xs)
    except TypeError:
        return False
