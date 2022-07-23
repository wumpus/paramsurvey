def helper_pre(pset, user_kwargs):
    user_kwargs['pre_count'] = user_kwargs.get('pre_count', 0) + 1
    return True


def helper_post(pset, user_kwargs):
    if 'pre_count' not in user_kwargs or user_kwargs['pre_count'] < 1:
        raise ValueError('did not see pre_count')
    pass
