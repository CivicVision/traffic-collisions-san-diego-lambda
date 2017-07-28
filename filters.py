def killed(r):
    return r['killed'] > 0

def killed_or_injured(r):
    return r['killed'] > 0 or r['injured'] > 0

def injured(r):
    return r['injured'] > 0

