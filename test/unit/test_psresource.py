import platform

from paramsurvey import psresource


def test_vmem():
    vmem0 = psresource.vmem()
    big = bytearray(10000000)  # 10 megs
    vmem1 = psresource.vmem()
    # vmem might not go up at all, if there is free memory
    assert vmem1 <= vmem0 + 0.011, 'vmem does not go up more than expected'


def test_resource_stats():
    rs = psresource.resource_stats()
    for k in ('hostname', 'pid', 'total', 'available', 'load1', 'worker'):
        assert k in rs


def test_resource_complaint(capsys):
    rs = psresource.resource_stats()

    h = 'bob'
    hp = 'bob:32'

    rsc = rs.copy()
    rsc['available'] = rsc['total'] * 0.06
    psresource._memory_complaint(h, hp, rsc)
    out, err = capsys.readouterr()
    assert err
    psresource._memory_complaint(h, hp, rsc)
    out, err = capsys.readouterr()
    assert not err, 'one complaint per level'

    rsc = rs.copy()
    rsc['available'] = rsc['total'] * 0.01
    psresource._memory_complaint(h, hp, rsc)
    out, err = capsys.readouterr()
    assert err, 'second complaint if available falls'

    rsc = rs.copy()
    rsc['load1'] = 0.
    psresource._loadavg_complaint(h, hp, rsc)
    out, err = capsys.readouterr()
    assert not err

    rsc = rs.copy()
    rsc['load1'] = 100.
    psresource._loadavg_complaint(h, hp, rsc)
    out, err = capsys.readouterr()
    assert err

    rsc = rs.copy()
    rsc['load1'] = 0.
    psresource._loadavg_complaint(h, hp, rsc)
    out, err = capsys.readouterr()
    assert err, 'load returned to normal'

    rsc = rs.copy()
    rsc['load1'] = 0.
    psresource._loadavg_complaint(h, hp, rsc)
    out, err = capsys.readouterr()
    assert not err

    rs1 = rs.copy()
    rs1['uss'] = rs1['total'] / 4
    rs1['dirty'] = 0
    rs1['swap'] = 0

    for weird_key in ('uss', 'dirty', 'swap'):
        rs1c = rs1.copy()
        del rs1c[weird_key]
        psresource._other_complaint(h, hp, rs1c)
        out, err = capsys.readouterr()
        assert not err

    rs1c = rs1.copy()
    psresource._other_complaint(h, hp, rs1c)
    out, err = capsys.readouterr()
    assert not err

    rs1c = rs1.copy()
    rs1c['dirty'] = rs1c['uss']
    psresource._other_complaint(h, hp, rs1c)
    out, err = capsys.readouterr()
    assert err

    rs1c = rs1.copy()
    rs1c['swap'] = rs1c['uss']
    psresource._other_complaint(h, hp, rs1c)
    out, err = capsys.readouterr()
    assert err


def test_memory_limits():
    lim, limits = psresource.memory_limits(raw=True)
    assert 'available' in limits

    megabyte = 1024 * 1024

    assert lim > megabyte, 'at least a megabyte'
    assert lim < megabyte ** 3, 'less than an exabyte'

    assert lim <= limits['available']

    if platform.system == 'Linux':
        for e in ('rlimit_as', 'rlimit_rss', 'cgroups'):
            assert e in limits, 'expected '+e+' in limits'
    if platform.system == 'Darwin':
        for e in ('rrlimit_rss'):
            assert e in limits, 'expected '+e+' in limits'
        assert 'cgroup' not in limits, 'wut macos now has cgroups?'
