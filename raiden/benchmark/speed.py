# -*- coding: utf8 -*-
from __future__ import print_function

import time

import gevent
from ethereum import slogging
from ethereum.utils import sha3

from raiden.app import DEFAULT_SETTLE_TIMEOUT
from raiden.network.rpc.client import BlockChainServiceMock, MOCK_REGISTRY_ADDRESS
from raiden.network.transport import UDPTransport
from raiden.tasks import MediatedTransferTask
from raiden.tests.utils.network import create_network

log = slogging.getLogger('test.speed')  # pylint: disable=invalid-name
slogging.configure(':debug')

# set shorter timeout for testing
MediatedTransferTask.timeout_per_hop = 0.1


def test_mediated_transfer(num_transfers=100, num_nodes=10, num_assets=1,
                           channels_per_node=2, deposit=100):
    # pylint: disable=too-many-locals
    private_keys = [
        sha3('mediated_transfer:{}'.format(position))
        for position in range(num_nodes)
    ]

    assets = [
        sha3('asset:{}'.format(number))[:20]
        for number in range(num_assets)
    ]

    apps = create_network(
        private_keys,
        assets,
        MOCK_REGISTRY_ADDRESS,
        channels_per_node,
        deposit,
        DEFAULT_SETTLE_TIMEOUT,
        UDPTransport,
        BlockChainServiceMock
    )

    assert len(apps) > num_assets

    def start_transfers(idx, num_transfers):
        amount = 10
        curr_app = apps[idx]

        assets = sorted(curr_app.raiden.assetmanagers.keys())
        curr_asset = assets[idx]
        asset_manager = curr_app.raiden.assetmanagers[curr_asset]

        # search for a path of length=2 A > B > C
        num_hops = 2
        source = curr_app.raiden.address
        all_paths = asset_manager.channelgraph.get_paths_of_length(source, num_hops)
        assert len(all_paths)
        for path in all_paths:
            assert len(path) == num_hops + 1
            assert path[0] == source

        path = all_paths[0]
        target = path[-1]
        shortest_path_length = min(
            len(path)
            for path in asset_manager.channelgraph.get_shortest_paths(source, target)
        )

        assert path in asset_manager.channelgraph.get_shortest_paths(source, target)
        assert shortest_path_length == num_hops + 1

        finished = gevent.event.Event()

        def _completion_cb(task, success):
            _completion_cb.num_transfers -= 1
            if _completion_cb.num_transfers > 0:
                curr_app.raiden.api.transfer(curr_asset, amount, target)
            else:
                finished.set()

        _completion_cb.num_transfers = num_transfers
        assetmanagers_by_address = {
            node.raiden.address: node.raiden.assetmanagers
            for node in apps
        }

        next_hop = path[1]
        next_assetmanager = assetmanagers_by_address[next_hop][curr_asset]
        next_assetmanager.transfermanager.on_task_completed_callbacks.append(_completion_cb)

        curr_app.raiden.api.transfer(curr_asset, amount, target)
        return finished

    start_time = time.time()
    finished_events = []
    assert num_assets <= num_nodes

    # Start all transfers
    for i in range(num_assets):
        print('finished {}'.format(i))
        finished = start_transfers(i, num_transfers)
        finished_events.append(finished)

    # Wait until all transfers are done
    gevent.wait(finished_events)

    elapsed = time.time() - start_time
    completed_transfers = num_transfers * num_assets
    tps = completed_transfers / elapsed
    print('Completed {} transfers at {} tps'.format(completed_transfers, tps))


def print_serialization(pstats):  # pylint: disable=too-many-locals
    print('ncalls         tottime  percall  %    cumtime  percall  function')
    total_pct = 0.0

    for path_line_func, data in pstats.sort_stats('module', 'cumulative').stats.items():
        path, line, func = path_line_func  # pylint: disable=unused-variable

        is_rlp = 'rlp' in path
        is_encoding = 'encoding' in path
        is_umsgpack = 'msgpack' in path
        if is_rlp or is_encoding or is_umsgpack:
            # primitive calls dont count recursion
            # total calls count recursion
            # total time is the time for the function itself (excluding subcalls)
            # accumulated_time is the time of the function plus the subcalls
            primitive_calls, total_calls, total_time, acc_time, callers = data  # pylint: disable=unused-variable

            if primitive_calls != total_calls:
                calls = '{}/{}'.format(total_calls, primitive_calls)
            else:
                calls = str(primitive_calls)

            pct = (total_time / float(pstats.total_tt)) * 100
            total_pct += pct
            print('{:<14} {:<8.3f} {:<8.3f} {:<3.2f} {:<8.3f} {:<8.3f} {}'.format(
                calls,
                total_time,
                float(total_time) / total_calls,
                pct,
                acc_time,
                float(acc_time) / total_calls,
                func,
            ))

    print(' Runtime: {}, Total %: {}'.format(pstats.total_tt, total_pct))


def print_slow_path(pstats):
    pstats.strip_dirs().sort_stats('cumulative').print_stats(15)


def print_slow_function(pstats):
    pstats.strip_dirs().sort_stats('time').print_stats(15)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--transfers', default=100, type=int)
    parser.add_argument('--nodes', default=10, type=int)
    parser.add_argument('--assets', default=1, type=int)
    parser.add_argument('--channels-per-node', default=2, type=int)
    parser.add_argument('-p', '--profile', default=False, action='store_true')
    args = parser.parse_args()

    if args.profile:
        import GreenletProfiler
        GreenletProfiler.set_clock_type('cpu')
        GreenletProfiler.start()

    # test_mediated_transfer(num_assets=2)
    # test_mediated_transfer(num_transfers=1000)
    # test_mediated_transfer(num_transfers=1000, num_nodes=10, num_assets=9, channels_per_node=3)
    test_mediated_transfer(
        num_transfers=args.transfers,
        num_nodes=args.nodes,
        num_assets=args.assets,
        channels_per_node=args.channels_per_node,
    )

    if args.profile:
        GreenletProfiler.stop()
        stats = GreenletProfiler.get_func_stats()
        pstats = GreenletProfiler.convert2pstats(stats)

        print_serialization(pstats)
        print_slow_path(pstats)
        print_slow_function(pstats)

        # stats.print_all()
        # stats.save('profile.callgrind', type='callgrind')


if __name__ == '__main__':
    main()
