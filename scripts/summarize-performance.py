#!/usr/bin/env python3
"""Summarize benchmark CSVs or compare equivalent before/after runs using only stdlib."""
import argparse
import csv
import statistics
from pathlib import Path


def read_samples(path):
    with path.open(newline='') as handle:
        rows = list(csv.DictReader(line for line in handle if not line.startswith('#')))
    measured = [row for row in rows if row.get('phase') in ('sample', 'measured')]
    if not measured:
        raise ValueError(f'{path}: no completed measured samples')
    if 'operation' in measured[0]:
        final = rows[-1]
        if final.get('phase') != 'validation' or final.get('content_verified') != 'true':
            raise ValueError(f'{path}: missing final filesystem verification')
        if any(row['roots_equal'] != 'true' or row['conflicts'] != '0' for row in measured):
            raise ValueError(f'{path}: synchronization validation failed')
    groups = {}
    for row in measured:
        key = tuple((k, row[k]) for k in ('layout', 'count', 'records', 'operation') if k in row)
        groups.setdefault(key, []).append(row)
    return rows, groups


def values(samples, column):
    return [float(row[column]) for row in samples if row[column] not in ('', 'NA')]


def describe(numbers):
    return f'{statistics.median(numbers):.3f} [{min(numbers):.3f}, {max(numbers):.3f}]'


def summarize(path):
    _, groups = read_samples(path)
    print(f'\n{path}')
    for key, samples in groups.items():
        print(dict(key), f'n={len(samples)}')
        for column in samples[0]:
            if column.endswith(('_ms', '_kib')):
                numbers = values(samples, column)
                if numbers:
                    print(f'  {column}: median [min, max] = {describe(numbers)}')


def compare(before, after):
    before_rows, before_groups = read_samples(before)
    after_rows, after_groups = read_samples(after)
    # Every non-resource CSV field must agree, including sample IDs, action/payload/query
    # counts, Merkle roots and the independently computed final filesystem digest.
    def outcomes(rows):
        return [{k: v for k, v in row.items() if not k.endswith(('_ms', '_kib'))}
                for row in rows]
    if outcomes(before_rows) != outcomes(after_rows):
        raise ValueError('Before/after scenarios, sample counts or correctness outputs differ')
    print(f'\n{before} -> {after}: all non-timing outputs identical')
    for key, previous in before_groups.items():
        current = after_groups[key]
        print(dict(key), f'n={len(previous)} per version')
        for column in previous[0]:
            if not column.endswith(('_ms', '_kib')):
                continue
            left, right = values(previous, column), values(current, column)
            if not left or not right:
                continue
            delta = statistics.median(right) - statistics.median(left)
            percent = f'{delta / statistics.median(left) * 100:+.2f}%' if statistics.median(left) else 'n/a'
            print(f'  {column}: {describe(left)} -> {describe(right)}; '
                  f'delta {delta:+.3f} ({percent})')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('samples', nargs='*', type=Path)
    parser.add_argument('--compare', nargs=2, type=Path, metavar=('BEFORE', 'AFTER'))
    args = parser.parse_args()
    if not args.samples and not args.compare:
        parser.error('provide sample CSVs or --compare BEFORE AFTER')
    try:
        if args.compare:
            compare(*args.compare)
        for path in args.samples:
            summarize(path)
    except (ValueError, KeyError) as error:
        parser.exit(1, f'{error}\n')


if __name__ == '__main__':
    main()
