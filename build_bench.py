#!/usr/bin/env python3

from datetime import datetime
from subprocess import run
from sys import argv
import time

'''
Builds IOx at 7 day intervals, gathers build times, stores them in build_bench.csv.

git checkout main
git pull

week_in_seconds = 7*24*60*60

commits_to_benchmark = [current_commit]

t = int(time.time())
t -= t % week_in_seconds

while t >= target:
	git show -s --format=%ct
	commits_to_benchmark.append(current_commit)
	t -= week_in_seconds

benchmark_build:
	cargo build
	cargo build --release
	cargo clean
	cargo build (gather duration)
	cargo clean
	cargo build -release (gather duration)
	output commit hash, commit date, and build times
'''

def benchmark_build(f, commit_time, commit_hash):
	p = run('git checkout %s' % commit_hash, shell=True, text=True, capture_output=True)
	p.check_returncode()

	p = run('cargo build && cargo build --release && cargo clean', shell=True, text=True, capture_output=True)
	p.check_returncode()
	ta = time.time()
	p = run('cargo build', shell=True, text=True, capture_output=True)
	p.check_returncode()
	tb = time.time()
	p = run('cargo build --release', shell=True, text=True, capture_output=True)
	p.check_returncode()
	tc = time.time()

	commit_date = datetime.fromtimestamp(commit_time).strftime('%Y-%m-%d')
	f.write('%s,%s,%d,%d\n' % (commit_date, commit_hash, tb-ta, tc-tb))
	f.flush()

def commits():
	for i in range(1000000):
		p = run(('git', 'show', 'HEAD~%d' % i, '-s', '--format=%ct %H'), text=True, capture_output=True)
		p.check_returncode()
		commit_time = int(p.stdout.split()[0])
		commit_hash = p.stdout.split()[1]
		yield((commit_time, commit_hash))

start_time = int(datetime.strptime(argv[1], '%Y-%m-%d').timestamp())
print('collecting weekly benchmarks between %s and today' % datetime.fromtimestamp(start_time).strftime('%Y-%m-%d'))
print('writing to %s' % argv[2])
f = open(argv[2], mode='w')
f.write('date,hash,debug,release\n')
f.flush()

p = run('git checkout main && git pull', shell=True, text=True, capture_output=True)
p.check_returncode()

commit_generator = commits()

to_benchmark = [next(commit_generator)]

week_in_seconds = 7*24*60*60

last_time, last_hash = to_benchmark[0]
week_time = last_time - (last_time % week_in_seconds)

while last_time > start_time:
	next_time, next_hash = next(commit_generator)
	if next_time < week_time:
		to_benchmark.append((last_time, last_hash))
		week_time -= week_in_seconds
	last_time = next_time

for commit_time, commit_hash in reversed(to_benchmark):
	print('benchmarking %s %s' % (commit_time, commit_hash))
	benchmark_build(f, commit_time, commit_hash)

