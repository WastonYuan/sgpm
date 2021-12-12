# Environment set up

1. off the mod `go env -w  GO111MODULE=off`
2. change to `GPATH` to project directory `go env -w GOPATH=$HOME/sgpm`

# Usage

This project serves the article: **SGPM: A coroutine scheduling model for wound-wait concurrency control optimization**

```shell
// change root
cd sgpm
```

## Run workloads

```shell
type = [nondeterministic, determinisitc]
```

> non-deterministic concurrency control protocols include: 
>
> tpl(2pl), sstpl, sstpl_nw, occ_nw, mvcc
>
> deterministic concurrency control protocols include:
>
> calvin, calvin_nw, bohm, bohm_nw, pwv, pwv_nw, aria.

### high contention

```shell
go run src/t_coro/benchmark/${type}/high_conflict/main.go
// sample
go run src/t_coro/benchmark/nondeterministic/high_conflict/main.go
tpl:
thread: 1	ktps: 13203.4326812422	    reset_cnt: 0	ag_cnt: 0
thread: 2	ktps: 12833.339578891928	reset_cnt: 0	ag_cnt: 6301
thread: 3	ktps: 8493.386278668624	    reset_cnt: 0	ag_cnt: 24566
...

sstpl:
thread: 1	ktps: 12409.277324639228	reset_cnt: 0	ag_cnt: 0
thread: 2	ktps: 15492.526648377492	reset_cnt: 0	ag_cnt: 5419
thread: 3	ktps: 13830.129088350392	reset_cnt: 0	ag_cnt: 12211
...

```

### low contention

```shell
go run src/t_coro/benchmark/${type}/low_conflict/main.go
// sample
go run src/t_coro/benchmark/deterministic/low_conflict/main.go
calvin:
thread: 1	ktps: 3080.4342981099535	reset_cnt: 0	ag_cnt: 0
thread: 2	ktps: 6755.481267119352		reset_cnt: 0	ag_cnt: 0
thread: 3	ktps: 10538.865470475926	reset_cnt: 0	ag_cnt: 0
...

pwv:
thread: 1	ktps: 5865.370279066106		reset_cnt: 0	ag_cnt: 0
thread: 2	ktps: 13005.117140347866	reset_cnt: 0	ag_cnt: 0
thread: 3	ktps: 18185.346535817327	reset_cnt: 0	ag_cnt: 3456
...
```

## Process Queue size Test

```shell
type = [nondeterministic, determinisitc]
```

### high contention

```shell
go run t_coro/benchmark/${type}/p_test/high_conf_p/main.go -help
Usage:
  -t int
        specifiy the thread count (default 1)
// sample
go run src/t_coro/benchmark/nondeterministic/p_test/high_conf_p/main.go

calvin:
thread: 1	ktps: 3081.119371900259		reset_cnt: 0		ag_cnt: 0
thread: 2	ktps: 6043.745505100265		reset_cnt: 0		ag_cnt: 6
thread: 3	ktps: 10451.180001190492	reset_cnt: 0		ag_cnt: 61
...

pwv:
thread: 1	ktps: 6959.77334523733		reset_cnt: 0	ag_cnt: 0
thread: 2	ktps: 14039.339580087875	reset_cnt: 0	ag_cnt: 2
thread: 3	ktps: 20134.522449235472	reset_cnt: 0	ag_cnt: 44
...
```

### low contention

```shell
go run t_coro/benchmark/${type}/p_test/low_conf_p/main.go -help
Usage:
  -t int
        specifiy the thread count (default 1)
// sample
go run src/t_coro/benchmark/deterministic/p_test/low_conf_p/main.go

calvin:
thread: 128	p_size: 1	ktps: 8.927117583707796	    reset_cnt: 0	ag_cnt: 0
thread: 128	p_size: 2	ktps: 7.572126534571706	    reset_cnt: 0	ag_cnt: 1
thread: 128	p_size: 3	ktps: 8.060705027695164	    reset_cnt: 0	ag_cnt: 0
...

pwv:
thread: 32	p_size: 1	ktps: 3.1161268676958214	reset_cnt: 0	ag_cnt: 22854
thread: 32	p_size: 2	ktps: 71.54406706014281	    reset_cnt: 0	ag_cnt: 4
thread: 32	p_size: 3	ktps: 76.44205646254906	    reset_cnt: 0	ag_cnt: 1
...
```

## Run workload with Coroutine

```shell
type = [nondeterministic, determinisitc]
```

### high contention

```shell
go run src/t_coro/benchmark/{$type}/p_test/high_conf_compare/main.go -help
Usage:
  -p int
        specifiy the processQueue size (default 1)
// sample
go run src/t_coro/benchmark/nondeterminisitc/p_test/high_conf_compare/main.go -p 15
tpl:
thread: 1	ktps: 13085.124228860917	reset_cnt: 0	ag_cnt: 0
thread: 2	ktps: 27736.84051258458		reset_cnt: 0	ag_cnt: 126
thread: 3	ktps: 32100.771563873062	reset_cnt: 0	ag_cnt: 1951
...

sstpl:
thread: 1	ktps: 14182.76765364547		reset_cnt: 0	ag_cnt: 0
thread: 2	ktps: 16045.794517841021	reset_cnt: 0	ag_cnt: 3020
thread: 3	ktps: 21472.280448664762	reset_cnt: 0	ag_cnt: 3147
...
```

### low contention

```shell
go run src/t_coro/benchmark/{$type}/p_test/low_conf_compare/main.go -help
Usage:
  -p int
        specifiy the processQueue size (default 1)
// sample
go run src/t_coro/benchmark/determinisitc/p_test/low_conf_compare/main.go -p 15
calvin:
thread: 1	ktps: 3.2970616981793226	reset_cnt: 0	ag_cnt: 0
thread: 2	ktps: 7.411632828693931		reset_cnt: 0	ag_cnt: 0
thread: 3	ktps: 8.144347913968012		reset_cnt: 0	ag_cnt: 0
...

pwv:
thread: 1	ktps: 7.097991750997907		reset_cnt: 0	ag_cnt: 0
thread: 2	ktps: 14.109490379034808	reset_cnt: 0	ag_cnt: 0
thread: 3	ktps: 18.255801443594297	reset_cnt: 0	ag_cnt: 1
..
```

