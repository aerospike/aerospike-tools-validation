#!/usr/bin/env python3
"""Adds the bench-only instrumentation (make BENCH=1) to a branch checkout.

The bench code lives in this test bed's src/backup.c and Makefile (master plus
instrumentation). Cherry-picking it onto a branch that changed scan_callback()
or main() conflicts, so this script copies the same blocks into the branch at
fixed anchors instead.

    test/bench/add_bench.py <checkout>
"""

import os
import re
import sys

here = os.path.dirname(os.path.abspath(__file__))
tb_src = open(os.path.join(here, "../../src/backup.c")).read()
tb_mk = open(os.path.join(here, "../../Makefile")).read()


def block(start, end):
    i = tb_src.index(start)
    j = tb_src.index(end, i) + len(end)
    return tb_src[i:j]


def insert_after(s, anchor, text):
    assert s.count(anchor) == 1, anchor[:60]
    return s.replace(anchor, anchor + text)


def insert_before(s, anchor, text):
    assert s.count(anchor) == 1, anchor[:60]
    return s.replace(anchor, text + anchor)


def main():
    repo = sys.argv[1]
    path = os.path.join(repo, "src/backup.c")
    s = open(path).read()

    if "ASVAL_BENCH" in s:
        print("already instrumented")
        return

    top = block("#ifdef ASVAL_BENCH\ntypedef struct bench_stats_s {",
            "return x < y ? -1 : x > y;\n}\n#endif\n")
    replay = block("#ifdef ASVAL_BENCH\n__attribute__((noinline)) static void\nbench_replay_pass(",
            "cf_free(g_bench.recs);\n}\n#endif\n")
    env = block("#ifdef ASVAL_BENCH\n\tconst char *bench_noop", "\t}\n#endif\n")
    report = block("#ifdef ASVAL_BENCH\n\tfprintf(stderr, \"BENCH noop=",
            "\t\tbench_replay(&conf);\n\t}\n#endif\n")

    # cdt_check() takes a fourth argument on the integration branch.
    call = re.search(r"if \(! cdt_check\(pnc->conf->as, rec, pnc->conf(, [^)]*)?\)\) \{",
            s)
    assert call is not None
    extra = call.group(1) or ""

    if extra:
        top = top.replace(
                "#define BENCH_CHECK(_conf, _rec) cdt_check((_conf)->as, (_rec), (_conf))",
                "static uint32_t g_bench_deepest;\n"
                "#define BENCH_CHECK(_conf, _rec) cdt_check((_conf)->as, (_rec), (_conf), "
                "&g_bench_deepest)")

    s = insert_after(s, "                                                                    ///  threads.\n",
            "\n" + top)
    s = s.replace(call.group(0), """#ifdef ASVAL_BENCH
	if (g_bench.replay_passes != 0) {
		return bench_keep(rec);
	}

	uint64_t check_start = bench_ns(CLOCK_MONOTONIC);
	bool need_log = ! g_bench.noop &&
			cdt_check(pnc->conf->as, rec, pnc->conf%s);

	t_check_ns += bench_ns(CLOCK_MONOTONIC) - check_start;
#else
	bool need_log = cdt_check(pnc->conf->as, rec, pnc->conf%s);
#endif

	if (! need_log) {""" % (extra, extra))
    s = insert_after(s, "\tcf_queue *job_queue = cont;\n\tvoid *res = (void *)EXIT_FAILURE;\n",
            "\n#ifdef ASVAL_BENCH\n\tuint64_t cpu_start = bench_ns(CLOCK_THREAD_CPUTIME_ID);\n#endif\n")
    s = insert_before(s, "\tif (verbose) {\n\t\tver(\"Leaving validation thread\");",
            "#ifdef ASVAL_BENCH\n\tatomic_add(&g_bench.worker_cpu_ns,\n"
            "\t\t\tbench_ns(CLOCK_THREAD_CPUTIME_ID) - cpu_start);\n"
            "\tatomic_add(&g_bench.check_ns, t_check_ns);\n#endif\n\n")
    s = insert_before(s, "///\n/// It all starts here.\n///", replay + "\n")
    s = insert_after(s, "\tint32_t res = EXIT_FAILURE;\n\n\tenable_client_log();\n", "\n" + env)
    s = insert_after(s, "\tuint32_t n_threads_ok = 0;\n",
            "\n#ifdef ASVAL_BENCH\n\tuint64_t bench_start = bench_ns(CLOCK_MONOTONIC);\n#endif\n")

    join_end = "\t\t\tres = EXIT_FAILURE;\n\t\t}\n\t}\n\n"
    i = s.index(join_end, s.index("cleanup9:")) + len(join_end)
    s = s[:i] + report + "\n" + s[i:]
    open(path, "w").write(s)

    mk_path = os.path.join(repo, "Makefile")
    mk = open(mk_path).read()
    bench_mk = tb_mk[tb_mk.index("# Benchmark build:"):tb_mk.index("endif\n", tb_mk.index("# Benchmark build:")) + 6]
    mk = insert_after(mk, "TOML := $(DIR_TOML)/libtoml.a\n", "\n" + bench_mk)
    mk = mk.replace("$(DIR_OBJ):\n\tmkdir $(DIR_OBJ)", "$(DIR_OBJ):\n\tmkdir -p $(DIR_OBJ)")
    mk = insert_after(mk, "\trm -f $(DEPS) $(OBJS) $(BINS)\n",
            "\trm -rf obj/bench $(DIR_BIN)/asvalidation-bench\n")
    open(mk_path, "w").write(mk)
    print("instrumented")


main()
