from pyspark import AccumulatorParam
from typing import Dict, List, Tuple
from utils import IMAGE_EXTENSIONS, normalize_terms


# ── Custom Accumulator Types ──────────────────────────────────────────────────

class MinAccParam(AccumulatorParam):
    def zero(self, init): return float('inf')
    def addInPlace(self, v1, v2): return min(v1, v2)

class MaxAccParam(AccumulatorParam):
    def zero(self, init): return float('-inf')
    def addInPlace(self, v1, v2): return max(v1, v2)

class DictSumAccParam(AccumulatorParam):
    def zero(self, init): return {}
    def addInPlace(self, d1, d2):
        for k, v in d2.items():
            d1[k] = d1.get(k, 0) + v
        return d1

class DictMaxTitleAccParam(AccumulatorParam):
    def zero(self, init): return {}
    def addInPlace(self, d1, d2):
        for project, (title, hits) in d2.items():
            if project not in d1 or hits > d1[project][1]:
                d1[project] = (title, hits)
        return d1


# ── Q1 ───────────────────────────────────────────────────────────────────────

def loop_page_size_stats(rdd, sc) -> Tuple[int, int, float]:
    min_acc  = sc.accumulator(float('inf'), MinAccParam())
    max_acc  = sc.accumulator(float('-inf'), MaxAccParam())
    sum_acc  = sc.accumulator(0)
    cnt_acc  = sc.accumulator(0)

    def update(record):
        size = record[3]
        min_acc.add(size)
        max_acc.add(size)
        sum_acc.add(size)
        cnt_acc.add(1)

    rdd.foreach(update)

    avg = (sum_acc.value / cnt_acc.value) if cnt_acc.value > 0 else 0.0
    return int(min_acc.value), int(max_acc.value), float(avg)


# ── Q2 ───────────────────────────────────────────────────────────────────────

def loop_image_counts(rdd, sc) -> Tuple[int, int]:
    total_acc  = sc.accumulator(0)
    non_en_acc = sc.accumulator(0)

    def update(record):
        project, title, _, _ = record
        if title.lower().endswith(IMAGE_EXTENSIONS):
            total_acc.add(1)
            if project != "en":
                non_en_acc.add(1)

    rdd.foreach(update)
    return int(total_acc.value), int(non_en_acc.value)


# ── Q3 ───────────────────────────────────────────────────────────────────────

def loop_top_terms(rdd, sc) -> List[Tuple[str, int]]:
    term_acc = sc.accumulator({}, DictSumAccParam())

    def update(record):
        local = {}
        for term in normalize_terms(record[1]):
            local[term] = local.get(term, 0) + 1
        term_acc.add(local)

    rdd.foreach(update)
    sorted_terms = sorted(term_acc.value.items(), key=lambda x: -x[1])
    return sorted_terms[:10]


# ── Q4 ───────────────────────────────────────────────────────────────────────

def loop_top_projects(rdd, sc) -> List[Tuple[str, int]]:
    proj_acc = sc.accumulator({}, DictSumAccParam())

    def update(record):
        proj_acc.add({record[0]: record[2]})

    rdd.foreach(update)
    sorted_projects = sorted(proj_acc.value.items(), key=lambda x: -x[1])
    return sorted_projects[:5]


# ── Q5 ───────────────────────────────────────────────────────────────────────

def loop_top_title_per_project(rdd, sc) -> List[Tuple[str, str, int]]:
    best_acc = sc.accumulator({}, DictMaxTitleAccParam())

    def update(record):
        project, title, hits, _ = record
        best_acc.add({project: (title, hits)})

    rdd.foreach(update)
    result = [(p, t, h) for p, (t, h) in best_acc.value.items()]
    return sorted(result, key=lambda x: x[0])