from typing import List, Tuple
from utils import IMAGE_EXTENSIONS, normalize_terms


def mr_page_size_stats(rdd) -> Tuple[int, int, float]:
    sizes = rdd.map(lambda record: record[3])
    
    def seq_op(acc, val):
        min_v, max_v, sum_v, count = acc
        new_min = val if min_v is None else min(min_v, val)
        new_max = val if max_v is None else max(max_v, val)
        return (new_min, new_max, sum_v + val, count + 1)
        
    def comb_op(acc1, acc2):
        min1, max1, sum1, count1 = acc1
        min2, max2, sum2, count2 = acc2
        
        if min1 is None: new_min = min2
        elif min2 is None: new_min = min1
        else: new_min = min(min1, min2)
            
        if max1 is None: new_max = max2
        elif max2 is None: new_max = max1
        else: new_max = max(max1, max2)
            
        return (new_min, new_max, sum1 + sum2, count1 + count2)

    min_val, max_val, total_sum, total_count = sizes.aggregate(
        (None, None, 0, 0), seq_op, comb_op
    )
    
    avg_val = (total_sum / total_count) if total_count > 0 else 0.0
    return int(min_val or 0), int(max_val or 0), float(avg_val)


def mr_image_counts(rdd) -> Tuple[int, int]:
    images = rdd.filter(lambda record: record[1].lower().endswith(IMAGE_EXTENSIONS))
    images.cache()
    
    total_images = images.count()
    non_en_images = images.filter(lambda record: record[0] != "en").count()
    
    images.unpersist()
    return int(total_images), int(non_en_images)


def mr_top_terms(rdd) -> List[Tuple[str, int]]:
    return (
        rdd.flatMap(lambda record: normalize_terms(record[1]))
        .map(lambda term: (term, 1))
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(10, key=lambda item: -item[1])
    )


def mr_top_projects(rdd) -> List[Tuple[str, int]]:
    return (
        rdd.map(lambda record: (record[0], record[2]))
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(5, key=lambda item: -item[1])
    )


def mr_top_title_per_project(rdd) -> List[Tuple[str, str, int]]:
    return (
        rdd.map(lambda record: (record[0], (record[1], record[2])))
        .reduceByKey(lambda a, b: a if a[1] >= b[1] else b)
        .map(lambda item: (item[0], item[1][0], item[1][1]))
        .collect()
    )