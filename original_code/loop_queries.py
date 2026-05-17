from typing import Dict, List, Tuple
from utils import IMAGE_EXTENSIONS, iter_records_from_file, normalize_terms


def loop_page_size_stats(input_path: str) -> Tuple[int, int, float]:
    min_val = max_val = None
    total_sum = total_count = 0

    for *_, size in iter_records_from_file(input_path):
        if min_val is None or size < min_val:
            min_val = size
        if max_val is None or size > max_val:
            max_val = size
        total_sum += size
        total_count += 1

    avg_val = (total_sum / total_count) if total_count > 0 else 0.0
    return int(min_val or 0), int(max_val or 0), float(avg_val)


def loop_image_counts(input_path: str) -> Tuple[int, int]:
    total_images = non_en_images = 0

    for project, title, *_ in iter_records_from_file(input_path):
        if title.lower().endswith(IMAGE_EXTENSIONS):
            total_images += 1
            if project != "en":
                non_en_images += 1

    return int(total_images), int(non_en_images)


def loop_top_terms(input_path: str) -> List[Tuple[str, int]]:
    term_counts: Dict[str, int] = {}
    
    for _, title, *_ in iter_records_from_file(input_path):
        for term in normalize_terms(title):
            term_counts[term] = term_counts.get(term, 0) + 1

    sorted_terms = sorted(term_counts.items(), key=lambda item: -item[1])
    return sorted_terms[:10]


def loop_top_projects(input_path: str) -> List[Tuple[str, int]]:
    project_counts: Dict[str, int] = {}

    for project, _, hits, _ in iter_records_from_file(input_path):
        project_counts[project] = project_counts.get(project, 0) + hits

    sorted_projects = sorted(project_counts.items(), key=lambda item: -item[1])
    return sorted_projects[:5]


def loop_top_title_per_project(input_path: str) -> List[Tuple[str, str, int]]:
    best: Dict[str, Tuple[str, int]] = {}

    for project, title, hits, _ in iter_records_from_file(input_path):
        current = best.get(project)
        if current is None or hits > current[1]:
            best[project] = (title, hits)

    result_list = [(project, title, hits) for project, (title, hits) in best.items()]
    return sorted(result_list, key=lambda x: x[0])