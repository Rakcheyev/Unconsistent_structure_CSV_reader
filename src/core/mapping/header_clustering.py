from typing import List, Dict

from common.models import HeaderCluster


class HeaderClusteringService:
    """Very simple header clustering service.

    Current behavior: groups clusters only by lowercased canonical_name.
    This is intentionally minimal and deterministic; more advanced synonym
    handling can be layered on later.
    """

    def __init__(self, *args, **kwargs) -> None:  # accepts synonyms for future use
        pass

    def cluster(self, clusters: List[HeaderCluster]) -> List[HeaderCluster]:
        grouped: Dict[str, List[HeaderCluster]] = {}
        for cluster in clusters:
            key = cluster.canonical_name.strip().lower()
            grouped.setdefault(key, []).append(cluster)

        merged: List[HeaderCluster] = []
        for name, group in grouped.items():
            all_variants = []
            for c in group:
                all_variants.extend(c.variants)
            merged.append(
                HeaderCluster(
                    canonical_name=name,
                    variants=all_variants,
                    confidence_score=1.0,
                    needs_review=False,
                )
            )
        return merged
