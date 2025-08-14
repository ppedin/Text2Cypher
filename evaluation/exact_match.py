from itertools import product
from collections import defaultdict
import random
import neo4j
from typing import List, Tuple, Dict, Set

def exact_match_score(predicted_query, ground_truth_query):
    # Check for errors. Assume a successful query returns a list of dicts.
    if not isinstance(predicted_query, list):
        return 0.0

    # A simple direct comparison can be a fast path if results are identical.
    if predicted_query == ground_truth_query:
        return 1.0

    # The detailed comparison function handles complex cases like unordered rows/columns.
    return _compare_execution(predicted_query, ground_truth_query, order_matters=False)

def to_hashable(obj, unorder_list=True):
    """
    Recursively transforms a list, dictionary, or set into a hashable object.
    Lists and sets are converted to tuples. Dictionaries are converted to tuples of sorted (key, value) pairs.

    Args:
    obj: The object to be transformed into a hashable form.

    Returns:
    A hashable version of the input object.
    """
    if isinstance(obj, (tuple, int, float, str, bool, type(None))):
        # These are already hashable
        return obj
    elif isinstance(obj, neo4j.time.Date):
        return obj.iso_format()
    elif isinstance(obj, (list, tuple)):
        # Convert list to a tuple
        if unorder_list:
            return tuple(sorted(to_hashable(item) for item in obj))
        else:
            return tuple(to_hashable(item) for item in obj)
    elif isinstance(obj, set):
        # Convert set to a tuple of sorted elements
        return tuple(sorted(to_hashable(item) for item in obj))
    elif isinstance(obj, dict):
        # Convert dict to a tuple of sorted key-value pairs
        return tuple(sorted((to_hashable(k), to_hashable(v)) for k, v in obj.items()))
    else:
        # For other types, raise an error or handle as needed
        raise TypeError(f"Unhashable type: {type(obj)}")


def permute_tuple(element: Tuple, perm: Tuple) -> Tuple:
    assert len(element) == len(perm)
    return tuple([element[i] for i in perm])


def unorder_row(row: Tuple) -> Tuple:
    return tuple(sorted(row, key=lambda x: str(x) + str(type(x))))


# unorder each row in the table
# [result_1 and result_2 has the same bag of unordered row]
# is a necessary condition of
# [result_1 and result_2 are equivalent in denotation]
def quick_rej(result1: List[Tuple], result2: List[Tuple], order_matters: bool) -> bool:
    s1 = [unorder_row(row) for row in result1]
    s2 = [unorder_row(row) for row in result2]
    if order_matters:
        return s1 == s2
    else:
        return set(s1) == set(s2)


# return whether two bag of relations are equivalent
def multiset_eq(l1: List, l2: List) -> bool:
    if len(l1) != len(l2):
        return False
    d = defaultdict(int)
    for e in l1:
        d[e] = d[e] + 1
    for e in l2:
        d[e] = d[e] - 1
        if d[e] < 0:
            return False
    return True


def get_constraint_permutation(tab1_sets_by_columns: List[Set], result2: List[Tuple]):
    num_cols = len(result2[0])
    perm_constraints = [{i for i in range(num_cols)} for _ in range(num_cols)]
    if num_cols <= 3:
        return product(*perm_constraints)

    # we sample 20 rows and constrain the space of permutations
    for _ in range(20):
        random_tab2_row = random.choice(result2)

        for tab1_col in range(num_cols):
            for tab2_col in set(perm_constraints[tab1_col]):
                if random_tab2_row[tab2_col] not in tab1_sets_by_columns[tab1_col]:
                    perm_constraints[tab1_col].remove(tab2_col)
    return product(*perm_constraints)


# check whether two denotations are correct
def result_eq(result1: List[Tuple], result2: List[Tuple], order_matters: bool) -> bool:
    if len(result1) == 0 and len(result2) == 0:
        return True

    # if length is not the same, then they are definitely different bag of rows
    if len(result1) != len(result2):
        return False

    num_cols = len(result1[0])

    # if the results do not have the same number of columns, they are different
    if len(result2[0]) != num_cols:
        return False

    # unorder each row and compare whether the denotation is the same
    # this can already find most pair of denotations that are different
    if not quick_rej(result1, result2, order_matters):
        return False

    # the rest of the problem is in fact more complicated than one might think
    # we want to find a permutation of column order and a permutation of row order,
    # s.t. result_1 is the same as result_2
    # we return true if we can find such column & row permutations
    # and false if we cannot
    tab1_sets_by_columns = [{row[i] for row in result1} for i in range(num_cols)]

    # on a high level, we enumerate all possible column permutations that might make result_1 == result_2
    # we decrease the size of the column permutation space by the function get_constraint_permutation
    # if one of the permutation make result_1, result_2 equivalent, then they are equivalent
    for perm in get_constraint_permutation(tab1_sets_by_columns, result2):
        if len(perm) != len(set(perm)):
            continue
        if num_cols == 1:
            result2_perm = result2
        else:
            result2_perm = [permute_tuple(element, perm) for element in result2]
        if order_matters:
            if result1 == result2_perm:
                return True
        else:
            # in fact the first condition must hold if the second condition holds
            # but the first is way more efficient implementation-wise
            # and we use it to quickly reject impossible candidates
            if set(result1) == set(result2_perm) and multiset_eq(result1, result2_perm):
                return True
    return False


def to_tuples(result: List[Dict]) -> List[Tuple]:
    # Sort the keys to ensure a canonical column order.
    keys = sorted(list(result[0].keys()))
    for row in result:
        assert set(row.keys()) == set(keys)
    return [tuple([row[key] for key in keys]) for row in result]


def _compare_execution(
        pred_executed: list[dict], target_executed: list[dict], order_matters: bool
) -> float:
    """Execution match considering same order of the output"""
    if not pred_executed and not target_executed:
        return 1.0
    elif not pred_executed or not target_executed:
        return 0.0

    gold_tuples = to_tuples(target_executed)
    pred_tuples = to_tuples(pred_executed)
    return float(result_eq(gold_tuples, pred_tuples, order_matters=order_matters))
