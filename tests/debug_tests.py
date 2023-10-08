import re
from table_data_good import names, ages, dates, times, datetimes, booleans
from tests.test_data_plane_table import rows


def _find_elements(series, row_indices):
    # Find the rows corresponding to a set of row indices
    return [rows[i] for i in row_indices]


def _get_indices_for_range(series, min_val, max_val):
    # Matches an IN_RANGE.  Returns the indices in series where the value is between min_val and max_val inclusive
    return set([i for i in range(len(series)) if series[i] >= min_val and series[i] <= max_val])


def _get_indices_for_in_list(series, value_list):
    # Matches an IN_LIST filter.  Returns the indices in series where the value appears in value_list
    return set([i for i in range(len(series)) if series[i] in value_list])


def _get_indices_for_re_match(series, expression):
    # Matches a REGEX_MATCH filter.  Returns the indices in series which fully match re
    regex = re.compile(expression)
    return set([i for i in range(len(series)) if regex.fullmatch(series[i])])


def _get_indices_in_all_sets(sets):
    # Used for an ALL filter.  Given a list of sets, returns the members of every set
    result = sets[0]
    for member_set in sets[1:]:
        result = result & member_set
    return result


def _get_indices_in_any_set(sets):
    # Used for an ANY filter. Given a kist of sets, returns the set of items that appear in at least one set
    result = sets[0]
    for member_set in sets[1:]:
        result = result | member_set
    return result


def _get_indices_in_no_set(max_index, sets):
    # Used for a  NONE  filter. Given a kist of sets, returns the set of items that appear in none of the sets but in the universal set (range(max_index))
    universal_set = set(range(max_index))
    found_indices = _get_indices_in_any_set(sets)
    return universal_set - found_indices


pass
