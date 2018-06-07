"""
structure of list v:
[
    {
        'datetime': datetime.datetime object,
        'speed':xxx.xxx in m/s
    }, ...
]

structure of v_all:
[v_1, v_2, ...]

structure of l:
[length_1 in m, length_2 in m, ...]
"""
import datetime


def get_time_offset_single_line(delta_p, v, offset=datetime.timedelta(0)):
    """
    :param delta_p: the distance
    :param v: the speeds of the line
    :type delta_p: float
    :type v: list

    :return: the timedelta when the band should have been at the requested position
    :rtype: datetime.timedelta
    """
    delta_p -= v[0]['speed'] * (v[0]['datetime'] - v[1]['datetime']).total_seconds()
    if (delta_p <= 0):
        return v[0]['datetime'] - v[1]['datetime']
    return get_time_offset_single_line(delta_p, v[1:], v[0]['datetime'] - v[1]['datetime']) + offset


def line_id(p, l):
    """
    :param p: the position in meters
    :param l: all line lengths
    :type p: float
    :type l: list[float]

    :return: the line_id of the position
    :rtype: int
    """
    if (p - l[0] <= 0):
        return 0
    return line_id(p - l[0], l[1:]) + 1


def get_time_offset_multiple_lines(p_1, p_2, l, all_v):
    """
    :param p_1: The sensor position in m since start of first line
    :param p_2: The current position in m since start of first line
    :param l: all line lengths
    :param all_v: all line speeds
    :type p_1: float
    :type p_2: float
    :type l: list[float]
    :type all_v: list[list[dict]]

    :return: the time in seconds when the current position should have been at the requested sensor position
    :rtype: int
    """
    sensor_line = line_id(p_1, l)
    current_line = line_id(p_2, l)

    if (sensor_line == current_line):
        return get_time_offset_single_line(p_2 - p_1, all_v[current_line])

    if (sensor_line > current_line):
        # something broke up. That's not possible
        return

    time_offset = datetime.timedelta(0)

    first_distance = sum(l[:sensor_line + 1]) - p_1
    last_distance = p_2 - sum(l[:current_line])

    for i in reversed(range(sensor_line, current_line + 1)):
        """
        counter always equals time_offset :)
        counter = 0
        for v in reversed(all_v[i]):
            if (v['datetime'] == now - time_offset):
                break
            counter += 1"""
        # skip time_offset seconds of current list

        temp_v = []

        for v in all_v[i]:
            if (time_offset <= all_v[i][0]["datetime"] - v["datetime"]):
                temp_v.append(v)

        delta_p = l[i]
        if i == sensor_line:
            delta_p = first_distance
        elif i == current_line:
            delta_p = last_distance
        time_offset += get_time_offset_single_line(delta_p, temp_v)

    return time_offset
