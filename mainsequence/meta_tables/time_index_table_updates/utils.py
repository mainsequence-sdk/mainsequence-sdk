def string_freq_to_time_delta(frequency):
    import datetime

    if "m" in frequency:
        kwargs = {"minutes": int(frequency.replace("m", ""))}
    elif "d" in frequency:
        kwargs = {"days": int(frequency.replace("d", ""))}
    else:
        raise NotImplementedError

    time_delta = datetime.timedelta(**kwargs)
    return time_delta


def string_frequency_to_minutes(frequency):
    if "m" in frequency:
        minutes = int(frequency.replace("m", ""))
    elif "d" in frequency:
        minutes = int(frequency.replace("d", "")) * 24 * 60
    else:
        raise NotImplementedError

    return minutes
