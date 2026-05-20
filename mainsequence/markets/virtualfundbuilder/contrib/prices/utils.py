def transform_frequency_to_seconds(frequency_id: str) -> int:
    if frequency_id in ["1min", "1m"]:
        return 60
    elif frequency_id in ["5min", "5m"]:
        return 60 * 5
    elif frequency_id in ["15min", "15m"]:
        return 60 * 15
    elif frequency_id in ["1days", "1d"]:
        return 60 * 60 * 24
    else:
        raise NotImplementedError
