def notice_key(source_host, notice_id):
    if not source_host or not notice_id:
        raise ValueError('source host and notice id are required')
    return f"{source_host.lower().rstrip('/')}:{notice_id.strip()}"
