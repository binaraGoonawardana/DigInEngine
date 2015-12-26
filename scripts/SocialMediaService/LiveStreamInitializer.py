
import subprocess
import time

def initialize_stream(keyword, unique_id, tokens):
    try:
        subprocess.Popen(["python", "StreamingAnalytics.py", unique_id, keyword])
        time.sleep(10)

    except TypeError:
        raise

    return 1