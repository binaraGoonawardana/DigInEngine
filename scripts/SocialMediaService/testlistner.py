

import time

#SA.start_listner('nokia_queue', 'Nokia', limit=200)
import subprocess
# with open("Output.txt", "w") as text_file:
#     text_file.write('obama')
subprocess.Popen(["python", "StreamingAnalytics.py", 'SriLankaQ1', 'usa'])

print 'I could escape.........'
time.sleep( 5 )