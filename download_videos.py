## Pre-requisities: run 'pip install youtube-dl' to install the youtube-dl package.
## Specify your location of output videos and input json file.
import json
import os
import math
import requests
import os
from queue import Queue
from threading import Thread
from time import time
output_path = './videos/wildwildwest'
json_path = './COIN.json'

TOKEN = ""
MAIN_API = ""

if not os.path.exists(output_path):
	os.mkdir(output_path)
	
data = json.load(open(json_path, 'r'))['database']
youtube_ids = list(data.keys())

def the_uploader_function(youtube_id, vid_loc):

	payload={'projectId': '6245ec34173fd7006e1b7e8e'}
	files=[
		('files',( youtube_id + '.mp4',open(vid_loc,'rb'),'application/octet-stream'))
	]
	headers = {
		'authorization': f'Bearer {TOKEN}'
	}

	response = requests.request("POST", MAIN_API, headers=headers, data=payload, files=files)

	print(response.text)
	if os.path.exists(vid_loc):
		os.remove(vid_loc)

def get_dir_size(path='.'):
    total = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(entry.path)
    return total

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return s, size_name[i]

def start_downloadin(youtube_id):
	size, type = convert_size(get_dir_size(output_path))
	print(f"Current Folder Size {size, type}")
	info = data[youtube_id]
	type = info['recipe_type']
	url = info['video_url']
	vid_loc = output_path
	if not os.path.exists(vid_loc):
		os.makedirs(vid_loc, exist_ok=True)
	vid_loc += '/' + youtube_id + '.mp4'
	os.system('youtube-dl -o ' + vid_loc  + ' -f best ' + url)
	the_uploader_function(youtube_id, vid_loc)



class DownloadWorker(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            youtube_id = self.queue.get()
            try:
                start_downloadin(youtube_id)
            finally:
                self.queue.task_done()


def main():
	ts = time()
	queue = Queue()
    # Create 8 worker threads
	for x in range(8):
			worker = DownloadWorker(queue)
			worker.daemon = True
			worker.start()
	for youtube_id in data:
		# print('Queueing {}'.format(youtube_id))
		queue.put(youtube_id)
	queue.join()
	print('Took %s', time() - ts)


if __name__ == '__main__':
    main()