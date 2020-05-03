import pandas as pd
import requests as rq
import re
import os
from pdfrw import PdfReader, PdfWriter
from queue import Queue
from threading import Thread

path_to_books = "./Free+English+textbooks.xlsx"
base_url = "https://link.springer.com"
destination_folder = "/mnt/storage/Cuarentenbooks/Springer/"

# read excel and get the dataframe with pandas
df = pd.read_excel(path_to_books)
# show the columns
print(df.columns)

q = Queue(maxsize=0)
num_threads = 50

# add books row in the queue
for row_tuple in df.iterrows():
    q.put(row_tuple[1])

def scrap_and_download(row):
	springer_book_site = rq.get(row["OpenURL"], stream=True)
	text_of_springer_book_site = springer_book_site.text

	try:
		epub_identifier = re.compile("<a href=\"/download/epub/(.*?).epub\"").search(text_of_springer_book_site).group(1)
		destination = destination_folder + row["Book Title"].strip().replace("/", " - ") + ".epub"
		
		if not os.path.exists(destination):
			with open(destination, 'wb+') as f:
				r_epub = rq.get(base_url + "/content/epub/" + epub_identifier + ".epub", stream=True)
				f.write(r_epub.content)
				print("It creates the follow epub", destination)
	except Exception as e:
		print("Something failed trying to download epub file", str(e))

def execute_scrap_and_download(q):
    while not q.empty():
        work = q.get()
        try:
            scrap_and_download(work)
        except Exception as e:
            print("Something failed on the download" + str(e))
        q.task_done()
    return True

for i in range(num_threads):
    worker = Thread(target=execute_scrap_and_download, args=(q,))
    worker.setDaemon(True)
    worker.start()

q.join()