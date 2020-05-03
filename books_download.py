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

def identifier(site_content, resource_type):
	return re.compile("<a href=\"/download/" + resource_type + "/(.*?)." + resource_type + "\"").search(site_content).group(1)

def try_to_download(row, book_site_content, book_extension, do_on_success = None):
	generic_identifier = row["DOI URL"].replace("http://doi.org/", "")

	try:
		identifier = identifier(book_site_content, "epub")
	except Exception as e:
		identifier = generic_identifier

	destination = destination_folder + row["Book Title"].strip().replace("/", " - ") + "." + book_extension
	print(destination)

	if not os.path.exists(destination):
		with open(destination, 'wb+') as f:
			r_book = rq.get(base_url + "/content/" + book_extension + "/" + identifier + "." + book_extension, stream=True)
			f.write(r_book.content)
			print("It creates the follow" + book_extension, destination)

		if (do_on_success):
			do_on_success(row, book_site_content)

def scrap_and_download(row):
	springer_book_site = rq.get(row["OpenURL"], stream=True)
	text_of_springer_book_site = springer_book_site.text

	# download the epub
	try_to_download(row, text_of_springer_book_site, "epub")

	# download the pdf
	try_to_download(row, text_of_springer_book_site, "pdf", edit_metadata)

def edit_metadata(row, book_site_content):
	keywords = row["Subject Classification"].replace(";", ",")
	
	keywords_list = re.compile("<span data-test=\"book-keyword\" class=\"Keyword\">(.*?) </span>").findall(text_of_springer_book_site)		
	if (keywords_list):
		keywords = ', '.join(keywords_list)

	destination = destination_folder + row["Book Title"].strip().replace("/", " - ") + ".pdf"
	print("Start metadata edition")
	trailer = PdfReader(destination)
	trailer.Info.Title = row["Book Title"]
	trailer.Info.Subject = row["English Package Name"]
	trailer.Info.Keywords = keywords
	trailer.Info.Author = row["Author"]
	PdfWriter(destination, trailer=trailer).write()
	print("End metadata edition")

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