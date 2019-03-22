"""Take shameless from here:

https://stackoverflow.com/questions/31346790/

"""

import os
import zipfile

ZIP_DIR = "C:/Users/COTTRILLAD/Documents/1work/ScrapBook/AOFRC-334-GEN"
#ZIP_DIR = "C:/Users/COTTRILLAD/Desktop/Floppy_Disk_Project/004/LHA"

for path, dir_list, file_list in os.walk(ZIP_DIR):
    for file_name in file_list:
        if file_name.lower().endswith(".zip"):
            abs_file_path = os.path.join(path, file_name)

            # The following three lines of code are only useful if
            # a. the zip file is to unzipped in it's parent folder and
            # b. inside the folder of the same name as the file

            parent_path = os.path.split(abs_file_path)[0]
            output_folder_name = os.path.splitext(abs_file_path)[0]
            output_path = os.path.join(parent_path, output_folder_name)

            print("Unzipping {}".format(abs_file_path))
            zip_obj = zipfile.ZipFile(abs_file_path, 'r')
            zip_obj.extractall(output_path)
            zip_obj.close()
print("Done.")
