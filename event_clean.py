import pandas as pd
from zipfile import ZipFile
from zipfile import BadZipfile

from DataAnalysisProcessor import USER_ORIGIN

INPUT_PATH = USER_ORIGIN + "/Dropbox (Rosalind Advisors)/Data Analytics Shared Folder/Event Database Export.xlsm"
FOLDER = USER_ORIGIN + "/Dropbox (Rosalind Advisors)/Data Analytics Shared Folder/"

def extract_vbaProject(input_file, output_folder = './'):

    # The VBA project file we want to extract.
    vba_filename = 'vbaProject.bin'
    output_path = output_folder + 'vbaProject.bin'

    # Get the xlsm file name from the commandline.
    xlsm_file = input_file

    try:
        # Open the Excel xlsm file as a zip file.
        xlsm_zip = ZipFile(xlsm_file, 'r')

        # Read the xl/vbaProject.bin file.
        vba_data = xlsm_zip.read('xl/' + vba_filename)

        # Write the vba data to a local file.
        vba_file = open(output_path, "wb")
        vba_file.write(vba_data)
        vba_file.close()

    except IOError as e:
        print("File error: %s" % str(e))
        exit()

    except KeyError as e:
        # Usually when there isn't a xl/vbaProject.bin member in the file.
        print("File error: %s" % str(e))
        print("File may not be an Excel xlsm macro file: '%s'" % xlsm_file)
        exit()

    except BadZipfile as e:
        # Usually if the file is an xls file and not an xlsm file.
        print("File error: %s: '%s'" % (str(e), xlsm_file))
        print("File may not be an Excel xlsm macro file.")
        exit()

    except Exception as e:
        # Catch any other exceptions.
        print("File error: %s" % str(e))
        exit()

    print("Extracted: %s" % vba_filename)

# extract_vbaProject(INPUT_PATH, FOLDER)

df = pd.read_excel(INPUT_PATH, sheet_name="Event")



print(df)
