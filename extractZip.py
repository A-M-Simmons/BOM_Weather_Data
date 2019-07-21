import zipfile

def extractZip(file, targetdir):
    with zipfile.ZipFile(file,"r") as zip_ref:
        zip_ref.extractall(targetdir)

