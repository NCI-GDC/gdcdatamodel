from tcga_cghub_import import TCGACGHubImporter

def main():
    importer = TCGACGHubImporter()
    importer.import_all()

if __name__ == "__main__":
    main()
