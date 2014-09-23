from tcga_import import TCGAImporter

def main():
    importer = TCGAImporter()
    importer.import_latest_dcc()

if __name__ == "__main__":
    main()
