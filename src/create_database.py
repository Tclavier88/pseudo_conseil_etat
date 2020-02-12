import src.data.build_database as bdb


if __name__ == '__main__':

    raw_files_path = "/Users/thomasclavier/Documents/Projects/Etalab/prod/pseudo_conseil_etat/src/database"
    clean_data_path = "/Users/thomasclavier/Documents/Projects/Etalab/prod/pseudo_conseil_etat/src/clean_data"

    bdb.build_database(raw_files_path, clean_data_path)

