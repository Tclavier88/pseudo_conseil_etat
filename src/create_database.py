import os
import sys
import textract
import subprocess

import pandas as pd
import src.data.xml2conll as x2c

from glob import glob
from joblib import Parallel, delayed
from tqdm import tqdm

from src.data.doc2txt import doc2txt
from src.tools.utils import available_cpu_count

n_jobs = available_cpu_count()

database_path = "database/"
clean_data_path = "clean_data/"


def create_work_folders(database_path=database_path, clean_data_path=clean_data_path):
    path = os.getcwd()
    print("The current working directory is %s" % path)
    new_db_path = path + "/" + database_path
    new_clean_data_path = path + "/" + clean_data_path
    access_rights = 0o755

    if not os.path.isdir(new_db_path):
        try:
            os.mkdir(new_db_path, access_rights)
        except OSError:
            print("Creation of the directory %s failed" % new_db_path)
        else:
            print("Successfully created the directory %s" % new_db_path)

    else:
        print("Directory %s already exists" % new_db_path)

    if not os.path.isdir(new_clean_data_path):
        try:
            os.mkdir(new_clean_data_path, access_rights)
        except OSError:
            print("Creation of the directory %s failed" % new_clean_data_path)
        else:
            print("Successfully created the directory %s" % new_clean_data_path)

    else:
        print("Directory %s already exists" % new_clean_data_path)

def get_correct_line(df_decisions):
    """
    The passed df has repeated lines for the same file (same chemin_source).
    We take the most recent one.
    :param df_decisions: Dataframe of decisions
    :return: Dataframe without repeated lines (according to the chemin_source column)
    """
    return df_decisions.sort_values('timestamp_modification').drop_duplicates('chemin_source', keep='last')


def process_file(row):

    source_path = (row["chemin_source"]).replace("\\", "/")  # Windows path -> Linux path cool hack

    if "manuel" in source_path:
        source_path = "/".join(source_path.split("/")[1:]).lower()
        decision_file_id = os.path.splitext(os.path.basename(source_path))[0]
    else:
        source_path = "/".join(source_path.split("/")[3:])  # Remove server name from path
        decision_file_id = os.path.splitext(os.path.basename(source_path))[0]


    if os.path.isfile(database_path + source_path):
        # text = textract.process("database/" + source_path, encoding='utf-8').decode("utf8")
        # row["valid"] = True
        # row["text"] = text
        # row["local_path"] = source_path
        # TODO: piste a investiguer ^
        if os.path.isfile(clean_data_path + str(row["id"]) + "_CoNLL.txt"):
            print("###################### already done")
            return 1

        xml_path = clean_data_path + str(row["id"]) + ".xml"
        with open(xml_path, "w", encoding="utf-16") as xmlo:
            xmlo.write(row["detail_anonymisation"])

        txt_path = clean_data_path + str(row["id"]) + ".txt"
        subprocess.check_call(["textutil", "-convert", "txt", database_path + source_path, "-output", txt_path])
        # with open(txt_path, "w", encoding="utf-8") as txto:
        #     txto.write(text)

        x2c.run(xml_path, txt_path)
    else:
        row["valid"] = False

    return 1


if __name__ == '__main__':

    #params
    only_corriges = True

    df_decisions = pd.read_csv(database_path + "documents.csv")
    if only_corriges:
        df_decisions = df_decisions[df_decisions.statut == 5]
    df_decisions = get_correct_line(df_decisions)
    df_decisions = df_decisions.sample(frac=0.1)

    df_decisions["valid"] = False
    df_decisions["text"] = None
    df_decisions["local_path"] = None

    job_output = Parallel(n_jobs=n_jobs)(delayed(process_file)(row) for index, row in tqdm(df_decisions.iterrows()))


    # logger.info(
    #     f"{len(processed_fine)} XML/DOC files were treated and saved as CoNLL. "
    #     f"{len(job_output) - len(processed_fine)} files had some error.")

