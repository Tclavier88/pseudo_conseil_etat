import os
import src.data.xml2conll as x2c
import subprocess
import pandas as pd

from joblib import Parallel, delayed
from tqdm import tqdm
from src.tools.utils import available_cpu_count

def create_folder(folder_path, absolute=True):

    if not absolute:
        path = os.getcwd()
        folder_path = path + "/" + folder_path

    access_rights = 0o755

    if not os.path.isdir(folder_path):

        try:
            os.mkdir(folder_path, access_rights)
        except OSError:
            print("Creation of the directory %s failed" % folder_path)
        else:
            print("Successfully created the directory %s" % folder_path)

    else:
        print("Directory %s already exists" % folder_path)

    return folder_path + "/"


# def create_work_folders(raw_files_path, clean_data_path, absolute=True):
#
#     return raw_files_path + "/", clean_data_path + "/"

def get_correct_line(df_decisions):
    """
    The passed df has repeated lines for the same file (same chemin_source).
    We take the most recent one.
    :param df_decisions: Dataframe of decisions
    :return: Dataframe without repeated lines (according to the chemin_source column)
    """
    return df_decisions.sort_values('timestamp_modification').drop_duplicates('chemin_source', keep='last')


def process_file(row, raw_files_path, clean_data_path):
    source_path = (row["chemin_source"]).replace("\\", "/")  # Windows path -> Linux path cool hack

    if "manuel" in source_path:
        source_path = "/".join(source_path.split("/")[1:]).lower()
        decision_file_id = os.path.splitext(os.path.basename(source_path))[0]
    else:
        source_path = "/".join(source_path.split("/")[3:])  # Remove server name from path
        decision_file_id = os.path.splitext(os.path.basename(source_path))[0]

    if os.path.isfile(raw_files_path + source_path):
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
        subprocess.check_call(["textutil", "-convert", "txt", raw_files_path + source_path, "-output", txt_path])
        # with open(txt_path, "w", encoding="utf-8") as txto:
        #     txto.write(text)

        x2c.run(xml_path, txt_path)
    else:
        row["valid"] = False

    return 1


def build_database(raw_files_path, clean_data_path, only_corriges=True, n_jobs=available_cpu_count()):

    raw_files_path = create_folder(raw_files_path, absolute=True)
    clean_data_path = create_folder(clean_data_path, absolute=True)
    train_test_dev = create_folder(clean_data_path + "train_test_dev", absolute=True)
    train = create_folder(clean_data_path + "train_test_dev/train", absolute=True)
    test = create_folder(clean_data_path + "train_test_dev/test", absolute=True)
    dev = create_folder(clean_data_path + "train_test_dev/dev", absolute=True)

    df_decisions = pd.read_csv(raw_files_path  + "documents.csv")
    if only_corriges:
        df_decisions = df_decisions[df_decisions.statut == 5] # modifier ici
    df_decisions = get_correct_line(df_decisions)
    df_decisions = df_decisions.sample(frac=0.1)

    df_decisions["valid"] = False
    df_decisions["text"] = None
    df_decisions["local_path"] = None

    job_output = Parallel(n_jobs=n_jobs)(delayed(process_file)(row, raw_files_path, clean_data_path) for index, row in tqdm(df_decisions.iterrows()))


if __name__ == '__main__':

    raw_files_path = ""
    clean_data_path = ""

    build_database(raw_files_path, clean_data_path)
