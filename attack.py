import pandas as pd
import numpy as np
import os

pd.set_option('display.max_rows', 500)

FILE_TRUTH = "./data/ground_truth.csv"
SUB_DIR = "./data/S_files/"

COL = {'id_user':1, 'date':2, 'hours':3, 'id_item':4, 'price':5, 'qty':6}

GENERAL_FRAME = pd.DataFrame(data={COL['id_user']:[], 'id_user_s':[]})

def init_general_frame():
    global GENERAL_FRAME
    GENERAL_FRAME = pd.DataFrame(data={COL['id_user']:[], 'id_user_s':[]})

def get_csv(file_path):
    # Read the ground truth file
    csv = pd.read_csv(file_path, sep=',', engine='c', na_filter=False, low_memory=False)
    csv.columns = COL.values()
    return csv

def get_id_users(init=False):
    global id_users_t
    global id_users_s
    if init:
        id_users_t = csv_t.drop_duplicates(subset=[COL['id_user']])[COL['id_user']].astype(str).values
    id_users_s = csv_s.drop_duplicates(subset=[COL['id_user']])[COL['id_user']].astype(str).values

def saveIDs(IDs):
    global GENERAL_FRAME
    GENERAL_FRAME = GENERAL_FRAME.append(pd.DataFrame(data={COL['id_user']: IDs[COL['id_user']].astype(str), 'id_user_s': IDs['id_user_s'].astype(str)}), ignore_index=True)
    GENERAL_FRAME = GENERAL_FRAME.drop_duplicates(subset=[COL['id_user']])
    #print('Identified {} users'.format(GENERAL_FRAME.shape[0]))
    clearCSV()

def saveID(IDs):
    global GENERAL_FRAME
    GENERAL_FRAME = GENERAL_FRAME.append(pd.DataFrame(data={COL['id_user']: [IDs[1]], 'id_user_s': [IDs[0]]}), ignore_index=True)
    GENERAL_FRAME = GENERAL_FRAME.drop_duplicates(subset=[COL['id_user']])
    #print('Identified {} users'.format(GENERAL_FRAME.shape[0]))
    clearCSV()
    get_id_users()

def clearCSV():
    global csv_s
    csv_s = csv_s[~csv_s[COL['id_user']].isin(GENERAL_FRAME['id_user_s'].values)]

def linker(csv, items, column_a, column_b, reindex=True):
    table = pd.DataFrame(data={column_a: csv[column_a], column_b: csv[column_b]})
    table = table[table[column_b].isin(items[0])]
    if reindex:
        nda = table.values.transpose()
        table = pd.DataFrame(data={column_a: nda[0]}, index=nda[1])
    return table

def items_counter(csv):
    # Create the DataFrame of items
    table = csv[COL['id_item']].value_counts()
    table = pd.DataFrame(table)
    return table

def unique_items_attack():
    tb_t = items_counter(csv_t)
    tb_s = items_counter(csv_s)
    tb = tb_t.assign(tb_s = tb_s)
    tb = tb.where(tb == 1)
    tb = tb.dropna()
    nda_tb = tb.reset_index().values.transpose()
    linked_t = linker(csv_t, nda_tb, COL['id_user'], COL['id_item'])
    linked_s = linker(csv_s, nda_tb, COL['id_user'], COL['id_item'])
    linked = linked_t.assign(id_user_s = linked_s[COL['id_user']]).drop_duplicates(subset=[COL['id_user']])
    linked[COL['id_user']] = linked[COL['id_user']].astype(str)
    saveIDs(linked)

def unique_combination():
    id_users_en_cours = id_users_s
    for id_user in id_users_en_cours:
        user_table = csv_s.where(csv_s[COL['id_user']].astype(str) == id_user)
        user_table = user_table.dropna()

        for rg in range(3, 10):
            if user_table.empty:
                break
            arr = np.array([])
            for i in range(rg):
                id_item = user_table.sample(n=1, axis=0)[COL['id_item']].astype(str).values
                arr = np.append(arr, id_item)
                arr = np.unique(arr)
            
            csv_filtered_s = csv_s[csv_s[COL['id_item']].isin(arr)].drop_duplicates(subset=[COL['id_user'], COL['id_item']])
            
            counter_s = csv_filtered_s[COL['id_user']].value_counts()
            counter_s = counter_s.where(counter_s == arr.shape[0]).dropna()

            if counter_s.shape[0] == 1:
                csv_filtered_t = csv_t[csv_t[COL['id_item']].isin(arr)].drop_duplicates(subset=[COL['id_user'], COL['id_item']])

                counter_t = csv_filtered_t[COL['id_user']].value_counts()
                counter_t = counter_t.where(counter_t == arr.shape[0]).dropna()

                if counter_t.shape[0] == 1:
                    table = counter_t.astype(int).reset_index().values.transpose()
                    saveID([id_user, table[0][0]])
                    break

def main():
    global csv_t
    global csv_s
    csv_t = get_csv(FILE_TRUTH)

    this = os.path.abspath(os.path.join(os.path.curdir, 'out/'))
    if not os.path.exists(this): os.makedirs(this)
    
    for file in os.listdir(SUB_DIR):
        init_general_frame()
        csv_s = get_csv(SUB_DIR + file)
        print('Opened file {}'.format(file))
        csv_s = csv_s.where(csv_s[COL['id_user']].astype(str) != 'DEL').dropna()

        unique_items_attack() # Get users who bought unique items

        get_id_users(init=True)
        unique_combination()
        if GENERAL_FRAME.shape[0] < id_users_s.shape[0]:
            unique_combination()
        if GENERAL_FRAME.shape[0] < id_users_s.shape[0]:
            unique_combination()

        print("GENERAL_FRAME Shape: {}".format(GENERAL_FRAME.shape[0]))
        print("Unique IDs: {}".format(GENERAL_FRAME.drop_duplicates(subset=['id_user_s']).shape[0]))
        GENERAL_FRAME.to_csv('out/'+file, sep=',', index=0)
        print('Saved file out/{}'.format(file))

if __name__ == "__main__":
    main()