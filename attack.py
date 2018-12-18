import pandas as pd
import numpy as np
import os

pd.set_option('display.max_rows', 500)

FILE_TRUTH = "./data/ground_truth.csv"
SUB_DIR = "./data/S_files/"

COL = {'id_user':1, 'date':2, 'hours':3, 'id_item':4, 'price':5, 'qty':6}
GEN_COL = {'Jan': 2, 'Feb': 3, 'Mar': 4, 'Apr': 5, 'May': 6, 'Jun': 7, 'Jul': 8, 'Aug': 9, 'Sep': 10, 'Oct': 11, 'Nov': 12, 'Dec': 13}

def init_general_frame():
    global GENERAL_FRAME
    GENERAL_FRAME = pd.DataFrame(index=id_users_t)

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
    else:
        id_users_s = csv_s.drop_duplicates(subset=[COL['id_user']])[COL['id_user']].astype(str).values

def saveIDs(IDs):
    global GENERAL_FRAME
    if month == 1:
        mt = 'Jan'
    elif month == 2:
        mt = 'Feb'
    elif month == 3:
        mt = 'Mar'
    elif month == 4:
        mt = 'Apr'
    elif month == 5:
        mt = 'May'
    elif month == 6:
        mt = 'Jun'
    elif month == 7:
        mt = 'Jul'
    elif month == 8:
        mt = 'Aug'
    elif month == 9:
        mt = 'Sep'
    elif month == 10:
        mt = 'Oct'
    elif month == 11:
        mt = 'Nov'
    elif month == 12:
        mt = 'Dec'
        
    GENERAL_FRAME = pd.concat([
        GENERAL_FRAME,
        pd.DataFrame(data={GEN_COL[]})
    ])
    GENERAL_FRAME = GENERAL_FRAME.append(pd.DataFrame(data={COL['id_user']: IDs[COL['id_user']].astype(str), month: IDs['id_user_s'].astype(str)}), ignore_index=True)
    GENERAL_FRAME = GENERAL_FRAME.drop_duplicates(subset=[COL['id_user']])
    #print('Identified {} users'.format(GENERAL_FRAME.shape[0]))
    clearCSV()

def saveID(IDs):
    global GENERAL_FRAME
    GENERAL_FRAME = GENERAL_FRAME.append(pd.DataFrame(data={COL['id_user']: [IDs[1]], month: [IDs[0]]}), ignore_index=True)
    GENERAL_FRAME = GENERAL_FRAME.drop_duplicates(subset=[COL['id_user']])
    #print('Identified {} users'.format(GENERAL_FRAME.shape[0]))
    clearCSV()
    get_id_users()

def clearCSV():
    global csv_s
    csv_s = csv_s[~csv_s[COL['id_user']].isin(GENERAL_FRAME[month].values)]

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
        #print('Identifying user {}'.format(id_user))
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
                    #print('Identified user {}'.format([id_user, table[0][0]]))
                    saveID([id_user, table[0][0]])
                    break

def main():
    global csv_t
    global csv_s
    global month
    #csv = get_csv(FILE_TRUTH)
    csv_t = get_csv(FILE_TRUTH)
    get_id_users(init=True)

    this = os.path.abspath(os.path.join(os.path.curdir, 'out/'))
    if not os.path.exists(this): os.makedirs(this)
    
    for file in os.listdir(SUB_DIR):
        init_general_frame()
        csv_sub = get_csv(SUB_DIR + file)
        print('Opened file {}'.format(file))
        for month in range(1, 13):
            #csv_t = csv.loc[csv[COL['date']].str.contains('/'+str(month).zfill(2)+'/')]
            csv_s = csv_sub.loc[csv_sub[COL['date']].str.contains('/'+str(month).zfill(2)+'/')]
            csv_s = csv_s.where(csv_s[COL['id_user']].astype(str) != 'DEL').dropna()
            print('For month {}, we have {} entries'.format(month, csv_s.shape[0]))
            #unique_items_attack() # Get users who bought unique items

            get_id_users()
            unique_combination()
            # if GENERAL_FRAME.shape[0] < id_users_s.shape[0]:
            #     unique_combination()

        print("GENERAL_FRAME Shape: {}".format(GENERAL_FRAME.shape[0]))
        print("Unique IDs: {}".format(GENERAL_FRAME.drop_duplicates(subset=[GEN_COL['id_user']]).shape[0]))
        print(GENERAL_FRAME[GENERAL_FRAME[GEN_COL['id_user']].duplicated(False)])
        GENERAL_FRAME.to_csv('out/'+file, sep=',', index=0)
        print('Saved file out/{}'.format(file))

if __name__ == "__main__":
    main()