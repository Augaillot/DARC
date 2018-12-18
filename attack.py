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

def saveID(data):
    global GENERAL_FRAME
    GENERAL_FRAME = pd.concat([
        GENERAL_FRAME,
        pd.DataFrame(data=data[1], index=data[0], columns=[GEN_COL[mt]])],
        axis=1, sort=True)
    clearCSV()
    get_id_users()

def clearCSV():
    global csv_s
    csv_s = csv_s[~csv_s[COL['id_user']].isin(GENERAL_FRAME[GEN_COL[mt]].values)]

def unique_combination():
    IDs = [[], []]
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

                if counter_t.shape[0] == 1 :
                    table = counter_t.astype(int).reset_index().values.transpose()
                    mask = np.isin(IDs[0], table[0][0].astype(str))
                    already = 0
                    if mask.shape[0] > 0:
                        already = IDs[0][mask].shape[0]
                    #print('Identified user {}'.format([id_user, table[0][0]]))
                    if already == 0:
                        IDs = np.append(IDs, [[table[0][0].astype(str)], [id_user]], axis=1)
                        break
    saveID(IDs)

def main():
    global csv_t
    global csv_s
    global month
    global mt
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
            #print(GENERAL_FRAME)
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
            #csv_t = csv.loc[csv[COL['date']].str.contains('/'+str(month).zfill(2)+'/')]
            csv_s = csv_sub.loc[csv_sub[COL['date']].str.contains('/'+str(month).zfill(2)+'/')]
            csv_s = csv_s.where(csv_s[COL['id_user']].astype(str) != 'DEL').dropna()
            print('For {}, we have {} entries'.format(mt, csv_s.shape[0]))

            get_id_users()
            unique_combination()
            # if GENERAL_FRAME.shape[0] < id_users_s.shape[0]:
            #     unique_combination()

        print("GENERAL_FRAME Shape: {}".format(GENERAL_FRAME.shape[0]))
        print("Unique IDs: {}".format(GENERAL_FRAME.dropna(how='all').shape[0]))
        GENERAL_FRAME.to_csv('out/'+file, sep=',', index_label='1')
        print('Saved file out/{}'.format(file))

if __name__ == "__main__":
    main()