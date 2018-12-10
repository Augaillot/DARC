import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 500)

FILE_TRUTH = "./data/ground_truth.csv"
FILE_SUB = "./data/example_files/submission.csv"

FILE_PIET = "./data/submission_piet_months.csv"
FILE_PIET_IM = "./data/submission_piet_id_modified.csv"
FILE_PIET_SI = "./data/submission_piet_same_id.csv"

FILE_PAPY_IM = "./data/submission_papy_id_modified.csv"

COL = {1:'id_user', 2:'date', 3:'hours', 4:'id_item', 5:'price', 6:'qty'}

GENERAL_FRAME = pd.DataFrame(data={COL[1]:[], 'id_user_s':[]})

def get_csv(file_path):
    # Read the ground truth file
    csv = pd.read_csv(file_path, sep=',', engine='c',\
                               na_filter=False, low_memory=False)
    #print(csv.drop_duplicates(subset=[T_COL[1]])[T_COL[1]])
    return csv

def linker(csv, items, column_a, column_b):
    table = pd.DataFrame(data={column_a: csv[column_a], column_b: csv[column_b]})
    table = table[table[column_b].isin(items[0])]
    nda = table.values.transpose()
    table = pd.DataFrame(data={column_a: nda[0]}, index=nda[1])
    return table

def count_items(csv, groupby):
    csv['total'] = csv[COL[6]].groupby(groupby).transform('sum')
    table = csv.drop_duplicates(subset=COL[1])
    table = table.drop([COL[2], COL[3], COL[4], COL[5], COL[6]], axis=1)
    table = table.sort_values(by='total', ascending=False)
    table = table.reset_index(drop=True)
    return table

def items_counter(csv):
    # Create the DataFrame of items
    table = csv[COL[4]].value_counts()
    table = pd.DataFrame(table)
    return table

def unique_items_attack():
    tb_t = items_counter(csv_t)
    tb_s = items_counter(csv_s)

    ## Join DataFrames
    tb = tb_t.assign(tb_s = tb_s)

    ## Delete where count != 1
    tb = tb.where(tb == 1)
    tb = tb.dropna()

    ## Create ndarray
    nda_tb = tb.reset_index().values.transpose()

    ## Linking the DataFrames
    linked_t = linker(csv_t, nda_tb, COL[1], COL[4])
    linked_s = linker(csv_s, nda_tb, COL[1], COL[4])

    #print(linked_s[linked_s.index.duplicated()])

    linked = linked_t.assign(id_s = linked_s[COL[1]]).drop_duplicates(subset=[COL[1]])

    ## Prints
    print(linked, linked.shape)

def unify_transactions(csv):
    unique_transac = csv.drop_duplicates(subset=[COL[1], COL[2], COL[3]])
    unique_transac = unique_transac.drop([COL[4], COL[5], COL[6]], axis=1)
    table = unique_transac[COL[1]].value_counts()
    nda = table.reset_index().values.transpose()
    table = pd.DataFrame(data={COL[1]:nda[0], 'transac_count':nda[1]})
    table = table.where(table != 'DEL')
    table = table.dropna().reset_index(drop=True)
    return table

def unique_transactions_attack():
    tb_t = unify_transactions(csv_t)
    tb_s = unify_transactions(csv_s)

    ## Join DataFrames
    tb = tb_t.assign(transac_s=tb_s['transac_count'], id_user_s=tb_s[COL[1]])

    ## Delete rows with condition
    tb = tb.where(tb['transac_s'] > 15) # or ad[ad > 1] = np.nan
    tb = tb.dropna()
    saveIDs(tb.head(3).astype(int).astype(str))

def count_items_per_transaction():
    tb_t = count_items(csv_t, [csv_t[COL[2]], csv_t[COL[3]]])
    tb_s = count_items(csv_s, [csv_s[COL[2]], csv_s[COL[3]]])

def count_items_per_user():
    tb_t = count_items(csv_t, [csv_t[COL[1]]])
    tb_s = count_items(csv_s, [csv_s[COL[1]]])

    tb = tb_t.assign(total_s=tb_s['total'], id_user_s=tb_s[COL[1]])

    #tb['ok'] = np.where(tb[COL[1]]==tb['id_user_s'], 1, 0)
    saveIDs(tb.head(3))
    saveIDs(tb.tail(3))
    
def saveIDs(IDs):
    global GENERAL_FRAME
    GENERAL_FRAME = GENERAL_FRAME.append(pd.DataFrame(data={COL[1]: IDs[COL[1]].astype(str), 'id_user_s': IDs['id_user_s'].astype(str)}), ignore_index=True)
    GENERAL_FRAME.drop_duplicates(subset=[COL[1]])
    clearCSV()

def clearCSV():
    global csv_t
    global csv_s
    csv_t = csv_t[~csv_t[COL[1]].isin(GENERAL_FRAME[COL[1]].values)]
    csv_s = csv_s[~csv_s[COL[1]].isin(GENERAL_FRAME['id_user_s'].values)]

def main():
    global csv_t
    global csv_s
    csv_t = get_csv(FILE_TRUTH)
    print(csv_t.shape)
    csv_s = get_csv(FILE_PIET_SI)
    #csv_s = get_csv(FILE_PAPY_IM)

    count_items_per_user() # Gives approx. 6 users
    unique_transactions_attack() # Gives approx. 3 users
    unique_items_attack()

    print(GENERAL_FRAME)
    print(csv_t.shape)
    unique_items_attack()
    
    count_items_per_transaction()
    

if __name__ == "__main__":
    main()