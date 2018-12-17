import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 500)

FILE_TRUTH = "./data/ground_truth.csv"
FILE_SUB = "./data/example_files/submission.csv"

FILE_PIET = "./data/submission_piet_months.csv"
FILE_PIET_IM = "./data/submission_piet_id_modified.csv"
FILE_PIET_SI = "./data/submission_piet_same_id.csv"

FILE_PAPY_IM = "./data/submission_papy_id_modified.csv"

COL = {'id_user':1, 'date':2, 'hours':3, 'id_item':4, 'price':5, 'qty':6}

GENERAL_FRAME = pd.DataFrame(data={COL['id_user']:[], 'id_user_s':[]})

def get_csv(file_path):
    # Read the ground truth file
    csv = pd.read_csv(file_path, sep=',', engine='c',\
                               na_filter=False, low_memory=False)
    csv.columns = COL.values()
    return csv

def saveIDs(IDs):
    global GENERAL_FRAME
    GENERAL_FRAME = GENERAL_FRAME.append(pd.DataFrame(data={COL['id_user']: IDs[COL['id_user']].astype(str), 'id_user_s': IDs['id_user_s'].astype(str)}), ignore_index=True)
    GENERAL_FRAME.drop_duplicates(subset=[COL['id_user']])
    clearCSV()

def clearCSV():
    global csv_t
    global csv_s
    csv_t = csv_t[~csv_t[COL['id_user']].isin(GENERAL_FRAME[COL['id_user']].values)]
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

def unify_transactions(csv):
    unique_transac = csv.drop_duplicates(subset=[COL['id_user'], COL['date'], COL['hours']])
    unique_transac = unique_transac.drop([COL['id_item'], COL['price'], COL['qty']], axis=1)
    table = unique_transac[COL['id_user']].value_counts()
    nda = table.reset_index().values.transpose()
    table = pd.DataFrame(data={COL['id_user']:nda[0], 'transac_count':nda[1]})
    table = table.where(table != 'DEL')
    table = table.dropna().reset_index(drop=True)
    return table

def unique_transactions_attack():
    tb_t = unify_transactions(csv_t)
    tb_s = unify_transactions(csv_s)

    ## Join DataFrames
    tb = tb_t.assign(transac_s=tb_s['transac_count'], id_user_s=tb_s[COL['id_user']])

    ## Delete rows with condition
    tb = tb.where(tb['transac_s'] > 15) # or ad[ad > 1] = np.nan
    tb = tb.dropna()
    tb[COL['id_user']] = tb[COL['id_user']].astype(int)
    saveIDs(tb.head(3))

def count_items_per_transaction():
    tb_t = count_items(csv_t, [csv_t[COL['date']], csv_t[COL['hours']]])
    tb_s = count_items(csv_s, [csv_s[COL['date']], csv_s[COL['hours']]])

def count_items(csv, groupby):
    csv['total'] = csv[COL['qty']].groupby(groupby).transform('sum')
    table = csv.drop_duplicates(subset=COL['id_user'])
    table = table.drop([COL['date'], COL['hours'], COL['id_item'], COL['price'], COL['qty']], axis=1)
    table = table.sort_values(by='total', ascending=False)
    table = table.reset_index(drop=True)
    return table

def count_items_per_user():
    tb_t = count_items(csv_t, [csv_t[COL['id_user']]])
    tb_s = count_items(csv_s, [csv_s[COL['id_user']]])
    tb = tb_t.assign(total_s=tb_s['total'], id_user_s=tb_s[COL['id_user']])
    saveIDs(tb.head(3))
    saveIDs(tb.tail(3))

def double_item_attack():
    tb_t = items_counter(csv_t)
    tb_s = items_counter(csv_s)
    tb = tb_t.assign(tb_s = tb_s)
    tb = tb.where(tb == 2)
    tb = tb.dropna()
    nda_tb = tb.reset_index().values.transpose()
    linked_t = linker(csv_t, nda_tb, COL['id_user'], COL['id_item'], False).sort_values(by=[COL['id_item']]).reset_index(drop=True)
    linked_s = linker(csv_s, nda_tb, COL['id_user'], COL['id_item'], False).sort_values(by=[COL['id_item']]).reset_index(drop=True)
    linked = linked_t.assign(id_item_s=linked_s[COL['id_item']], id_user_s = linked_s[COL['id_user']])
    print(linked.shape)
    linked = identify_same(linked)
    print(linked.shape)

def identify_same(table):
    table_bool = table.duplicated(subset=[COL['id_user'], COL['id_item']])
    tb = table.where(table_bool == True).dropna()
    tb[COL['id_user']] = tb[COL['id_user']].astype(int)
    table = table[~table[COL['id_user']].isin(tb[COL['id_user']].values)]
    saveIDs(tb)
    return table

def pick_random_user():
    return True

def main():
    global csv_t
    global csv_s
    csv_t = get_csv(FILE_TRUTH)
    print("CSV Truth Shape: {}".format(csv_t.shape))
    #csv_s = get_csv(FILE_PIET_SI)
    csv_s = get_csv(FILE_PAPY_IM)

    #print(csv_s.sample(n=1, axis=0)[COL[1]].values[0])
    count_items_per_user() # Gives 6 users
    unique_transactions_attack() # Gives 3 users
    unique_items_attack() # Can give up to approx. 140 users
    double_item_attack()

    print("GENERAL_FRAME Shape: {}".format(GENERAL_FRAME.shape))
    print(GENERAL_FRAME)
    print("CSV Truth New Shape: {}".format(csv_t.shape))
    
    #count_items_per_transaction()
    

if __name__ == "__main__":
    main()