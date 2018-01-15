#coding:utf8
import pandas as pd
import numpy as np

class RatingDataProcess(object):
    def __init__(self,data_path):
        self.data_path=data_path

    def select_heavy_users(self,data_file, to_file,heavy_num=50):
        # temp_1 has removed the 0.0 rating
        # 'temp_1.dat'
        each_moive = pd.read_table(data_file, header=None)

        users_set = each_moive[0].drop_duplicates()
        items_set = each_moive[1].drop_duplicates()

        num_users = len(users_set)
        num_items = len(items_set)

        heavy_users_ratinging_count_list = []
        heavy_user_list = []
        count = 0
        for u in users_set:
            size = len(each_moive[each_moive[0] == u])
            if size < heavy_num:
                continue
            heavy_users_ratinging_count_list.append({'userId': u, 'num_rating': size})
            heavy_user_list.append(u)
        # count+=1
        # if count > 3:
        # 	break

        print 'total users:{0}'.format(len(heavy_users_ratinging_count_list))

        new_data_set = each_moive[each_moive[0].isin(heavy_user_list)]

        new_data_set.to_csv(to_file, header=False, index=False, sep='\t')
        print new_data_set.describe()
        print "unique_use={0},unique_movie={1}".format(len(new_data_set[0].drop_duplicates()),
                                                       len(new_data_set[1].drop_duplicates()))

    def remapping_id(self,data_file, to_file=None,isverbose=False):
        '''
        remapping the user id and itemid from 1
        '''
        data = pd.read_table(data_file, header=None)
        unique_users = data[0].drop_duplicates()
        unique_items = data[1].drop_duplicates()
        size_users = len(unique_users)
        size_items = len(unique_items)
        new_userIDs = range(size_users)
        new_itemIDs = range(size_items)

        print "size of users={0},size of items={1}".format(size_users, size_items)

        user_replace_dict = {}
        for oleID, newID in zip(np.sort(unique_users), new_userIDs):
            user_replace_dict[oleID] = newID
            if newID % 100 == 0 and isverbose:
                print 'newID:{0}'.format(newID)

        item_replace_dict = {}
        for oleID, newID in zip(np.sort(unique_items), new_itemIDs):
            item_replace_dict[oleID] = newID
            if newID % 100 == 0 and isverbose:
                print 'newID:{0}'.format(newID)

        mapped_userID_serie = data[0].replace(user_replace_dict)

        mapped_itemID_serie = data[1].replace(item_replace_dict)

        data[0] = mapped_userID_serie
        data[1] = mapped_itemID_serie

        print data.describe()
        print 'saving'
        data.to_csv(to_file, head=False, index=False, sep='\t')

    def data_split(self,data_file, to_file, test_frac=0.2):
        '''
        split the data set accroding to the user,say,for each user,80% pairs are train data,the rest are test data
        '''
        from sklearn.cross_validation import train_test_split
        data = pd.read_table(data_file, header=None)
        train, test = train_test_split(data, test_size=test_frac)
        print train.describe()
        print test.describe()

        train_file = to_file + '_train.dat'
        test_file = to_file + '_test.dat'
        train.to_csv(train_file, header=False, index=False)
        test.to_csv(test_file, headr=False, index=False)
        print "train users:{0};test users:{1}".format(len(train[0].drop_duplicates()), len(test[0].drop_duplicates()))
        print "train items:{0};test items:{1}".format(len(train[1].drop_duplicates()), len(test[1].drop_duplicates()))


if __name__=="__main__":

    processor = RatingDataProcess("/Users/dong/Desktop/BoostingFM-IJCAI18/dataset/ml-100k/")

    org_data_path = processor.data_path+'u.all.nontime'

    print "select_heavy_users......"
    processor.select_heavy_users(org_data_path,processor.data_path+'heavu_user_rating.dat')

    print "remapping id......"
    processor.remapping_id(processor.data_path+'heavu_user_rating.dat',processor.data_path+'remapping_user_rating.dat')

    print "spliting data......"
    processor.data_split(processor.data_path+'remapping_user_rating.dat',processor.data_path+'ml-100')

    print "DONE!"