# -*- coding: utf-8 -*-
import sys, xlrd, datetime, os, csv
from src.utility import Utility
reload(sys)
sys.setdefaultencoding('utf-8')

class IO():

    def __init__(self):
        self.output_path = os.getcwd() + "\\output";
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

    def print_df(self, filename, df):
        try:
            util = Utility()
            filewritetime = datetime.datetime.now()
            filepath = self.output_path + "\\%s.txt"%(filename)
            filepath = "%s.txt"%(filename)
            f = open(filepath, 'wb')
            writer = csv.writer(f)
            writer.writerows(df)
            f.close()
            util.printKeyValue(
            '    write file Time diff',
            datetime.datetime.now() - filewritetime, ' ', True, True)
            return filepath
        except Exception as inst:
            print type(inst)
            print inst.args