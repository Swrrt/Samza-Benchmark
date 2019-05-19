import sys
import datetime

# number of partitions
partition_number = 1

# interval of end point
interval = 1000

fp_list = []
dict = {}
fp = open('SB_sample.txt')
for i in range(1, partition_number+1):
    fp_list.append(open('partition'+str(i)+'.txt', 'w+'))

Order_No            = 0
Tran_Maint_Code     = 1
Last_Upd_Date       = 2
Last_Upd_Time       = 3
Last_Upd_Time_Dec   = 4
Entry_Date          = 5
Entry_Time          = 6
Entry_Time_Dec      = 7
Order_Price         = 8
Order_Exec_Vol      = 9
Order_Vol           = 10
Sec_Code            = 11
PBU_ID              = 12
Acct_ID             = 13
Acct_Attr           = 14
Branch_ID           = 15
PBU_Inter_Order_No  = 16
PBU_Inter_Txt       = 17
Aud_Type            = 18
Order_Type          = 19
Trade_Restr_Type    = 20
Order_Stat          = 21
Trade_Dir           = 22
Order_Restr_Type    = 23
Short_Sell_Flag     = 24
Credit_Type         = 25
Stat_PBU_ID         = 26
Order_Bal           = 27
Trade_Flag          = 28


text = fp.readline()
start_time = datetime.datetime.strptime("20100913 09:15:00 0.000000", '%Y%m%d %H:%M:%S 0.%f')

next_time_endpoint = start_time + datetime.timedelta(milliseconds=interval)
endpoint_list = [next_time_endpoint] * partition_number


while text:
    textArr = text.split("|")
    date_time_str = textArr[Last_Upd_Date] + " " + textArr[Last_Upd_Time] + " " + textArr[Last_Upd_Time_Dec][:-1]
    order_time = datetime.datetime.strptime(date_time_str, '%Y%m%d %H:%M:%S 0.%f')
    if order_time >= next_time_endpoint:
        next_time_endpoint = next_time_endpoint + datetime.timedelta(milliseconds=interval)
        for pfp in fp_list:
            pfp.write("end\n")

    fp_list[int(textArr[Sec_Code]) % partition_number].write(text)
    text = fp.readline()

fp.close()
for pfp in fp_list:
    pfp.close()
